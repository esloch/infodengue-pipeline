"""Finite execution of persisted Recife bulletin jobs."""

import json

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import Engine, func, or_, select, update
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.bulletin.readiness import CheckBulletinReadiness
from infodengue_pipeline.bulletin.runner import GEOCODE, run
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    BulletinJobRow,
    WorkflowRow,
)
from infodengue_pipeline.db.repositories import SqlBulletinJobRepository
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    BulletinJobStatus,
    BulletinKind,
    Coverage,
    Epiweek,
)

Render = Callable[..., Path]


def _lock_key(job_id: UUID) -> int:
    """Map a job UUID to a PostgreSQL advisory lock key."""
    return job_id.int & ((1 << 63) - 1)


class BulletinJobService:
    """Create, claim, execute, and explicitly retry bulletin jobs."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        readiness: CheckBulletinReadiness,
        *,
        revision: str,
        reference: Path,
        config: Path,
        output_root: Path,
        source_database_url: str | None,
        render: Render = run,
    ) -> None:
        self._sessions = sessions
        self._readiness = readiness
        self._revision = revision
        self._reference = reference
        self._config = config
        self._output_root = output_root
        self._source_database_url = source_database_url
        self._render = render

    def _eligible(self, week: Epiweek, analysis_id: UUID) -> bool:
        result = self._readiness.check(week, self._reference, self._config)
        return result.eligible and result.analysis_job_id == analysis_id

    def dispatch_candidates(self, limit: int) -> list[UUID]:
        """Create jobs only for ready PE analyses and return queued IDs."""
        with self._sessions() as session:
            candidates = session.execute(
                select(AnalysisJobRow.job_id, WorkflowRow.epiweek)
                .join(WorkflowRow)
                .where(
                    WorkflowRow.coverage == Coverage.PE,
                    AnalysisJobRow.status == AnalysisJobStatus.SUCCEEDED,
                    AnalysisJobRow.load.is_(True),
                    AnalysisJobRow.exit_code == 0,
                    AnalysisJobRow.finished_at.is_not(None),
                )
                .order_by(AnalysisJobRow.finished_at.desc())
                .limit(limit)
            ).all()
        ids: list[UUID] = []
        for analysis_id, week in candidates:
            epiweek = Epiweek.parse(week)
            if not self._eligible(epiweek, analysis_id):
                continue
            with self._sessions.begin() as session:
                row = SqlBulletinJobRepository(session).get_or_create(
                    kind=BulletinKind.RECIFE_INTRAMUNICIPAL,
                    municipality_geocode=GEOCODE,
                    analysis_job_id=analysis_id,
                    renderer_revision=self._revision,
                )
                if row.status is BulletinJobStatus.QUEUED:
                    ids.append(row.job_id)
        return ids

    def queued_candidates(self, limit: int) -> list[UUID]:
        """Find committed jobs needing publication or broker recovery."""
        with self._sessions() as session:
            return list(
                session.scalars(
                    select(BulletinJobRow.job_id)
                    .where(BulletinJobRow.status == BulletinJobStatus.QUEUED)
                    .order_by(BulletinJobRow.created_at)
                    .limit(limit)
                )
            )

    def claim_dispatch(self, job_id: UUID, retry_before: datetime) -> bool:
        """Lease one queued message for confirmed broker publication."""
        with self._sessions.begin() as session:
            row = session.get(BulletinJobRow, job_id)
            if row is None:
                return False
            analysis = session.get(AnalysisJobRow, row.analysis_job_id)
            workflow = (
                session.get(WorkflowRow, analysis.workflow_id)
                if analysis is not None
                else None
            )
            if workflow is None or not self._eligible(
                Epiweek.parse(workflow.epiweek), row.analysis_job_id
            ):
                return False
            claimed = session.scalar(
                update(BulletinJobRow)
                .where(
                    BulletinJobRow.job_id == job_id,
                    BulletinJobRow.status == BulletinJobStatus.QUEUED,
                    or_(
                        BulletinJobRow.dispatched_at.is_(None),
                        BulletinJobRow.dispatched_at <= retry_before,
                    ),
                )
                .values(dispatched_at=datetime.now(timezone.utc))
                .returning(BulletinJobRow.job_id)
            )
            return claimed is not None

    def execute(self, job_id: UUID) -> bool:
        """Commit the claim before R/Quarto and persist its result."""
        with self._execution_lock(job_id) as acquired:
            return self._execute_locked(job_id) if acquired else False

    @contextmanager
    def _execution_lock(self, job_id: UUID) -> Iterator[bool]:
        """Keep a session lock while rendering, without an open transaction."""
        engine: Engine = self._sessions.kw['bind']
        with engine.connect() as connection:
            acquired = bool(
                connection.scalar(
                    select(func.pg_try_advisory_lock(_lock_key(job_id)))
                )
            )
            connection.commit()
            try:
                yield acquired
            finally:
                if acquired:
                    connection.scalar(
                        select(func.pg_advisory_unlock(_lock_key(job_id)))
                    )
                    connection.commit()

    def _execute_locked(self, job_id: UUID) -> bool:
        """Claim, render, and persist one execution under its process lock."""
        with self._sessions.begin() as session:
            row = session.scalar(
                select(BulletinJobRow)
                .where(BulletinJobRow.job_id == job_id)
                .with_for_update()
            )
            if row is None or row.status is not BulletinJobStatus.QUEUED:
                return False
            analysis = session.get(AnalysisJobRow, row.analysis_job_id)
            workflow = (
                session.get(WorkflowRow, analysis.workflow_id)
                if analysis is not None
                else None
            )
            if workflow is None or not self._eligible(
                Epiweek.parse(workflow.epiweek), row.analysis_job_id
            ):
                return False
            week = workflow.epiweek
            row.status = BulletinJobStatus.RUNNING
            row.attempts += 1
            row.started_at = datetime.now(timezone.utc)
            row.finished_at = None
            row.last_error = None
            row.exit_code = None

        artifact: Path | None = None
        error: str | None = None
        try:
            if self._source_database_url is None:
                raise ValueError('bulletin source database is not configured')
            manifest = self._render(
                week=week,
                reference=self._reference,
                output_root=self._output_root,
                config=self._config,
                database_url=self._source_database_url,
            )
            payload = json.loads(manifest.read_text(encoding='utf-8'))
            pdf = Path(payload['outputs']['pdf'])
            if (
                payload['status'] != 'succeeded'
                or not pdf.is_file()
                or not pdf.stat().st_size
            ):
                raise ValueError('bulletin PDF is missing')
            artifact = pdf
        except Exception as exc:
            error = f'{type(exc).__name__} during bulletin execution'

        with self._sessions.begin() as session:
            row = session.scalar(
                select(BulletinJobRow)
                .where(BulletinJobRow.job_id == job_id)
                .with_for_update()
            )
            if row is None or row.status is not BulletinJobStatus.RUNNING:
                return False
            row.status = (
                BulletinJobStatus.SUCCEEDED
                if artifact is not None
                else BulletinJobStatus.FAILED
            )
            row.finished_at = datetime.now(timezone.utc)
            row.exit_code = 0 if artifact is not None else 1
            row.artifact_path = str(artifact) if artifact is not None else None
            row.last_error = error
        return artifact is not None

    def retry(self, job_id: UUID) -> bool:
        """Explicitly queue a failed job, preserving its attempt count."""
        with self._sessions.begin() as session:
            row = session.scalar(
                select(BulletinJobRow)
                .where(BulletinJobRow.job_id == job_id)
                .with_for_update()
            )
            if row is None or row.status is not BulletinJobStatus.FAILED:
                return False
            row.status = BulletinJobStatus.QUEUED
            row.started_at = None
            row.finished_at = None
            row.exit_code = None
            row.last_error = None
            row.artifact_path = None
            row.dispatched_at = None
            return True

    def fail_interrupted(self, timeout_seconds: int) -> int:
        """Mark abandoned attempts failed; a human may explicitly retry."""
        cutoff = datetime.now(timezone.utc) - timedelta(
            seconds=timeout_seconds
        )
        with self._sessions.begin() as session:
            stale_ids = session.scalars(
                select(BulletinJobRow.job_id).where(
                    BulletinJobRow.status == BulletinJobStatus.RUNNING,
                    BulletinJobRow.started_at <= cutoff,
                )
            ).all()
            recovered = 0
            for job_id in stale_ids:
                if not session.scalar(
                    select(func.pg_try_advisory_xact_lock(_lock_key(job_id)))
                ):
                    continue
                result = session.scalars(
                    update(BulletinJobRow)
                    .where(
                        BulletinJobRow.job_id == job_id,
                        BulletinJobRow.status == BulletinJobStatus.RUNNING,
                        BulletinJobRow.started_at <= cutoff,
                    )
                    .values(
                        status=BulletinJobStatus.FAILED,
                        finished_at=datetime.now(timezone.utc),
                        exit_code=1,
                        last_error=(
                            'bulletin execution interrupted or timed out'
                        ),
                    )
                    .returning(BulletinJobRow.job_id)
                )
                recovered += len(result.all())
            return recovered
