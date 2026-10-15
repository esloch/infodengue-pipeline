"""Transactional claim and finite execution of analysis jobs."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import structlog

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    AnalysisJobStatus,
    WorkflowRow,
)
from infodengue_pipeline.db.repositories import SqlWorkflowRepository
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobResult,
    Coverage,
    Epiweek,
    WorkflowStatus,
)
from infodengue_pipeline.protocols import AnalysisRunner

logger = structlog.get_logger(__name__)


class AnalysisJobService:
    """Create, explicitly retry, and execute persisted analysis jobs."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        image: str,
        cores: int,
        load: bool,
        output_root: Path,
    ) -> None:
        self._sessions = sessions
        self._image = image
        self._cores = cores
        self._load = load
        self._output_root = output_root.resolve()

    def claim_ready(self, coverage: Coverage, epiweek: Epiweek) -> UUID | None:
        """Create or reuse one job for the current ready fingerprint."""
        with self._sessions.begin() as session:
            workflow = session.scalar(
                select(WorkflowRow)
                .where(
                    WorkflowRow.coverage == coverage,
                    WorkflowRow.epiweek == int(epiweek),
                )
                .with_for_update()
            )
            if workflow is None or workflow.status is not WorkflowStatus.READY:
                return None
            snapshot = SqlWorkflowRepository(session).get_for_update(
                coverage, epiweek
            )
            if snapshot is None or not snapshot.readiness.ready:
                return None
            fingerprint = snapshot.readiness.input_fingerprint
            if fingerprint is None:
                return None
            existing = session.scalar(
                select(AnalysisJobRow)
                .where(
                    AnalysisJobRow.workflow_id == workflow.workflow_id,
                    AnalysisJobRow.input_fingerprint == fingerprint,
                )
                .with_for_update()
            )
            if existing is not None:
                return existing.job_id
            job_id = uuid4()
            job = AnalysisJobRow(
                job_id=job_id,
                workflow_id=workflow.workflow_id,
                input_fingerprint=fingerprint,
                status=AnalysisJobStatus.QUEUED,
                image=self._image,
                cores=self._cores,
                load=self._load,
                output_dir=str(self._output_root / str(job_id)),
            )
            session.add(job)
            workflow.status = WorkflowStatus.QUEUED
            workflow.queued_fingerprint = fingerprint
            return job_id

    def retry(self, job_id: UUID) -> bool:
        """Explicitly return only a FAILED job to QUEUED."""
        with self._sessions.begin() as session:
            job = session.scalar(
                select(AnalysisJobRow)
                .where(AnalysisJobRow.job_id == job_id)
                .with_for_update()
            )
            if job is None or job.status is not AnalysisJobStatus.FAILED:
                return False
            job.status = AnalysisJobStatus.QUEUED
            job.started_at = None
            job.finished_at = None
            job.exit_code = None
            job.last_error = None
            job.dispatched_at = None
            job.output_dir = str(
                self._output_root / str(job_id) / f'attempt-{job.attempts + 1}'
            )
            workflow = session.get(WorkflowRow, job.workflow_id)
            if workflow is not None:
                snapshot = SqlWorkflowRepository(session).get_for_update(
                    workflow.coverage, Epiweek.parse(workflow.epiweek)
                )
                if (
                    snapshot is not None
                    and snapshot.readiness.input_fingerprint
                    == job.input_fingerprint
                ):
                    workflow.status = WorkflowStatus.QUEUED
                    workflow.queued_fingerprint = job.input_fingerprint
            return True

    def execute(self, job_id: UUID, runner: AnalysisRunner) -> bool:
        """Claim a queued job, run it, and persist its terminal state."""
        with self._sessions.begin() as session:
            job = session.scalar(
                select(AnalysisJobRow)
                .where(AnalysisJobRow.job_id == job_id)
                .with_for_update()
            )
            if job is None or job.status is not AnalysisJobStatus.QUEUED:
                return False
            workflow_row = session.get(WorkflowRow, job.workflow_id)
            if workflow_row is None:
                job.status = AnalysisJobStatus.FAILED
                job.last_error = 'workflow record missing'
                job.finished_at = datetime.now(timezone.utc)
                return False
            job.status = AnalysisJobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            job.attempts += 1
            request = AnalysisJobRequest(
                job_id=job.job_id,
                coverage=workflow_row.coverage,
                epiweek=Epiweek.parse(workflow_row.epiweek),
                input_fingerprint=job.input_fingerprint,
                cores=job.cores,
                load=job.load,
                output_dir=Path(job.output_dir),
                image=job.image,
                workflow_id=job.workflow_id,
            )
            workflow_row_id = workflow_row.workflow_id
            coverage = workflow_row.coverage
            epiweek = Epiweek.parse(workflow_row.epiweek)
            fingerprint = job.input_fingerprint
            snapshot = SqlWorkflowRepository(session).get_for_update(
                coverage, epiweek
            )
            if (
                snapshot is not None
                and snapshot.readiness.input_fingerprint == fingerprint
            ):
                workflow_row.status = WorkflowStatus.RUNNING
            logger.info(
                'analysis_job_claimed',
                job_id=str(job.job_id),
                workflow_id=str(workflow_row_id),
                input_fingerprint=fingerprint,
                coverage=coverage.value,
                epiweek=str(epiweek),
                job_status=AnalysisJobStatus.RUNNING.value,
            )

        result: AnalysisJobResult | None = None
        failure = ''
        try:
            result = runner.run(request)
            if not result.succeeded:
                failure = result.error or 'analysis runner reported failure'
        except Exception as exc:
            failure = f'{type(exc).__name__} during OCI execution'

        succeeded = result is not None and result.succeeded and not failure
        with self._sessions.begin() as session:
            job = session.scalar(
                select(AnalysisJobRow)
                .where(AnalysisJobRow.job_id == job_id)
                .with_for_update()
            )
            if job is None or job.status is not AnalysisJobStatus.RUNNING:
                return False
            job.status = (
                AnalysisJobStatus.SUCCEEDED
                if succeeded
                else AnalysisJobStatus.FAILED
            )
            job.finished_at = datetime.now(timezone.utc)
            job.exit_code = (
                result.exit_code
                if result is not None and result.exit_code is not None
                else (0 if succeeded else 1)
            )
            job.last_error = None if succeeded else failure[:512]
            workflow = session.get(WorkflowRow, workflow_row_id)
            if workflow is not None:
                snapshot = SqlWorkflowRepository(session).get_for_update(
                    coverage, epiweek
                )
                if (
                    snapshot is not None
                    and snapshot.readiness.input_fingerprint == fingerprint
                    and workflow.queued_fingerprint == fingerprint
                ):
                    workflow.status = (
                        WorkflowStatus.SUCCEEDED
                        if succeeded
                        else WorkflowStatus.FAILED
                    )
            logger.info(
                'analysis_job_finished',
                job_id=str(job.job_id),
                workflow_id=str(job.workflow_id),
                input_fingerprint=job.input_fingerprint,
                job_status=job.status.value,
                exit_code=job.exit_code,
            )
        return succeeded
