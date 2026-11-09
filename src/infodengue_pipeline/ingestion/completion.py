"""Read-only AlertaDengue completion boundary and idempotent event bridge."""

import re

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID

import structlog

from infodengue_pipeline.db.models import IngressObjectRow, IngressStatus
from infodengue_pipeline.db.repositories import SqlOutboxRepository
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    Coverage,
    Disease,
    Epiweek,
    disease_from_cid10,
)
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CompletedRun:
    """Only the completed external run fields consumed by the bridge."""

    run_id: UUID
    status: str
    uf: str
    disease: str
    delivery_se: int
    sha256: str
    finished_at: datetime
    canonical_cid10: str | None = None


def resolve_run_disease(disease: str, canonical_cid10: str | None) -> Disease:
    """Resolve an upstream coarse or canonical disease and optional CID-10."""
    coarse = disease.strip().upper()
    canonical = canonical_cid10.strip().upper() if canonical_cid10 else None
    if canonical is None:
        if coarse == 'A92':
            raise ValueError(
                'AlertaDengue run has ambiguous A92 disease '
                'without canonical CID'
            )
        return disease_from_cid10(coarse)

    resolved = disease_from_cid10(canonical)
    expected_coarse = 'A90' if canonical == 'A90' else 'A92'
    if coarse not in (expected_coarse, canonical):
        raise ValueError(
            f'inconsistent AlertaDengue disease {coarse!r} and canonical '
            f'CID {canonical!r}'
        )
    return resolved


class CompletedIngestionSource(Protocol):
    """Read-only query contract for AlertaDengue ``ingestion.run``."""

    def by_sha256(self, digest: str) -> CompletedRun | None:
        """Return the completed run matching a local materialized file."""
        ...

    def reconcile_by_sha256(self, digest: str) -> CompletedRun | None:
        """Return a safe completed match or fail for an existing run."""
        ...


class PostgresCompletedIngestionSource:
    """Query the external PostgreSQL contract without importing Django."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def close(self) -> None:
        """Dispose the source connection pool for one-shot operator use."""
        self._engine.dispose()

    def exists_sha256(self, digest: str) -> bool:
        """Read whether any upstream run, regardless of status, owns a SHA."""
        with self._engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            return bool(
                connection.execute(
                    text(
                        'SELECT EXISTS (SELECT 1 FROM ingestion.run '
                        'WHERE sha256 = :sha256)'
                    ),
                    {'sha256': digest},
                ).scalar_one()
            )

    def status_by_sha256(self, digest: str) -> str | None:
        """Return the current upstream run status for this SHA."""
        with self._engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            rows = (
                connection.execute(
                    text(
                        'SELECT status FROM ingestion.run '
                        'WHERE sha256 = :sha256'
                    ),
                    {'sha256': digest},
                )
                .mappings()
                .all()
            )
        if len(rows) > 1:
            raise ValueError('multiple AlertaDengue runs share one SHA-256')
        return str(rows[0]['status']) if rows else None

    def by_sha256(self, digest: str) -> CompletedRun | None:
        """Read one completed run by the unique file SHA-256."""
        with self._engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            rows = (
                connection.execute(
                    text(
                        'SELECT id, status, uf, disease, canonical_cid10, '
                        'delivery_se, sha256, '
                        'finished_at FROM ingestion.run '
                        'WHERE sha256 = :sha256 AND status = :status'
                    ),
                    {'sha256': digest, 'status': 'completed'},
                )
                .mappings()
                .all()
            )
        if len(rows) > 1:
            raise ValueError('multiple completed runs share one SHA-256')
        if not rows:
            return None
        row = rows[0]
        finished_at = row['finished_at']
        if finished_at is None:
            return None
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=timezone.utc)
        return CompletedRun(
            run_id=UUID(str(row['id'])),
            status=str(row['status']),
            uf=str(row['uf']),
            disease=str(row['disease']),
            delivery_se=int(row['delivery_se']),
            sha256=str(row['sha256']),
            finished_at=finished_at.astimezone(timezone.utc),
            canonical_cid10=(
                str(row['canonical_cid10'])
                if row['canonical_cid10'] is not None
                else None
            ),
        )

    def reconcile_by_sha256(self, digest: str) -> CompletedRun | None:
        """Return a completed run or fail closed for any existing run.

        A non-completed source record can represent an ingestion whose
        effects are still in progress or need operator recovery. Publishing
        the same bytes again could duplicate those effects.
        """
        statuses = self._statuses_by_sha256(digest)
        if not statuses:
            return None
        if len(statuses) > 1:
            raise ValueError('multiple AlertaDengue runs share one SHA-256')
        if statuses[0] != 'completed':
            raise RuntimeError(
                'matching AlertaDengue ingestion exists without a verifiable '
                f'completed record (status={statuses[0]})'
            )
        run = self.by_sha256(digest)
        if run is not None:
            if run.sha256 != digest:
                raise ValueError('completed run SHA-256 does not match object')
            if run.status != 'completed':
                raise ValueError('historical run is not completed')
            if not re.fullmatch(r'[0-9a-f]{64}', run.sha256):
                raise ValueError('completed run SHA-256 is invalid')
            if run.canonical_cid10 is None:
                raise ValueError('completed run lacks canonical CID-10')
            disease = resolve_run_disease(run.disease, run.canonical_cid10)
            if disease.canonical_cid10 != run.canonical_cid10.upper():
                raise ValueError('completed run CID-10 is not canonical')
            Epiweek.parse(run.delivery_se)
            Coverage(run.uf)
            if (
                run.finished_at.tzinfo is None
                or run.finished_at.utcoffset() is None
            ):
                raise ValueError(
                    'completed run timestamp is not timezone aware'
                )
            return run
        raise RuntimeError(
            'completed AlertaDengue ingestion has no verifiable run metadata'
        )

    def _statuses_by_sha256(self, digest: str) -> list[str]:
        """Read all matching source states in a short read-only transaction."""
        with self._engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            rows = (
                connection.execute(
                    text(
                        'SELECT status FROM ingestion.run '
                        'WHERE sha256 = :sha256'
                    ),
                    {'sha256': digest},
                )
                .mappings()
                .all()
            )
        return [str(row['status']) for row in rows]


class IngestionCompletionBridge:
    """Persist a deterministic completion event then correlate the ingress."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        source: CompletedIngestionSource,
    ) -> None:
        self._sessions = sessions
        self._source = source

    def poll(self, limit: int = 100) -> int:
        """Bridge materialized files whose external runs have completed."""
        if limit < 1:
            raise ValueError('limit must be positive')
        with self._sessions() as session:
            rows = session.scalars(
                select(IngressObjectRow)
                .where(
                    IngressObjectRow.status == IngressStatus.MATERIALIZED,
                    IngressObjectRow.sha256.is_not(None),
                    IngressObjectRow.last_error.is_(None),
                )
                .order_by(IngressObjectRow.created_at)
                .limit(limit)
            ).all()
            candidates = [(row.object_id, row.sha256) for row in rows]
        bridged = 0
        for object_id, digest in candidates:
            if digest is None:
                continue
            run = self._source.by_sha256(digest)
            if (
                run is None
                or run.status != 'completed'
                or run.sha256 != digest
            ):
                continue
            try:
                coverage = Coverage(run.uf)
                disease = resolve_run_disease(run.disease, run.canonical_cid10)
                epiweek = Epiweek.parse(run.delivery_se)
            except ValueError as exc:
                with self._sessions.begin() as session:
                    ingress = session.get(IngressObjectRow, object_id)
                    if ingress is not None:
                        ingress.last_error = (
                            f'invalid completed run metadata: {exc}'
                        )[:512]
                logger.error(
                    'completed_ingestion_metadata_invalid',
                    object_id=str(object_id),
                    sha256=run.sha256,
                    run_id=str(run.run_id),
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                continue
            event = IngestionCompleted.new(
                run_id=run.run_id,
                disease=disease,
                coverage=coverage,
                epiweek=epiweek,
                fingerprint=run.sha256,
                completed_at=run.finished_at,
            )
            with self._sessions.begin() as session:
                ingress = session.scalar(
                    select(IngressObjectRow)
                    .where(IngressObjectRow.object_id == object_id)
                    .with_for_update()
                )
                if ingress is None or ingress.status is IngressStatus.INGESTED:
                    continue
                if ingress.sha256 != run.sha256:
                    continue
                SqlOutboxRepository(session).add(event)
                ingress.run_id = run.run_id
                ingress.status = IngressStatus.INGESTED
            bridged += 1
            logger.info(
                'completed_ingestion_bridged',
                object_id=str(object_id),
                sha256=run.sha256,
                run_id=str(run.run_id),
                event_id=str(event.event_id),
                coverage=coverage.value,
                epiweek=str(epiweek),
            )
        return bridged


def build_source(database_url: str) -> PostgresCompletedIngestionSource:
    """Create the separately configured read-only source connection."""
    engine = create_engine(database_url, pool_pre_ping=True)
    return PostgresCompletedIngestionSource(engine)
