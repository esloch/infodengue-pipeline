"""Read-only AlertaDengue completion boundary and idempotent event bridge."""

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


class PostgresCompletedIngestionSource:
    """Query the external PostgreSQL contract without importing Django."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

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
            row = (
                connection.execute(
                    text(
                        'SELECT status FROM ingestion.run '
                        'WHERE sha256 = :sha256'
                    ),
                    {'sha256': digest},
                )
                .mappings()
                .first()
            )
        return str(row['status']) if row is not None else None

    def by_sha256(self, digest: str) -> CompletedRun | None:
        """Read one completed run by the unique file SHA-256."""
        with self._engine.connect() as connection:
            connection.execute(text('SET TRANSACTION READ ONLY'))
            row = (
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
                .first()
            )
        if row is None:
            return None
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
