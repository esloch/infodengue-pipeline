"""SQLAlchemy implementations of pipeline repositories."""

import hashlib

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    BulletinJobRow,
    OutboxEventRow,
    OutboxStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    BulletinJobStatus,
    BulletinKind,
    Coverage,
    Epiweek,
    IngestionInput,
    Readiness,
    WorkflowSnapshot,
    WorkflowStatus,
)


class SqlWorkflowRepository:
    """Lock and persist epiweek workflow state."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get(
        self, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Read one workflow without taking a writer lock."""
        row = self._session.scalar(
            select(WorkflowRow).where(
                WorkflowRow.coverage == coverage,
                WorkflowRow.epiweek == int(epiweek),
            )
        )
        return self._snapshot(row, coverage, epiweek)

    def last_persisted_analysis_week(
        self, coverage: Coverage, initial_week: Epiweek
    ) -> Epiweek | None:
        """Return the contiguous cursor backed by current successful jobs."""
        if self._session.bind is not None and (
            self._session.bind.dialect.name == 'postgresql'
        ):
            lock_key = int.from_bytes(
                hashlib.sha256(
                    f'analysis-cursor:{coverage.value}'.encode()
                ).digest()[:8],
                'big',
                signed=True,
            )
            self._session.execute(select(func.pg_advisory_xact_lock(lock_key)))
        weeks = self._session.scalars(
            select(WorkflowRow.epiweek)
            .join(
                AnalysisJobRow,
                AnalysisJobRow.workflow_id == WorkflowRow.workflow_id,
            )
            .where(
                WorkflowRow.coverage == coverage,
                WorkflowRow.status == WorkflowStatus.SUCCEEDED,
                AnalysisJobRow.input_fingerprint
                == WorkflowRow.queued_fingerprint,
                AnalysisJobRow.status == AnalysisJobStatus.SUCCEEDED,
                AnalysisJobRow.load.is_(True),
            )
        ).all()
        completed = set(weeks)
        cursor: Epiweek | None = None
        candidate = initial_week
        while int(candidate) in completed:
            cursor = candidate
            candidate = candidate.next()
        return cursor

    def completed_ingestions(
        self, coverage: Coverage | None = None
    ) -> list[IngestionCompleted]:
        """Read completed deliveries from the retained versioned outbox."""
        rows = self._session.scalars(
            select(OutboxEventRow)
            .where(OutboxEventRow.event_type == 'sinan.ingestion.completed')
            .order_by(OutboxEventRow.created_at, OutboxEventRow.event_id)
        ).all()
        events = [
            IngestionCompleted.model_validate(row.payload) for row in rows
        ]
        return [
            event
            for event in events
            if coverage is None or event.coverage is coverage
        ]

    def get_for_update(
        self, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Serialize writers for a week, including its first row insertion."""
        if self._session.bind is not None and (
            self._session.bind.dialect.name == 'postgresql'
        ):
            lock_key = int.from_bytes(
                hashlib.sha256(
                    f'{coverage.value}:{epiweek}'.encode()
                ).digest()[:8],
                'big',
                signed=True,
            )
            self._session.execute(select(func.pg_advisory_xact_lock(lock_key)))
        row = self._session.scalar(
            select(WorkflowRow)
            .where(
                WorkflowRow.coverage == coverage,
                WorkflowRow.epiweek == int(epiweek),
            )
            .with_for_update()
        )
        return self._snapshot(row, coverage, epiweek)

    @staticmethod
    def _snapshot(
        row: WorkflowRow | None, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Translate a loaded workflow to its domain snapshot."""
        if row is None:
            return None
        inputs = {
            input_row.disease: IngestionInput(
                input_row.run_id,
                input_row.fingerprint,
                input_row.completed_at.replace(tzinfo=timezone.utc)
                if input_row.completed_at.tzinfo is None
                else input_row.completed_at.astimezone(timezone.utc),
            )
            for input_row in row.inputs
        }
        return WorkflowSnapshot(
            workflow_id=row.workflow_id,
            readiness=Readiness(coverage, epiweek, inputs),
            status=row.status,
            queued_fingerprint=row.queued_fingerprint,
        )

    def save(self, workflow: WorkflowSnapshot) -> None:
        """Insert or update one workflow under its epiweek lock."""
        readiness = workflow.readiness
        row = self._session.get(WorkflowRow, workflow.workflow_id)
        if row is None:
            row = WorkflowRow(
                workflow_id=workflow.workflow_id,
                coverage=readiness.coverage,
                epiweek=int(readiness.epiweek),
            )
            self._session.add(row)
        existing_inputs = {item.disease: item for item in row.inputs}
        for item in tuple(row.inputs):
            if item.disease not in readiness.inputs:
                row.inputs.remove(item)
        for disease, incoming in readiness.inputs.items():
            existing_item = existing_inputs.get(disease)
            if existing_item is None:
                row.inputs.append(
                    WorkflowInputRow(
                        disease=disease,
                        run_id=incoming.run_id,
                        fingerprint=incoming.fingerprint,
                        completed_at=incoming.completed_at,
                    )
                )
            else:
                existing_item.run_id = incoming.run_id
                existing_item.fingerprint = incoming.fingerprint
                existing_item.completed_at = incoming.completed_at
        row.status = workflow.status
        row.queued_fingerprint = workflow.queued_fingerprint


class SqlBulletinJobRepository:
    """Persist one Recife bulletin row per scientific and renderer identity."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, job_id: UUID) -> BulletinJobRow | None:
        """Load one persisted bulletin job by its durable identifier."""
        return self._session.get(BulletinJobRow, job_id)

    def get_by_identity(
        self,
        *,
        kind: BulletinKind,
        municipality_geocode: int,
        analysis_job_id: UUID,
        renderer_revision: str,
    ) -> BulletinJobRow | None:
        """Load the one row for a complete bulletin identity."""
        return self._session.scalar(
            select(BulletinJobRow).where(
                BulletinJobRow.kind == kind,
                BulletinJobRow.municipality_geocode == municipality_geocode,
                BulletinJobRow.analysis_job_id == analysis_job_id,
                BulletinJobRow.renderer_revision == renderer_revision,
            )
        )

    def get_or_create(
        self,
        *,
        kind: BulletinKind,
        municipality_geocode: int,
        analysis_job_id: UUID,
        renderer_revision: str,
    ) -> BulletinJobRow:
        """Insert once under the PostgreSQL identity constraint."""
        if kind is not BulletinKind.RECIFE_INTRAMUNICIPAL:
            raise ValueError('unsupported bulletin kind')
        if municipality_geocode != 2611606:
            raise ValueError('Recife municipality geocode is required')
        if (
            not renderer_revision
            or renderer_revision.strip() != renderer_revision
            or len(renderer_revision) > 128
        ):
            raise ValueError('renderer revision must be 1-128 nonblank chars')
        analysis = self._session.get(AnalysisJobRow, analysis_job_id)
        if analysis is None:
            raise ValueError('analysis job does not exist')
        workflow = self._session.get(WorkflowRow, analysis.workflow_id)
        if workflow is None or workflow.coverage is not Coverage.PE:
            raise ValueError('Recife bulletin requires PE analysis coverage')
        if self._session.bind is None or (
            self._session.bind.dialect.name != 'postgresql'
        ):
            raise RuntimeError('bulletin job persistence requires PostgreSQL')

        new_id = uuid4()
        statement = (
            pg_insert(BulletinJobRow)
            .values(
                job_id=new_id,
                kind=kind,
                municipality_geocode=municipality_geocode,
                analysis_job_id=analysis_job_id,
                renderer_revision=renderer_revision,
                status=BulletinJobStatus.QUEUED,
                attempts=0,
            )
            .on_conflict_do_nothing(constraint='uq_bulletin_job_identity')
            .returning(BulletinJobRow.job_id)
        )
        persisted_id = self._session.scalar(statement)
        if persisted_id is not None:
            row = self.get(persisted_id)
        else:
            row = self.get_by_identity(
                kind=kind,
                municipality_geocode=municipality_geocode,
                analysis_job_id=analysis_job_id,
                renderer_revision=renderer_revision,
            )
        if row is None:
            raise RuntimeError('bulletin job insert did not persist a row')
        return row


class SqlOutboxRepository:
    """Durable outbox with duplicate event and publisher safety."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, event: IngestionCompleted) -> bool:
        """Insert an event UUID once, safe against concurrent redelivery."""
        values = {
            'event_id': event.event_id,
            'event_type': event.event_type,
            'version': event.version,
            'payload': event.model_dump(mode='json'),
            'status': OutboxStatus.PENDING,
        }
        dialect = self._session.bind.dialect.name if self._session.bind else ''
        if dialect == 'postgresql':
            statement_pg = (
                pg_insert(OutboxEventRow)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[OutboxEventRow.event_id]
                )
                .returning(OutboxEventRow.event_id)
            )
            return self._session.scalar(statement_pg) is not None
        if dialect == 'sqlite':
            statement_sqlite = (
                sqlite_insert(OutboxEventRow)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[OutboxEventRow.event_id]
                )
                .returning(OutboxEventRow.event_id)
            )
            return self._session.scalar(statement_sqlite) is not None
        raise RuntimeError(f'Unsupported database dialect: {dialect}')

    def pending(self, limit: int) -> list[IngestionCompleted]:
        """Lock pending rows and decode versioned payloads."""
        rows = self._session.scalars(
            select(OutboxEventRow)
            .where(OutboxEventRow.status == OutboxStatus.PENDING)
            .order_by(OutboxEventRow.created_at, OutboxEventRow.event_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        ).all()
        return [IngestionCompleted.model_validate(row.payload) for row in rows]

    def mark_published(self, event_id: UUID) -> None:
        """Store broker confirmation time for one pending event."""
        self._session.execute(
            update(OutboxEventRow)
            .where(
                OutboxEventRow.event_id == event_id,
                OutboxEventRow.status == OutboxStatus.PENDING,
            )
            .values(
                status=OutboxStatus.PUBLISHED,
                published_at=datetime.now(timezone.utc),
            )
        )
