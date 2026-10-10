"""SQLAlchemy implementations of pipeline repositories."""

from datetime import datetime, timezone
from uuid import UUID

from infodengue_pipeline.adapters.postgres.models import (
    OutboxEventRow,
    OutboxStatus,
    WorkflowRow,
)
from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.events import IngestionCompleted
from infodengue_pipeline.domain.workflow import IngestionInput, Readiness
from infodengue_pipeline.ports.contracts import WorkflowSnapshot
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session


class SqlWorkflowRepository:
    """Lock and persist epiweek workflow state."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_for_update(self, epiweek: Epiweek) -> WorkflowSnapshot | None:
        """Serialize writers for a week, including its first row insertion."""
        if self._session.bind is not None and (
            self._session.bind.dialect.name == 'postgresql'
        ):
            self._session.execute(
                select(func.pg_advisory_xact_lock(int(epiweek)))
            )
        row = self._session.scalar(
            select(WorkflowRow)
            .where(WorkflowRow.epiweek == int(epiweek))
            .with_for_update()
        )
        if row is None:
            return None
        dengue = (
            IngestionInput(row.dengue_run_id, row.dengue_fingerprint)
            if row.dengue_run_id is not None
            and row.dengue_fingerprint is not None
            else None
        )
        chik = (
            IngestionInput(row.chikungunya_run_id, row.chikungunya_fingerprint)
            if row.chikungunya_run_id is not None
            and row.chikungunya_fingerprint is not None
            else None
        )
        return WorkflowSnapshot(
            workflow_id=row.workflow_id,
            readiness=Readiness(epiweek, dengue, chik),
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
                epiweek=int(readiness.epiweek),
            )
            self._session.add(row)
        row.dengue_run_id = (
            readiness.dengue.run_id if readiness.dengue else None
        )
        row.dengue_fingerprint = (
            readiness.dengue.fingerprint if readiness.dengue else None
        )
        row.chikungunya_run_id = (
            readiness.chikungunya.run_id if readiness.chikungunya else None
        )
        row.chikungunya_fingerprint = (
            readiness.chikungunya.fingerprint
            if readiness.chikungunya
            else None
        )
        row.status = workflow.status
        row.queued_fingerprint = workflow.queued_fingerprint


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
