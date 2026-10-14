"""SQLAlchemy implementations of pipeline repositories."""

import hashlib

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from infodengue_pipeline.db.models import (
    OutboxEventRow,
    OutboxStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    Coverage,
    Epiweek,
    IngestionInput,
    Readiness,
    WorkflowSnapshot,
)


class SqlWorkflowRepository:
    """Lock and persist epiweek workflow state."""

    def __init__(self, session: Session) -> None:
        self._session = session

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
