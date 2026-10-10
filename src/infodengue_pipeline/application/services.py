"""Idempotent ingestion and coordination use cases."""

from dataclasses import replace
from typing import Callable
from uuid import uuid4

from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.events import IngestionCompleted
from infodengue_pipeline.domain.workflow import (
    IngestionInput,
    Readiness,
    WorkflowStatus,
)
from infodengue_pipeline.ports.contracts import (
    EventPublisher,
    UnitOfWork,
    WorkflowSnapshot,
)


class RecordIngestionCompletion:
    """Write an ingestion event into the current database transaction."""

    def __init__(self, unit_of_work: Callable[[], UnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def execute(self, event: IngestionCompleted) -> bool:
        """Persist an event once; future merge work shares this transaction."""
        with self._unit_of_work() as work:
            inserted = work.outbox.add(event)
            work.commit()
        return inserted


class CoordinateEpiweek:
    """Persist same epiweek readiness without scheduling scientific work."""

    def __init__(self, unit_of_work: Callable[[], UnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def record(self, event: IngestionCompleted) -> WorkflowSnapshot:
        """Apply an ingestion event idempotently to one epiweek."""
        epiweek = Epiweek.parse(event.epiweek)
        with self._unit_of_work() as work:
            current = work.workflows.get_for_update(epiweek)
            if current is None:
                current = WorkflowSnapshot(
                    workflow_id=uuid4(),
                    readiness=Readiness(epiweek),
                    status=WorkflowStatus.WAITING,
                )
            incoming = IngestionInput(event.run_id, event.fingerprint)
            readiness = current.readiness.with_input(event.disease, incoming)
            fingerprint = readiness.input_fingerprint
            if not readiness.ready:
                status = WorkflowStatus.WAITING
            elif fingerprint == current.queued_fingerprint:
                status = current.status
            else:
                status = WorkflowStatus.READY
            updated = replace(
                current,
                readiness=readiness,
                status=status,
            )
            work.workflows.save(updated)
            work.commit()
        return updated

    def claim_ready(self, epiweek: Epiweek) -> WorkflowSnapshot | None:
        """Claim current inputs at most once per fingerprint."""
        with self._unit_of_work() as work:
            current = work.workflows.get_for_update(epiweek)
            if current is None or current.status is not WorkflowStatus.READY:
                return None
            fingerprint = current.readiness.input_fingerprint
            if fingerprint is None:
                return None
            queued = replace(
                current,
                status=WorkflowStatus.QUEUED,
                queued_fingerprint=fingerprint,
            )
            work.workflows.save(queued)
            work.commit()
        return queued


class PublishOutbox:
    """Publish pending events with at least once delivery semantics."""

    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        publisher: EventPublisher,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._publisher = publisher

    def execute(self, limit: int = 100) -> int:
        """Mark rows published only after confirmed broker publication."""
        if limit < 1:
            raise ValueError('limit must be positive')
        with self._unit_of_work() as work:
            events = work.outbox.pending(limit)
            for event in events:
                self._publisher.publish(event)
                work.outbox.mark_published(event.event_id)
            work.commit()
        return len(events)
