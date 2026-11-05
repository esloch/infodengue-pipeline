"""Idempotent ingestion and coordination use cases."""

from dataclasses import replace
from typing import Callable
from uuid import uuid4

from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    DEFAULT_BR_REQUIRED_DISEASES,
    DEFAULT_UF_REQUIRED_DISEASES,
    Coverage,
    Disease,
    Epiweek,
    IngestionInput,
    Readiness,
    WorkflowSnapshot,
    WorkflowStatus,
)
from infodengue_pipeline.protocols import (
    EventPublisher,
    UnitOfWork,
)


class RecordIngestionCompletion:
    """Write an ingestion event into the current database transaction."""

    def __init__(self, unit_of_work: Callable[[], UnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def execute(self, event: IngestionCompleted) -> bool:
        """Persist a producer event once in its own transaction."""
        with self._unit_of_work() as work:
            inserted = work.outbox.add(event)
            work.commit()
        return inserted


class CoordinateEpiweek:
    """Persist coverage and epiweek readiness without scheduling analysis."""

    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        br_required_diseases: frozenset[
            Disease
        ] = DEFAULT_BR_REQUIRED_DISEASES,
        uf_required_diseases: frozenset[
            Disease
        ] = DEFAULT_UF_REQUIRED_DISEASES,
        initial_analysis_week: Epiweek | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._initial_analysis_week = initial_analysis_week
        self._required_by_coverage = {
            Coverage.BR: frozenset(br_required_diseases),
            **{
                coverage: frozenset(uf_required_diseases)
                for coverage in Coverage
                if not coverage.is_national
            },
        }
        if any(
            not diseases for diseases in self._required_by_coverage.values()
        ):
            raise ValueError('required diseases must not be empty')

    def record(self, event: IngestionCompleted) -> WorkflowSnapshot | None:
        """Rebuild readiness from durable deliveries after one completion."""
        with self._unit_of_work() as work:
            work.outbox.add(event)
            events = work.workflows.completed_ingestions(event.coverage)
            if all(existing.event_id != event.event_id for existing in events):
                events.append(event)
            updated = self._rebuild(work, event.coverage, events)
            work.commit()
        return updated

    def reconcile_all(self) -> list[WorkflowSnapshot]:
        """Rebuild each next coverage candidate from durable event history."""
        if self._initial_analysis_week is None:
            return []
        with self._unit_of_work() as work:
            events = work.workflows.completed_ingestions()
            snapshots = [
                snapshot
                for coverage in sorted(
                    {event.coverage for event in events},
                    key=lambda item: item.value,
                )
                if (
                    snapshot := self._rebuild(
                        work,
                        coverage,
                        [
                            event
                            for event in events
                            if event.coverage is coverage
                        ],
                    )
                )
                is not None
            ]
            work.commit()
        return snapshots

    def _rebuild(
        self,
        work: UnitOfWork,
        coverage: Coverage,
        events: list[IngestionCompleted],
    ) -> WorkflowSnapshot | None:
        """Select current eligible inputs for one contiguous candidate week."""
        if self._initial_analysis_week is None:
            return None
        last_processed = work.workflows.last_persisted_analysis_week(
            coverage, self._initial_analysis_week
        )
        epiweek = (
            last_processed.next()
            if last_processed is not None
            else self._initial_analysis_week
        )
        eligible = [
            event
            for event in events
            if event.coverage is coverage
            and Epiweek.parse(event.epiweek) >= epiweek
        ]
        if not eligible:
            return None
        required_diseases = self._required_by_coverage[coverage]
        current = work.workflows.get_for_update(coverage, epiweek)
        readiness = Readiness(
            coverage,
            epiweek,
            required_diseases=required_diseases,
        )
        for event in sorted(
            eligible,
            key=lambda item: (item.completed_at, item.run_id.int),
        ):
            readiness = readiness.with_input(
                event.disease,
                IngestionInput(
                    event.run_id, event.fingerprint, event.completed_at
                ),
            )
        if current is None:
            current = WorkflowSnapshot(
                workflow_id=uuid4(),
                readiness=readiness,
                status=WorkflowStatus.WAITING,
            )
        fingerprint = readiness.input_fingerprint
        if not readiness.ready:
            status = WorkflowStatus.WAITING
        elif fingerprint == current.queued_fingerprint:
            status = current.status
        else:
            status = WorkflowStatus.READY
        updated = replace(current, readiness=readiness, status=status)
        work.workflows.save(updated)
        return updated

    def claim_ready(
        self, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Claim current inputs at most once per fingerprint."""
        with self._unit_of_work() as work:
            current = work.workflows.get_for_update(coverage, epiweek)
            if current is None:
                return None
            readiness = current.readiness.with_required_diseases(
                self._required_by_coverage[coverage]
            )
            if (
                current.status is not WorkflowStatus.READY
                or not readiness.ready
            ):
                return None
            fingerprint = readiness.input_fingerprint
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
