"""Repository and application transactions against isolated SQL state."""

from uuid import uuid4

import pytest

from infodengue_pipeline.adapters.postgres.models import (
    OutboxEventRow,
    OutboxStatus,
    WorkflowRow,
)
from infodengue_pipeline.application.services import (
    CoordinateEpiweek,
    PublishOutbox,
    RecordIngestionCompletion,
)
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.workflow import WorkflowStatus
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker


class FakePublisher:
    """A publisher that can fail before confirming a message."""

    def __init__(self, fail: bool = False) -> None:
        self.events: list[object] = []
        self.fail = fail

    def publish(self, event: object) -> None:
        """Record confirmed publications or simulate a broker failure."""
        if self.fail:
            raise RuntimeError('broker unavailable')
        self.events.append(event)


def test_outbox_duplicate_and_publication(
    sessions: sessionmaker[Session], work_factory, completed_event
) -> None:
    """One event identity persists once and publishes once when confirmed."""
    event = completed_event()
    recorder = RecordIngestionCompletion(work_factory)
    assert recorder.execute(event)
    assert not recorder.execute(event)
    with sessions() as session:
        row = session.get(OutboxEventRow, event.event_id)
        assert row is not None
        assert row.status is OutboxStatus.PENDING
        assert row.published_at is None
        assert row.payload['disease'] == 'A90'
    publisher = FakePublisher()
    service = PublishOutbox(work_factory, publisher)
    assert service.execute() == 1
    assert service.execute() == 0
    assert publisher.events == [event]
    with sessions() as session:
        row = session.get(OutboxEventRow, event.event_id)
        assert row is not None
        assert row.status is OutboxStatus.PUBLISHED
        assert row.published_at is not None


def test_outbox_failure_keeps_pending(
    sessions: sessionmaker[Session], work_factory, completed_event
) -> None:
    """A failed broker confirmation cannot advance outbox state."""
    event = completed_event()
    RecordIngestionCompletion(work_factory).execute(event)
    with pytest.raises(RuntimeError, match='broker unavailable'):
        PublishOutbox(work_factory, FakePublisher(fail=True)).execute()
    with sessions() as session:
        row = session.get(OutboxEventRow, event.event_id)
        assert row is not None
        assert row.status is OutboxStatus.PENDING


def test_epiweek_readiness_and_idempotent_claim(
    sessions: sessionmaker[Session], work_factory, completed_event
) -> None:
    """A90 and A92 of one week are required before one claim succeeds."""
    coordinator = CoordinateEpiweek(work_factory)
    dengue = completed_event(Disease.DENGUE)
    chik = completed_event(Disease.CHIKUNGUNYA)
    waiting = coordinator.record(dengue)
    assert waiting.status is WorkflowStatus.WAITING
    assert coordinator.record(dengue).status is WorkflowStatus.WAITING
    assert coordinator.claim_ready(Epiweek.parse(202639)) is None
    ready = coordinator.record(chik)
    assert ready.status is WorkflowStatus.READY
    claimed = coordinator.claim_ready(Epiweek.parse(202639))
    assert claimed is not None
    assert claimed.status is WorkflowStatus.QUEUED
    assert claimed.queued_fingerprint == ready.readiness.input_fingerprint
    assert coordinator.claim_ready(Epiweek.parse(202639)) is None
    assert coordinator.record(chik).status is WorkflowStatus.QUEUED
    with sessions() as session:
        assert len(session.scalars(select(WorkflowRow)).all()) == 1


def test_different_epiweeks_never_combine(
    work_factory, completed_event
) -> None:
    """A90 and A92 from different weeks remain independently waiting."""
    coordinator = CoordinateEpiweek(work_factory)
    assert coordinator.record(completed_event(Disease.DENGUE)).status is (
        WorkflowStatus.WAITING
    )
    assert (
        coordinator.record(
            completed_event(Disease.CHIKUNGUNYA, week=202638)
        ).status
        is WorkflowStatus.WAITING
    )
    assert coordinator.claim_ready(Epiweek.parse(202639)) is None
    assert coordinator.claim_ready(Epiweek.parse(202638)) is None


def test_new_input_fingerprint_reopens_readiness(
    work_factory, completed_event
) -> None:
    """A new ingestion identity can cause a new analysis claim."""
    coordinator = CoordinateEpiweek(work_factory)
    dengue = completed_event(Disease.DENGUE)
    coordinator.record(dengue)
    coordinator.record(completed_event(Disease.CHIKUNGUNYA))
    first = coordinator.claim_ready(Epiweek.parse(202639))
    assert first is not None
    changed = dengue.model_copy(
        update={
            'event_id': uuid4(),
            'run_id': uuid4(),
            'fingerprint': 'new-source',
        }
    )
    updated = coordinator.record(changed)
    assert updated.status is WorkflowStatus.READY
    assert updated.readiness.input_fingerprint != first.queued_fingerprint
