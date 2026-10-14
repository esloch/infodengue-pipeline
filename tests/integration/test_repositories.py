"""Repository and application transactions against isolated SQL state."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from infodengue_pipeline.db.models import (
    OutboxEventRow,
    OutboxStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.models import (
    DEFAULT_BR_REQUIRED_DISEASES,
    Coverage,
    Disease,
    Epiweek,
    WorkflowStatus,
)
from infodengue_pipeline.services import (
    CoordinateEpiweek,
    PublishOutbox,
    RecordIngestionCompletion,
)
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
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
    assert coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639)) is None
    ready = coordinator.record(chik)
    assert ready.status is WorkflowStatus.READY
    claimed = coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert claimed is not None
    assert claimed.status is WorkflowStatus.QUEUED
    assert claimed.queued_fingerprint == ready.readiness.input_fingerprint
    assert coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639)) is None
    assert coordinator.record(chik).status is WorkflowStatus.QUEUED
    with sessions() as session:
        assert len(session.scalars(select(WorkflowRow)).all()) == 1
        assert len(session.scalars(select(WorkflowInputRow)).all()) == 2


@pytest.mark.parametrize(
    ('diseases', 'required_diseases', 'expected'),
    [
        ((Disease.DENGUE,), None, WorkflowStatus.WAITING),
        ((Disease.CHIKUNGUNYA,), None, WorkflowStatus.WAITING),
        ((Disease.ZIKA,), None, WorkflowStatus.WAITING),
        ((Disease.DENGUE, Disease.ZIKA), None, WorkflowStatus.WAITING),
        ((Disease.CHIKUNGUNYA, Disease.ZIKA), None, WorkflowStatus.WAITING),
        (
            (Disease.DENGUE, Disease.CHIKUNGUNYA),
            None,
            WorkflowStatus.READY,
        ),
        (
            (Disease.DENGUE, Disease.CHIKUNGUNYA, Disease.ZIKA),
            None,
            WorkflowStatus.READY,
        ),
        (
            (Disease.DENGUE, Disease.CHIKUNGUNYA),
            frozenset((*DEFAULT_BR_REQUIRED_DISEASES, Disease.ZIKA)),
            WorkflowStatus.WAITING,
        ),
        (
            (Disease.DENGUE, Disease.CHIKUNGUNYA, Disease.ZIKA),
            frozenset((*DEFAULT_BR_REQUIRED_DISEASES, Disease.ZIKA)),
            WorkflowStatus.READY,
        ),
    ],
)
def test_configurable_disease_readiness(
    sessions: sessionmaker[Session],
    work_factory,
    completed_event,
    diseases: tuple[Disease, ...],
    required_diseases: frozenset[Disease] | None,
    expected: WorkflowStatus,
) -> None:
    """Persist all inputs while applying the configured readiness set."""
    kwargs = (
        {'br_required_diseases': required_diseases}
        if required_diseases is not None
        else {}
    )
    coordinator = CoordinateEpiweek(work_factory, **kwargs)
    snapshot = None
    for disease in diseases:
        snapshot = coordinator.record(
            completed_event(disease, fingerprint=f'{disease.value}-source')
        )
    assert snapshot is not None
    assert snapshot.status is expected
    assert snapshot.readiness.received_diseases == frozenset(diseases)
    with sessions() as session:
        assert len(session.scalars(select(WorkflowInputRow)).all()) == len(
            diseases
        )


def test_repeated_same_disease_event_remains_one_input(
    sessions: sessionmaker[Session], work_factory, completed_event
) -> None:
    """Repeated delivery does not duplicate a disease input row."""
    coordinator = CoordinateEpiweek(work_factory)
    event = completed_event(Disease.ZIKA)
    first = coordinator.record(event)
    second = coordinator.record(event)
    assert first.status is WorkflowStatus.WAITING
    assert second.status is WorkflowStatus.WAITING
    assert second.readiness.inputs[Disease.ZIKA].run_id == event.run_id
    with sessions() as session:
        rows = session.scalars(select(WorkflowInputRow)).all()
        assert len(rows) == 1
        assert rows[0].disease is Disease.ZIKA


def test_workflow_input_rejects_duplicate_disease(
    sessions: sessionmaker[Session],
) -> None:
    """The database enforces one input row per workflow disease."""
    workflow_id = uuid4()
    with sessions() as session:
        session.add(
            WorkflowRow(
                workflow_id=workflow_id,
                coverage=Coverage.BR,
                epiweek=202639,
                status=WorkflowStatus.WAITING,
            )
        )
        session.flush()
        session.add_all(
            [
                WorkflowInputRow(
                    workflow_input_id=uuid4(),
                    workflow_id=workflow_id,
                    disease=Disease.ZIKA,
                    run_id=uuid4(),
                    fingerprint=f'fingerprint-{index}',
                    completed_at=datetime.now(timezone.utc),
                )
                for index in range(2)
            ]
        )
        with pytest.raises(IntegrityError):
            session.commit()


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
    assert coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639)) is None
    assert coordinator.claim_ready(Coverage.BR, Epiweek.parse(202638)) is None


def test_new_input_fingerprint_reopens_readiness(
    work_factory, completed_event
) -> None:
    """A new ingestion identity can cause a new analysis claim."""
    coordinator = CoordinateEpiweek(work_factory)
    dengue = completed_event(Disease.DENGUE)
    coordinator.record(dengue)
    coordinator.record(completed_event(Disease.CHIKUNGUNYA))
    first = coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert first is not None
    changed = dengue.model_copy(
        update={
            'event_id': uuid4(),
            'run_id': uuid4(),
            'fingerprint': 'new-source',
            'completed_at': dengue.completed_at + timedelta(seconds=1),
        }
    )
    updated = coordinator.record(changed)
    assert updated.status is WorkflowStatus.READY
    assert updated.readiness.input_fingerprint != first.queued_fingerprint


def test_stale_event_does_not_reopen_queued_or_regress_ready(
    work_factory, completed_event
) -> None:
    """Delayed older inputs cannot replace a newer queued or ready input."""
    coordinator = CoordinateEpiweek(work_factory)
    old = completed_event(
        Disease.DENGUE,
        completed_at=datetime(2026, 10, 13, tzinfo=timezone.utc),
    )
    chik = completed_event(
        Disease.CHIKUNGUNYA,
        completed_at=datetime(2026, 10, 13, tzinfo=timezone.utc),
    )
    coordinator.record(old)
    coordinator.record(chik)
    first = coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert first is not None
    new = completed_event(
        Disease.DENGUE,
        fingerprint='new',
        completed_at=datetime(2026, 10, 14, tzinfo=timezone.utc),
    )
    ready = coordinator.record(new)
    assert ready.status is WorkflowStatus.READY
    assert coordinator.record(old).readiness == ready.readiness
    second = coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert second is not None
    stale = coordinator.record(old)
    assert stale.status is WorkflowStatus.QUEUED
    assert stale.queued_fingerprint == second.queued_fingerprint
    assert stale.readiness.inputs[Disease.DENGUE].run_id == new.run_id


def test_coverage_and_epiweek_form_distinct_workflow_identity(
    sessions: sessionmaker[Session], work_factory, completed_event
) -> None:
    """Same week deliveries remain separated by national and state coverage."""
    coordinator = CoordinateEpiweek(work_factory)
    mg_dengue = coordinator.record(
        completed_event(Disease.DENGUE, coverage=Coverage.MG)
    )
    br_chik = coordinator.record(
        completed_event(Disease.CHIKUNGUNYA, coverage=Coverage.BR)
    )
    assert mg_dengue.status is WorkflowStatus.READY
    assert br_chik.status is WorkflowStatus.WAITING
    assert coordinator.claim_ready(Coverage.BR, Epiweek.parse(202639)) is None
    with sessions() as session:
        rows = session.scalars(select(WorkflowRow)).all()
        assert {(row.coverage, row.epiweek) for row in rows} == {
            (Coverage.MG, 202639),
            (Coverage.BR, 202639),
        }


def test_different_ufs_never_combine(work_factory, completed_event) -> None:
    """MG dengue cannot combine with SP chikungunya."""
    coordinator = CoordinateEpiweek(work_factory)
    mg = coordinator.record(
        completed_event(Disease.DENGUE, coverage=Coverage.MG)
    )
    sp = coordinator.record(
        completed_event(Disease.CHIKUNGUNYA, coverage=Coverage.SP)
    )
    assert mg.status is WorkflowStatus.READY
    assert sp.status is WorkflowStatus.WAITING
    assert coordinator.claim_ready(Coverage.SP, Epiweek.parse(202639)) is None


def test_mg_chikungunya_alone_waits(work_factory, completed_event) -> None:
    """State chikungunya is optional and cannot satisfy dengue readiness."""
    coordinator = CoordinateEpiweek(work_factory)
    state_chik = coordinator.record(
        completed_event(Disease.CHIKUNGUNYA, coverage=Coverage.MG)
    )
    assert state_chik.status is WorkflowStatus.WAITING
