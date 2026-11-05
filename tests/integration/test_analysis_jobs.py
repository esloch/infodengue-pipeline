"""Persisted analysis job transitions and stale completion protection."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from uuid import UUID, uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    Base,
    WorkflowRow,
)
from infodengue_pipeline.db.repositories import SqlWorkflowRepository
from infodengue_pipeline.models import (
    AnalysisJobResult,
    AnalysisJobStatus,
    Coverage,
    Disease,
    Epiweek,
    WorkflowStatus,
)
from infodengue_pipeline.services import (
    CoordinateEpiweek,
    RecordIngestionCompletion,
)
from infodengue_pipeline.workers import tasks
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


class FakeRunner:
    """Return a finite successful or failed result."""

    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.requests = []

    def run(self, request):
        """Return the configured finite result."""
        self.requests.append(request)
        return AnalysisJobResult(
            request.job_id, Path('/tmp/output'), self.success
        )


def ready_workflow(work_factory, completed_event) -> None:
    """Create a BR workflow with both required disease inputs."""
    coordinator = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202639)
    )
    coordinator.record(completed_event(Disease.DENGUE))
    coordinator.record(completed_event(Disease.CHIKUNGUNYA))


def test_next_analysis_week_uses_persisted_cursor_without_skipping(
    sessions, work_factory, completed_event
) -> None:
    """Delivery 644 feeds the immediate week after persisted execution."""
    for processed, candidate in ((202643, 202644), (202641, 202642)):
        with sessions.begin() as session:
            workflow = WorkflowRow(
                coverage=Coverage.BR,
                epiweek=processed,
                status=WorkflowStatus.SUCCEEDED,
                queued_fingerprint='persisted',
            )
            session.add(workflow)
            session.flush()
            session.add(
                AnalysisJobRow(
                    job_id=uuid4(),
                    workflow_id=workflow.workflow_id,
                    input_fingerprint='persisted',
                    status=AnalysisJobStatus.SUCCEEDED,
                    image='analysis:test',
                    cores=1,
                    load=True,
                    output_dir='/tmp/completed',
                )
            )
            coordinator = CoordinateEpiweek(
                work_factory, initial_analysis_week=Epiweek.parse(candidate)
            )
        first = coordinator.record(
            completed_event(Disease.DENGUE, week=202644)
        )
        ready = coordinator.record(
            completed_event(Disease.CHIKUNGUNYA, week=202644)
        )
        assert first is not None and ready is not None
        assert int(ready.readiness.epiweek) == candidate
        with sessions.begin() as session:
            session.query(AnalysisJobRow).delete()
            session.query(WorkflowRow).delete()


def test_delivery_before_candidate_does_not_create_workflow(
    work_factory, completed_event
) -> None:
    """A delivery below the pending week cannot feed it."""
    coordinator = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202644)
    )
    assert coordinator.record(completed_event(week=202643)) is None


def test_one_delivery_advances_pending_weeks_without_new_events(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """Durable delivery 644 can feed consecutive candidates 642 and 643."""
    recorder = RecordIngestionCompletion(work_factory)
    recorder.execute(completed_event(Disease.DENGUE, week=202644))
    recorder.execute(completed_event(Disease.CHIKUNGUNYA, week=202644))
    coordinator = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202642)
    )
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=True,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202642),
    )

    first = coordinator.reconcile_all()
    assert len(first) == 1
    assert first[0].readiness.ready
    assert first[0].readiness.epiweek == Epiweek.parse(202642)
    first_job = service.claim_ready(Coverage.BR, Epiweek.parse(202642))
    assert first_job is not None
    assert service.execute(first_job, FakeRunner())

    second = coordinator.reconcile_all()
    assert len(second) == 1
    assert second[0].readiness.ready
    assert second[0].readiness.epiweek == Epiweek.parse(202643)
    second_job = service.claim_ready(Coverage.BR, Epiweek.parse(202643))
    assert second_job is not None
    assert service.execute(second_job, FakeRunner())

    third = coordinator.reconcile_all()
    assert len(third) == 1
    assert third[0].readiness.ready
    assert third[0].readiness.epiweek == Epiweek.parse(202644)


def test_failed_load_true_job_does_not_advance_cursor(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """A failed persistence attempt leaves its analysis week pending."""
    ready_workflow(work_factory, completed_event)
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=True,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = service.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert job_id is not None
    assert not service.execute(job_id, FakeRunner(False))
    with sessions() as session:
        assert (
            SqlWorkflowRepository(session).last_persisted_analysis_week(
                Coverage.BR, Epiweek.parse(202639)
            )
            is None
        )


def test_durable_input_selection_ignores_event_arrival_order(
    work_factory, completed_event
) -> None:
    """The latest completed run wins even when it was recorded first."""
    old = completed_event(
        Disease.DENGUE,
        week=202644,
        fingerprint='old',
        completed_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    latest = completed_event(
        Disease.DENGUE,
        week=202644,
        fingerprint='latest',
        completed_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
    )
    chik = completed_event(
        Disease.CHIKUNGUNYA,
        week=202644,
        completed_at=datetime(2026, 10, 1, tzinfo=timezone.utc),
    )
    recorder = RecordIngestionCompletion(work_factory)
    for event in (latest, chik, old):
        recorder.execute(event)
    snapshots = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202644)
    ).reconcile_all()
    assert len(snapshots) == 1
    assert (
        snapshots[0].readiness.inputs[Disease.DENGUE].run_id == latest.run_id
    )
    assert (
        snapshots[0].readiness.inputs[Disease.DENGUE].fingerprint == 'latest'
    )


def test_uninitialized_job_service_blocks_queued_work(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """Queued work stays blocked until an explicit bootstrap is configured."""
    ready_workflow(work_factory, completed_event)
    initialized = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = initialized.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert job_id is not None
    uninitialized = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=False,
        output_root=tmp_path,
    )
    runner = FakeRunner()
    assert not uninitialized.execute(job_id, runner)
    assert runner.requests == []


def test_cursor_does_not_skip_an_unfinished_earlier_week(sessions) -> None:
    """A later successful week cannot move the cursor over a gap."""
    with sessions.begin() as session:
        workflow = WorkflowRow(
            coverage=Coverage.BR,
            epiweek=202643,
            status=WorkflowStatus.SUCCEEDED,
            queued_fingerprint='later-week',
        )
        session.add(workflow)
        session.flush()
        session.add(
            AnalysisJobRow(
                job_id=uuid4(),
                workflow_id=workflow.workflow_id,
                input_fingerprint='later-week',
                status=AnalysisJobStatus.SUCCEEDED,
                image='analysis:test',
                cores=1,
                load=True,
                output_dir='/tmp/later-week',
            )
        )
    with sessions() as session:
        assert (
            SqlWorkflowRepository(session).last_persisted_analysis_week(
                Coverage.BR, Epiweek.parse(202642)
            )
            is None
        )


def test_dispatcher_advances_from_retained_delivery_without_duplicates(
    sessions, work_factory, completed_event, tmp_path: Path, monkeypatch
) -> None:
    """Periodic dispatch walks consecutive candidates from one delivery."""
    coordinator = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202642)
    )
    coordinator.record(completed_event(Disease.DENGUE, week=202644))
    coordinator.record(completed_event(Disease.CHIKUNGUNYA, week=202644))
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_load=True,
        analysis_initial_epiweek=202642,
        analysis_output_root=tmp_path,
    )
    monkeypatch.setattr(tasks, 'get_settings', lambda: settings)
    monkeypatch.setattr(tasks, 'configure_logging', lambda _level: None)
    monkeypatch.setattr(tasks, 'unit_of_work', work_factory)
    monkeypatch.setattr(tasks, 'session_factory', lambda: sessions)
    published: list[UUID] = []
    monkeypatch.setattr(
        tasks.celery_app,
        'send_task',
        lambda _name, args, **_kwargs: published.append(UUID(args[0])),
    )

    assert tasks.dispatch_ready() == 1
    assert tasks.dispatch_ready() == 0
    with sessions() as session:
        first = session.get(AnalysisJobRow, published[0])
        assert first is not None
        first_week = session.get(WorkflowRow, first.workflow_id)
        assert first_week is not None and first_week.epiweek == 202642
    assert tasks._analysis_jobs(settings).execute(published[0], FakeRunner())

    assert tasks.dispatch_ready() == 1
    with sessions() as session:
        jobs = session.scalars(select(AnalysisJobRow)).all()
        weeks = sorted(
            session.get(WorkflowRow, job.workflow_id).epiweek for job in jobs
        )
    assert weeks == [202642, 202643]
    assert len(published) == 2


def test_failed_task_publish_stays_queued_for_periodic_redispatch(
    sessions, work_factory, completed_event, tmp_path: Path, monkeypatch
) -> None:
    """A broker publish failure leaves the job queued for timed redispatch."""
    coordinator = CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202642)
    )
    coordinator.record(completed_event(Disease.DENGUE, week=202644))
    coordinator.record(completed_event(Disease.CHIKUNGUNYA, week=202644))
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_load=True,
        analysis_initial_epiweek=202642,
        analysis_dispatch_retry_seconds=60,
        analysis_output_root=tmp_path,
    )
    monkeypatch.setattr(tasks, 'get_settings', lambda: settings)
    monkeypatch.setattr(tasks, 'configure_logging', lambda _level: None)
    monkeypatch.setattr(tasks, 'unit_of_work', work_factory)
    monkeypatch.setattr(tasks, 'session_factory', lambda: sessions)

    def fail_publish(*_args, **_kwargs):
        raise RuntimeError('RabbitMQ unavailable')

    monkeypatch.setattr(tasks.celery_app, 'send_task', fail_publish)
    with pytest.raises(RuntimeError, match='RabbitMQ unavailable'):
        tasks.dispatch_ready()

    with sessions() as session:
        job = session.scalar(select(AnalysisJobRow))
        assert job is not None
        job_id = job.job_id
        assert job.status is AnalysisJobStatus.QUEUED
        assert job.dispatched_at is not None

    published: list[UUID] = []
    monkeypatch.setattr(
        tasks.celery_app,
        'send_task',
        lambda _name, args, **_kwargs: published.append(UUID(args[0])),
    )
    assert tasks.dispatch_ready() == 0
    assert published == []

    with sessions.begin() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        job.dispatched_at = datetime.now(timezone.utc) - timedelta(seconds=61)
    assert tasks.dispatch_ready() == 1
    assert published == [job_id]
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.QUEUED


def test_concurrent_dispatchers_claim_a_queued_message_once(
    tmp_path: Path, monkeypatch
) -> None:
    """A compare-and-set on dispatched_at prevents concurrent republishes."""
    engine = create_engine(
        f'sqlite+pysqlite:///{tmp_path / "dispatch.db"}',
        execution_options={'schema_translate_map': {'pipeline': None}},
        connect_args={'timeout': 10},
    )
    Base.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    workflow_id = uuid4()
    job_id = uuid4()
    with sessions.begin() as session:
        session.add(
            WorkflowRow(
                workflow_id=workflow_id,
                coverage=Coverage.BR,
                epiweek=202642,
                status=WorkflowStatus.QUEUED,
                queued_fingerprint='fingerprint',
            )
        )
        session.add(
            AnalysisJobRow(
                job_id=job_id,
                workflow_id=workflow_id,
                input_fingerprint='fingerprint',
                status=AnalysisJobStatus.QUEUED,
                image='analysis:test',
                cores=1,
                load=True,
                output_dir='/tmp/queued',
            )
        )
    monkeypatch.setattr(tasks, 'session_factory', lambda: sessions)
    barrier = Barrier(2)
    retry_before = datetime.now(timezone.utc)

    def claim() -> bool:
        barrier.wait(timeout=5)
        return tasks._claim_queued_dispatch(job_id, retry_before)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: claim(), range(2)))
    assert sum(results) == 1
    engine.dispose()


def test_analysis_job_claim_run_duplicate_and_explicit_retry(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """One logical job moves through states; FAILED needs explicit retry."""
    ready_workflow(work_factory, completed_event)
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=2,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    week = Epiweek.parse(202639)
    first_id = service.claim_ready(Coverage.BR, week)
    assert first_id is not None
    with sessions() as session:
        first_job = session.get(AnalysisJobRow, first_id)
        assert first_job is not None
        assert first_job.output_dir == str(tmp_path / str(first_id))
    assert service.claim_ready(Coverage.BR, week) is None
    failed = FakeRunner(False)
    assert not service.execute(first_id, failed)
    assert not service.execute(first_id, FakeRunner())
    with sessions() as session:
        job = session.get(AnalysisJobRow, first_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.FAILED
        assert job.attempts == 1
    assert service.retry(first_id)
    assert not service.retry(first_id)
    with sessions() as session:
        retried_job = session.get(AnalysisJobRow, first_id)
        assert retried_job is not None
        assert retried_job.output_dir == str(
            tmp_path / str(first_id) / 'attempt-2'
        )
    assert service.execute(first_id, FakeRunner())
    with sessions() as session:
        job = session.get(AnalysisJobRow, first_id)
        workflow = session.scalar(select(WorkflowRow))
        assert job is not None and job.status is AnalysisJobStatus.SUCCEEDED
        assert job.attempts == 2
        assert workflow is not None
        assert workflow.status is WorkflowStatus.SUCCEEDED
        assert (
            SqlWorkflowRepository(session).last_persisted_analysis_week(
                Coverage.BR, Epiweek.parse(202639)
            )
            is None
        )


def test_old_job_completion_does_not_replace_newer_ready_fingerprint(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """A completion updates only the workflow fingerprint it claimed."""
    ready_workflow(work_factory, completed_event)
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=True,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = service.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert job_id is not None
    old = completed_event(Disease.DENGUE)
    newer = old.model_copy(
        update={
            'run_id': uuid4(),
            'event_id': uuid4(),
            'fingerprint': 'newer-input',
            'completed_at': datetime.now(timezone.utc).replace(
                year=2027, month=1, day=1
            ),
        }
    )
    CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202639)
    ).record(newer)
    stale_runner = FakeRunner()
    assert not service.execute(job_id, stale_runner)
    assert stale_runner.requests == []
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.FAILED
        assert job.attempts == 0
        assert job.last_error == 'analysis job input fingerprint is stale'
        assert (
            SqlWorkflowRepository(session).last_persisted_analysis_week(
                Coverage.BR, Epiweek.parse(202639)
            )
            is None
        )
    with sessions() as session:
        workflow = session.scalar(select(WorkflowRow))
        assert workflow is not None
        assert workflow.status is WorkflowStatus.READY
        assert workflow.queued_fingerprint != 'newer-input'


def test_stale_failed_retry_preserves_job_state_and_artifacts(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """A stale failed job remains untouched and cannot reuse old inputs."""
    ready_workflow(work_factory, completed_event)
    original_service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=True,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = original_service.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert job_id is not None
    failed_runner = FakeRunner(False)
    assert not original_service.execute(job_id, failed_runner)
    failed_request = failed_runner.requests[0]
    failed_request.output_dir.mkdir(parents=True)
    marker = failed_request.output_dir / 'failed-attempt.txt'
    marker.write_text('preserve', encoding='utf-8')

    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        old_fingerprint = job.input_fingerprint
        old_workflow_id = job.workflow_id
        before = (
            job.status,
            job.cores,
            job.attempts,
            job.started_at,
            job.finished_at,
            job.exit_code,
            job.last_error,
            job.dispatched_at,
            job.output_dir,
            job.load,
        )

    old_event = completed_event(Disease.DENGUE)
    newer = old_event.model_copy(
        update={
            'run_id': uuid4(),
            'event_id': uuid4(),
            'fingerprint': 'newer-input',
            'completed_at': datetime.now(timezone.utc).replace(
                year=2027, month=1, day=1
            ),
        }
    )
    CoordinateEpiweek(
        work_factory, initial_analysis_week=Epiweek.parse(202639)
    ).record(newer)
    retry_service = AnalysisJobService(
        sessions,
        worker_image='analysis:new',
        cores=8,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    assert not retry_service.retry(job_id)

    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        workflow = session.get(WorkflowRow, old_workflow_id)
        assert job is not None
        assert workflow is not None
        assert (
            job.status,
            job.cores,
            job.attempts,
            job.started_at,
            job.finished_at,
            job.exit_code,
            job.last_error,
            job.dispatched_at,
            job.output_dir,
            job.load,
        ) == before
        assert job.input_fingerprint == old_fingerprint
        assert workflow.status is WorkflowStatus.READY
        assert marker.read_text(encoding='utf-8') == 'preserve'


def test_failed_retry_refreshes_cores_and_preserves_job_and_attempt(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """Refresh allocation without changing job identity or failed artifacts."""
    ready_workflow(work_factory, completed_event)
    original_service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=True,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    week = Epiweek.parse(202639)
    job_id = original_service.claim_ready(Coverage.BR, week)
    assert job_id is not None
    failed_runner = FakeRunner(False)
    assert not original_service.execute(job_id, failed_runner)
    failed_request = failed_runner.requests[0]
    failed_request.output_dir.mkdir(parents=True)
    marker = failed_request.output_dir / 'attempt-artifact.txt'
    marker.write_text('preserve this failed attempt', encoding='utf-8')

    with sessions() as session:
        failed_job = session.get(AnalysisJobRow, job_id)
        assert failed_job is not None
        original_workflow_id = failed_job.workflow_id
        original_fingerprint = failed_job.input_fingerprint
        original_output_dir = failed_job.output_dir
        original_load = failed_job.load
        original_attempts = failed_job.attempts

    retry_service = AnalysisJobService(
        sessions,
        worker_image='analysis:new',
        cores=8,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    assert retry_service.retry(job_id)
    assert not retry_service.retry(job_id)
    with sessions() as session:
        retried_job = session.get(AnalysisJobRow, job_id)
        workflow = session.get(WorkflowRow, original_workflow_id)
        assert retried_job is not None
        assert workflow is not None
        assert retried_job.job_id == job_id
        assert retried_job.workflow_id == original_workflow_id
        assert retried_job.input_fingerprint == original_fingerprint
        assert retried_job.load is original_load is True
        assert retried_job.cores == 8
        assert retried_job.attempts == original_attempts == 1
        assert retried_job.output_dir == str(
            tmp_path / str(job_id) / 'attempt-2'
        )
        assert original_output_dir == str(tmp_path / str(job_id))
        assert workflow.workflow_id == original_workflow_id
        assert workflow.queued_fingerprint == original_fingerprint
        assert marker.read_text(encoding='utf-8') == (
            'preserve this failed attempt'
        )

    retry_runner = FakeRunner()
    assert retry_service.execute(job_id, retry_runner)
    assert retry_runner.requests[0].cores == 8
    assert retry_runner.requests[0].load is True
    assert retry_runner.requests[0].job_id == job_id


def test_retry_is_noop_for_non_failed_job(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """Only FAILED jobs may receive refreshed cores or be requeued."""
    ready_workflow(work_factory, completed_event)
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=8,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = service.claim_ready(Coverage.BR, Epiweek.parse(202639))
    assert job_id is not None
    with sessions.begin() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.cores == 8
        job.cores = 1
    assert not service.retry(job_id)
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.QUEUED
        assert job.cores == 1
