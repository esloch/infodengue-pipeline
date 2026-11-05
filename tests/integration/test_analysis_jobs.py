"""Persisted analysis job transitions and stale completion protection."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    WorkflowRow,
)
from infodengue_pipeline.models import (
    AnalysisJobResult,
    AnalysisJobStatus,
    Coverage,
    Disease,
    Epiweek,
    WorkflowStatus,
)
from infodengue_pipeline.services import CoordinateEpiweek
from sqlalchemy import select


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
    coordinator = CoordinateEpiweek(work_factory)
    coordinator.record(completed_event(Disease.DENGUE))
    coordinator.record(completed_event(Disease.CHIKUNGUNYA))


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


def test_old_job_completion_does_not_replace_newer_ready_fingerprint(
    sessions, work_factory, completed_event, tmp_path: Path
) -> None:
    """A completion updates only the workflow fingerprint it claimed."""
    ready_workflow(work_factory, completed_event)
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=False,
        output_root=tmp_path,
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
    CoordinateEpiweek(work_factory).record(newer)
    stale_runner = FakeRunner()
    assert not service.execute(job_id, stale_runner)
    assert stale_runner.requests == []
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.FAILED
        assert job.attempts == 0
        assert job.last_error == 'analysis job input fingerprint is stale'
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
    CoordinateEpiweek(work_factory).record(newer)
    retry_service = AnalysisJobService(
        sessions,
        worker_image='analysis:new',
        cores=8,
        load=False,
        output_root=tmp_path,
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
