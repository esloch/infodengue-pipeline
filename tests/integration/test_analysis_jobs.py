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
        image='analysis:test',
        cores=2,
        load=False,
        output_root=tmp_path,
    )
    week = Epiweek.parse(202639)
    first_id = service.claim_ready(Coverage.BR, week)
    assert first_id is not None
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
        image='analysis:test',
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
    assert service.execute(job_id, FakeRunner())
    with sessions() as session:
        workflow = session.scalar(select(WorkflowRow))
        assert workflow is not None
        assert workflow.status is WorkflowStatus.READY
        assert workflow.queued_fingerprint != 'newer-input'
