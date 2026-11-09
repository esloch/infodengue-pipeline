"""PostgreSQL row-lock test for atomic READY-to-QUEUED analysis claims."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from uuid import uuid4

from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.models import (
    Coverage,
    Disease,
    Epiweek,
    WorkflowStatus,
)
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker


def test_postgres_claim_creates_one_job_under_concurrent_calls(
    postgres_sessions: sessionmaker,
    tmp_path,
) -> None:
    """PostgreSQL locking and unique fingerprint allow one claimant."""
    coverage = Coverage.MG
    epiweek = Epiweek.parse(209901)
    workflow_id = uuid4()
    run_id = uuid4()
    with postgres_sessions.begin() as session:
        session.add(
            WorkflowRow(
                workflow_id=workflow_id,
                coverage=coverage,
                epiweek=int(epiweek),
                status=WorkflowStatus.READY,
            )
        )
        session.add(
            WorkflowInputRow(
                workflow_input_id=uuid4(),
                workflow_id=workflow_id,
                disease=Disease.DENGUE,
                run_id=run_id,
                fingerprint='postgres-lock-test',
                completed_at=datetime.now(timezone.utc),
            )
        )
    service = AnalysisJobService(
        postgres_sessions,
        worker_image='test-image',
        cores=1,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=epiweek,
    )
    barrier = Barrier(2)

    def claim():
        barrier.wait(timeout=5)
        return service.claim_ready(coverage, epiweek)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: claim(), range(2)))
    assert sum(result is not None for result in results) == 1
    with postgres_sessions() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(AnalysisJobRow)
                .where(AnalysisJobRow.workflow_id == workflow_id)
            )
            == 1
        )
        workflow = session.get(WorkflowRow, workflow_id)
        assert workflow is not None
        assert workflow.status is WorkflowStatus.QUEUED
