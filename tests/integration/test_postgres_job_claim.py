"""PostgreSQL row-lock test for atomic READY-to-QUEUED analysis claims.

Set ``INFODENGUE_PIPELINE_TEST_POSTGRES_URL`` to a disposable database that
has had ``alembic upgrade head`` applied to run this integration test.
"""

import os

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from uuid import uuid4

import pytest

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
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.orm import sessionmaker


def test_postgres_claim_creates_one_job_under_concurrent_calls(
    tmp_path,
) -> None:
    """PostgreSQL locking and unique fingerprint allow one claimant."""
    database_url = os.environ.get('INFODENGUE_PIPELINE_TEST_POSTGRES_URL')
    if not database_url:
        pytest.skip('dedicated disposable PostgreSQL URL is not configured')
    engine = create_engine(database_url, pool_pre_ping=True)
    sessions = sessionmaker(engine, expire_on_commit=False)
    coverage = Coverage.MG
    epiweek = Epiweek.parse(209901)
    workflow_id = uuid4()
    run_id = uuid4()
    with sessions.begin() as session:
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
        sessions,
        image='test-image',
        cores=1,
        load=False,
        output_root=tmp_path,
    )
    barrier = Barrier(2)

    def claim():
        barrier.wait(timeout=5)
        return service.claim_ready(coverage, epiweek)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: claim(), range(2)))
        assert sum(result is not None for result in results) == 1
        with sessions() as session:
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
    finally:
        with sessions.begin() as session:
            session.execute(
                delete(AnalysisJobRow).where(
                    AnalysisJobRow.workflow_id == workflow_id
                )
            )
            session.execute(
                delete(WorkflowInputRow).where(
                    WorkflowInputRow.workflow_id == workflow_id
                )
            )
            session.execute(
                delete(WorkflowRow).where(
                    WorkflowRow.workflow_id == workflow_id
                )
            )
        engine.dispose()
