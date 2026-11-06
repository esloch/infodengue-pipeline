"""Tests for the bounded, read-only operational status report."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from click import ClickException
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    IngressObjectRow,
    OutboxEventRow,
    OutboxStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    Coverage,
    Disease,
    WorkflowStatus,
)
from infodengue_pipeline.production import status
from infodengue_pipeline.production.status import report
from sqlalchemy import func, select


def test_status_handles_empty_database(sessions) -> None:
    """An empty database prints all categories as empty with zero counts."""
    with sessions() as session:
        output = report(session)
    assert 'Latest SINAN ingress objects: 0 total' in output
    assert 'Pending outbox events: 0 total' in output
    assert 'Queued analysis jobs: 0 total' in output
    assert '(none)' in output


def test_status_reports_pipeline_states_and_limits_without_mutation(
    sessions,
) -> None:
    """Categories and identifiers are reported without updating persisted rows.

    The report leaves existing database records unchanged.
    """
    now = datetime.now(timezone.utc)
    workflow_ids = [uuid4() for _ in WorkflowStatus]
    sample_run_id = uuid4()
    with sessions.begin() as session:
        for index, status in enumerate(WorkflowStatus):
            session.add(
                WorkflowRow(
                    workflow_id=workflow_ids[index],
                    coverage=Coverage.BR,
                    epiweek=202639 + index,
                    status=status,
                    created_at=now,
                    updated_at=now - timedelta(seconds=index),
                )
            )
        session.add(
            WorkflowInputRow(
                workflow_input_id=uuid4(),
                workflow_id=workflow_ids[0],
                disease=Disease.DENGUE,
                run_id=sample_run_id,
                fingerprint='sample-input',
                completed_at=now,
            )
        )
        # Add another workflow to cover every enum status and exercise a limit.
        session.add(
            WorkflowRow(
                workflow_id=uuid4(),
                coverage=Coverage.MG,
                epiweek=202644,
                status=WorkflowStatus.WAITING,
                created_at=now,
                updated_at=now,
            )
        )
        for index, status in enumerate(AnalysisJobStatus):
            session.add(
                AnalysisJobRow(
                    job_id=uuid4(),
                    workflow_id=workflow_ids[index],
                    input_fingerprint=f'fingerprint-{index}',
                    status=status,
                    image='analysis:test',
                    cores=1,
                    load=False,
                    output_dir=f'/tmp/job-{index}',
                    attempts=index,
                    created_at=now,
                    finished_at=now,
                )
            )
        for index, status in enumerate(('failed', 'materialized', 'ingested')):
            session.add(
                IngressObjectRow(
                    object_id=uuid4(),
                    bucket='sinan',
                    object_key=f'{index}.csv',
                    object_identity=f'identity-{index}',
                    etag='etag',
                    size=5,
                    destination=f'/tmp/{index}.csv',
                    status=status,
                    sha256=f'sha-{index}',
                    run_id=uuid4(),
                    created_at=now,
                    updated_at=now,
                )
            )
        session.add(
            OutboxEventRow(
                event_id=uuid4(),
                event_type='ingestion.completed',
                version=1,
                payload={'secret': 'must not appear'},
                status=OutboxStatus.PENDING,
                created_at=now,
            )
        )
    with sessions() as session:
        before = session.scalar(
            select(func.count()).select_from(AnalysisJobRow)
        )
        output = report(session, limit=1)
        after = session.scalar(
            select(func.count()).select_from(AnalysisJobRow)
        )

    assert 'Failed ingress objects: 1 total' in output
    assert 'Materialized ingress awaiting completion: 1 total' in output
    assert 'Pending outbox events: 1 total' in output
    assert 'queued: 1' in output
    assert 'running: 1' in output
    assert 'failed: 1' in output
    assert 'succeeded: 1' in output
    assert 'attempts=3' in output
    assert f'run_ids={sample_run_id}' in output
    assert 'object_key=0.csv' in output
    assert 'secret' not in output
    assert 'must not appear' not in output
    assert before == after == 4
    assert output.count('workflow_id=') <= 5


def test_status_rejects_unbounded_or_invalid_limit(sessions) -> None:
    """The report limit is constrained to a predictable range."""
    with sessions() as session:
        try:
            report(session, limit=0)
        except ValueError as exc:
            assert 'between 1 and 100' in str(exc)
        else:
            raise AssertionError('invalid limit was accepted')


def test_status_suppresses_connection_error_details(monkeypatch) -> None:
    """Database failures exit nonzero without echoing connection secrets."""

    class BrokenSession:
        def __enter__(self):
            raise RuntimeError('password=private-secret')

        def __exit__(self, *_args) -> None:
            return None

    class EngineStub:
        def dispose(self) -> None:
            return None

    monkeypatch.setattr(
        status, 'create_engine', lambda *_args, **_kwargs: EngineStub()
    )
    monkeypatch.setattr(status, 'Session', lambda _engine: BrokenSession())
    with pytest.raises(ClickException) as error:
        status.main()
    assert 'status query failed' in str(error.value)
    assert 'private-secret' not in str(error.value)
