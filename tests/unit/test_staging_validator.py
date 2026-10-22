"""Exact object, run, workflow and analysis job staging correlation."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    IngressObjectRow,
    IngressStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.ingestion.completion import resolve_run_disease
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    Coverage,
    Disease,
    ObjectMetadata,
    WorkflowStatus,
)
from infodengue_pipeline.staging.e2e import (
    load_analysis_job,
    load_current_workflow,
    load_ingress,
    load_workflow_input,
    validate_configuration,
)
from infodengue_pipeline.storage.ingress import object_identity


def test_validator_uses_current_object_identity(sessions) -> None:
    """A prior ingress version under the key cannot satisfy this fixture."""
    old = ObjectMetadata('sinan', 'approved-fixtures/file.csv', 2, 'old')
    current = ObjectMetadata('sinan', old.key, 3, 'new')
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=uuid4(),
                bucket=old.bucket,
                object_key=old.key,
                object_identity=object_identity(old),
                etag=old.etag,
                size=old.size,
                destination='/tmp/old.csv',
                status=IngressStatus.MATERIALIZED,
                sha256='a' * 64,
            )
        )

    assert (
        load_ingress(
            sessions, current.bucket, current.key, object_identity(current)
        )
        is None
    )


def test_validator_correlates_exact_run_and_current_job_fingerprint(
    sessions,
) -> None:
    """An old job for a changed input cannot satisfy the selected run."""
    workflow_id = uuid4()
    selected_run_id = uuid4()
    old_run_id = uuid4()
    completed_at = datetime.now(timezone.utc)
    with sessions.begin() as session:
        old_workflow = WorkflowRow(
            workflow_id=uuid4(),
            coverage=Coverage.RJ,
            epiweek=202639,
            status=WorkflowStatus.QUEUED,
        )
        workflow = WorkflowRow(
            workflow_id=workflow_id,
            coverage=Coverage.MG,
            epiweek=202639,
            status=WorkflowStatus.QUEUED,
        )
        session.add_all(
            [
                old_workflow,
                workflow,
                WorkflowInputRow(
                    workflow_input_id=uuid4(),
                    workflow_id=old_workflow.workflow_id,
                    disease=Disease.DENGUE,
                    run_id=old_run_id,
                    fingerprint='old-object-sha',
                    completed_at=completed_at,
                ),
                WorkflowInputRow(
                    workflow_input_id=uuid4(),
                    workflow_id=workflow_id,
                    disease=Disease.DENGUE,
                    run_id=selected_run_id,
                    fingerprint='selected-object-sha',
                    completed_at=completed_at,
                ),
                AnalysisJobRow(
                    job_id=uuid4(),
                    workflow_id=workflow_id,
                    input_fingerprint='old-workflow-fingerprint',
                    status=AnalysisJobStatus.SUCCEEDED,
                    image='analysis:test',
                    cores=1,
                    load=False,
                    output_dir='/tmp/old-job',
                ),
            ]
        )

    workflow_input = load_workflow_input(sessions, selected_run_id)
    assert workflow_input is not None
    assert workflow_input.run_id == selected_run_id
    assert workflow_input.fingerprint == 'selected-object-sha'
    old_input = load_workflow_input(sessions, old_run_id)
    assert old_input is not None
    assert old_input.workflow_id == old_workflow.workflow_id

    settings = Settings(
        _env_file=None,
        environment='staging',
        minio_prefix='approved-fixtures/',
    )
    expected = load_current_workflow(sessions, workflow_input, settings)
    assert expected is not None
    assert expected.workflow_id == workflow_id
    assert expected.fingerprint != 'old-workflow-fingerprint'
    assert load_analysis_job(sessions, expected) is None

    with sessions.begin() as session:
        current_job = AnalysisJobRow(
            job_id=uuid4(),
            workflow_id=workflow_id,
            input_fingerprint=expected.fingerprint,
            status=AnalysisJobStatus.SUCCEEDED,
            image='analysis:test',
            cores=1,
            load=False,
            output_dir='/tmp/current-job',
        )
        session.add(current_job)
    found = load_analysis_job(sessions, expected)
    assert found is not None
    assert found.workflow_id == workflow_id
    assert found.input_fingerprint == expected.fingerprint


def test_validator_requires_fixture_prefix_and_load_false() -> None:
    """Only explicit fixture keys and nonmutating analysis can be checked."""
    settings = Settings(
        _env_file=None,
        environment='staging',
        minio_prefix='approved-fixtures/',
        alerta_source_database_url=('postgresql://readonly:secret@source/db'),
    )
    validate_configuration(
        settings, 'approved-fixtures/staging-fixture.csv', 30
    )
    root = Path(__file__).resolve().parents[2]
    staging_env = (root / '.env.staging.example').read_text()
    operations = (root / 'docs/operations.md').read_text()
    assert 'INFODENGUE_PIPELINE_MINIO_PREFIX=approved-fixtures/' in staging_env
    assert 'approved-fixtures/staging-fixture.csv' in operations
    with pytest.raises(ValueError, match='fixture prefix'):
        validate_configuration(settings, 'other/staging-fixture.csv', 30)
    with pytest.raises(ValueError, match='analysis_load=false'):
        validate_configuration(
            settings.model_copy(update={'analysis_load': True}),
            'approved-fixtures/staging-fixture.csv',
            30,
        )
    with pytest.raises(ValueError, match='source database'):
        validate_configuration(
            settings.model_copy(
                update={
                    'alerta_source_database_url': None,
                    'analysis_database_url': 'postgresql://analysis@db/alerta',
                }
            ),
            'approved-fixtures/staging-fixture.csv',
            30,
        )


def test_staging_run_disease_resolution_accepts_canonical_a92() -> None:
    """The validator's source disease contract distinguishes A92 variants."""
    assert resolve_run_disease('A92', 'A92.0') is Disease.CHIKUNGUNYA
    assert resolve_run_disease('A92', 'A928') is Disease.ZIKA
    with pytest.raises(ValueError, match='ambiguous A92'):
        resolve_run_disease('A92', None)


def test_staging_validator_defaults_allow_real_analysis_duration() -> None:
    """Both validator entry points allow a bounded two-hour E2E run."""
    root = Path(__file__).resolve().parents[2]
    script = (root / 'scripts/validate_staging_e2e.py').read_text()
    makim = (root / '.makim.yaml').read_text()
    assert (
        "parser.add_argument('--timeout-seconds', type=float, default=7200)"
        in script
    )
    assert '            default: 7200' in makim


@pytest.mark.parametrize(
    'job_status',
    [AnalysisJobStatus.SUCCEEDED, AnalysisJobStatus.FAILED],
)
def test_reusable_validator_correlates_current_job(
    monkeypatch, capsys, job_status
):
    """The shared validator follows one object through the current job."""
    from types import SimpleNamespace

    from infodengue_pipeline.ingestion.completion import CompletedRun
    from infodengue_pipeline.staging import e2e

    now = datetime.now(timezone.utc)
    run_id = uuid4()
    workflow_id = uuid4()
    job_id = uuid4()
    sha = 'a' * 64
    metadata = ObjectMetadata('fixtures-test', 'fixtures/test.dbf', 3, 'etag')
    ingress = SimpleNamespace(
        status=IngressStatus.MATERIALIZED,
        sha256=sha,
        materialized_at=now,
        run_id=run_id,
    )
    run = CompletedRun(run_id, 'completed', 'MG', 'A90', 202412, sha, now)
    workflow_input = SimpleNamespace(
        run_id=run_id,
        disease=Disease.DENGUE,
        fingerprint=sha,
    )
    workflow = e2e.CurrentWorkflow(
        workflow_id,
        Coverage.MG,
        e2e.Epiweek.parse(202412),
        'current-fingerprint',
        WorkflowStatus.QUEUED,
    )
    job = SimpleNamespace(
        job_id=job_id,
        workflow_id=workflow_id,
        input_fingerprint='current-fingerprint',
        status=job_status,
        last_error=(
            'scientific failure'
            if job_status is AnalysisJobStatus.FAILED
            else None
        ),
        attempts=1,
        cores=1,
        output_dir='/tmp/artifacts',
    )
    monkeypatch.setattr(e2e, 'session_factory', lambda: object())
    monkeypatch.setattr(
        e2e,
        'build_object_store',
        lambda _settings: SimpleNamespace(stat=lambda _key: metadata),
    )
    monkeypatch.setattr(
        e2e,
        'load_ingress',
        lambda *_args: ingress,
    )
    monkeypatch.setattr(
        e2e,
        'build_source',
        lambda _url: SimpleNamespace(by_sha256=lambda _sha: run),
    )
    monkeypatch.setattr(
        e2e,
        'load_workflow_input',
        lambda *_args: workflow_input,
    )
    monkeypatch.setattr(
        e2e,
        'load_current_workflow',
        lambda *_args: workflow,
    )
    monkeypatch.setattr(e2e, 'load_analysis_job', lambda *_args: job)
    monkeypatch.setattr(e2e, 'load_terminal_analysis_job', lambda *_args: job)
    outputs = []
    monkeypatch.setattr(
        e2e,
        'validate_outputs',
        lambda path, request: outputs.append((path, request)),
    )
    configured = Settings(
        _env_file=None,
        environment='staging',
        minio_prefix='fixtures/',
        alerta_source_database_url='postgresql://readonly:secret@source/db',
    )
    if job_status is AnalysisJobStatus.FAILED:
        with pytest.raises(RuntimeError, match='analysis job failed'):
            e2e.validate_e2e(metadata.key, 10, configured)
        assert not outputs
        return
    result = e2e.validate_e2e(metadata.key, 10, configured)
    assert result.run_id == run_id
    assert result.job_id == job_id
    assert result.attempts == 1
    assert outputs[0][0] == Path('/tmp/artifacts')
    assert outputs[0][1].load is False
    printed = capsys.readouterr().out
    for stage in (
        'ingress materialization',
        'completed AlertaDengue ingestion',
        'workflow input',
        'workflow readiness',
        'analysis job',
        'analysis completion',
        'analysis artifacts',
    ):
        assert stage in printed


@pytest.mark.parametrize(
    ('status', 'message'),
    [
        (IngressStatus.FAILED, 'ingress materialization failed'),
        (IngressStatus.MATERIALIZED, 'predates current ingress'),
    ],
)
def test_reusable_validator_rejects_failed_ingress_and_old_run(
    monkeypatch, status, message
):
    """A failed ingress or historical completion cannot validate."""
    from types import SimpleNamespace

    from infodengue_pipeline.ingestion.completion import CompletedRun
    from infodengue_pipeline.staging import e2e

    now = datetime.now(timezone.utc)
    metadata = ObjectMetadata('fixtures-test', 'fixtures/test.dbf', 3, 'etag')
    ingress = SimpleNamespace(
        status=status,
        last_error='download failed',
        sha256='a' * 64,
        materialized_at=now,
    )
    run = CompletedRun(
        uuid4(),
        'completed',
        'MG',
        'A90',
        202412,
        'a' * 64,
        now.replace(year=2025),
    )
    monkeypatch.setattr(e2e, 'session_factory', lambda: object())
    monkeypatch.setattr(
        e2e,
        'build_object_store',
        lambda _settings: SimpleNamespace(stat=lambda _key: metadata),
    )
    monkeypatch.setattr(e2e, 'load_ingress', lambda *_args: ingress)
    monkeypatch.setattr(
        e2e,
        'build_source',
        lambda _url: SimpleNamespace(by_sha256=lambda _sha: run),
    )
    configured = Settings(
        _env_file=None,
        environment='staging',
        minio_prefix='fixtures/',
        alerta_source_database_url='postgresql://readonly:secret@source/db',
    )
    with pytest.raises(RuntimeError, match=message):
        e2e.validate_e2e(metadata.key, 10, configured)
