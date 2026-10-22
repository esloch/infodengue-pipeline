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
from infodengue_pipeline.storage.ingress import object_identity

from scripts.validate_staging_e2e import (
    load_analysis_job,
    load_current_workflow,
    load_ingress,
    load_workflow_input,
    validate_configuration,
)


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
