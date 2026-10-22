"""Static assertions for deployment topology and privilege boundaries."""

import re

from pathlib import Path

import pytest


def _service_section(path: str, name: str) -> str:
    """Return a service block from the small checked-in Compose contract."""
    text = (Path('containers') / path).read_text()
    return re.split(
        r'\n  [a-z][a-z-]*:\n',
        text.split(f'  {name}:\n', 1)[1],
        maxsplit=1,
    )[0]


@pytest.mark.parametrize('compose', ['compose.yaml', 'compose-staging.yaml'])
def test_worker_topology_and_concurrency(compose: str) -> None:
    """Workers consume their own queues and ingress is serialized."""
    ingestion = _service_section(compose, 'worker-ingestion')
    analysis = _service_section(compose, 'worker-analysis')
    assert '--queues=pipeline.ingestion' in ingestion
    assert '--concurrency=1' in ingestion
    assert '--queues=pipeline.analysis' in analysis
    assert '--concurrency=1' in analysis
    assert 'worker-analysis@%h' in analysis
    assert 'inspect ping' in analysis
    whole = (Path('containers') / compose).read_text()
    assert 'docker.sock' not in whole
    assert '/var/run/docker.sock' not in whole


def test_development_worker_network_and_output_mount() -> None:
    """Development aliases and stable output path are explicit."""
    text = Path('containers/compose.yaml').read_text()
    analysis = _service_section('compose.yaml', 'worker-analysis')
    events = _service_section('compose.yaml', 'worker-events')
    assert 'dev_integrations' in analysis and 'dev_integrations' in events
    assert (
        'pipeline_analysis_output:/var/lib/pipeline/analysis-output'
        in analysis
    )
    assert 'pipeline-postgres' in text
    assert 'pipeline-rabbitmq' in text


def test_staging_worker_env_and_output_mounts_are_isolated() -> None:
    """Only analysis gets its private env file and artifact directory."""
    analysis = _service_section('compose-staging.yaml', 'worker-analysis')
    core = Path('.env.staging.example').read_text()
    analysis_env = Path('.env.staging.analysis.example').read_text()
    validator = _service_section('compose-staging.yaml', 'validator')
    orchestration = _service_section(
        'compose-staging.yaml', 'worker-orchestration'
    )
    mount = (
        '${STAGING_ANALYSIS_OUTPUT_HOST_PATH:?set shared '
        'analysis output directory}'
        ':/var/lib/pipeline/analysis-output'
    )
    assert 'PIPELINE_ANALYSIS_ENV_FILE' in analysis
    assert 'PIPELINE_WORKER_ENV_FILE' not in analysis
    assert 'ANALYSIS_DATABASE_URL' not in core
    assert 'ALERTA_SOURCE_DATABASE_URL' in core
    assert 'ANALYSIS_DATABASE_URL' in analysis_env
    assert 'ALERTA_SOURCE_DATABASE_URL' not in analysis_env
    assert 'MINIO_ACCESS_KEY' not in analysis_env
    assert mount in analysis
    assert mount in validator
    fixed_root = (
        'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT: '
        '/var/lib/pipeline/analysis-output'
    )
    assert fixed_root in orchestration
    assert fixed_root in analysis
    assert fixed_root in validator
    assert mount not in orchestration
    assert '    volumes:' not in orchestration
    assert (
        'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT='
        '/var/lib/pipeline/analysis-output'
    ) in core
    assert (
        'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT='
        '/var/lib/pipeline/analysis-output'
    ) in analysis_env
    assert 'pipeline-postgres' in core
    assert 'pipeline-rabbitmq' in core
    assert 'minio:9000' in core
    assert 'MINIO_SECURE=false' in core
    assert 'MINIO_BUCKET=sinan-infodengue' in core
    assert 'MINIO_PREFIX=approved-fixtures/' in core
    assert (
        'STAGING_ANALYSIS_OUTPUT_HOST_PATH=/opt/data/staging/analysis-output'
        in core
    )
    assert 'STAGING_INTEGRATION_NETWORK=infodengue-staging_infodengue' in core
    assert (
        'external: true' in Path('containers/compose-staging.yaml').read_text()
    )
    assert (
        'privileged:'
        not in Path('containers/compose-staging.yaml').read_text()
    )
    assert (
        'network_mode:'
        not in Path('containers/compose-staging.yaml').read_text()
    )
    assert (
        'pid: host' not in Path('containers/compose-staging.yaml').read_text()
    )


def test_staging_env_file_template_and_ci_paths_are_distinct() -> None:
    """Copied operator config uses private files while CI renders templates."""
    template = Path('.env.staging.example').read_text()
    compose = Path('containers/compose-staging.yaml').read_text()
    makim = Path('.makim.yaml').read_text()

    assert 'PIPELINE_WORKER_ENV_FILE=../.env.staging\n' in template
    assert 'PIPELINE_ANALYSIS_ENV_FILE=../.env.staging.analysis\n' in template
    assert '${PIPELINE_WORKER_ENV_FILE:-../.env.staging}' in compose
    assert '${PIPELINE_ANALYSIS_ENV_FILE:-../.env.staging.analysis}' in compose
    assert 'env PIPELINE_WORKER_ENV_FILE=../.env.staging.example' in makim
    assert (
        'PIPELINE_ANALYSIS_ENV_FILE=../.env.staging.analysis.example' in makim
    )


def test_obsolete_runtime_settings_are_rejected() -> None:
    """Docker launch settings are outside the validated runtime contract."""
    from infodengue_pipeline.config import Settings
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None, analysis_runtime='docker')


def test_analysis_tasks_route_to_dedicated_queue() -> None:
    """Normal and explicit retry deliveries use only the analysis queue."""
    from infodengue_pipeline.messaging.celery import celery_app

    for name in ('pipeline.analysis.run', 'pipeline.analysis.retry'):
        route = celery_app.conf.task_routes[name]
        assert route == {
            'queue': 'pipeline.analysis',
            'routing_key': 'pipeline.analysis',
        }
    assert celery_app.conf.task_acks_late is True
    assert celery_app.conf.worker_prefetch_multiplier == 1


@pytest.mark.parametrize('example', ['.env.example', '.env.staging.example'])
def test_pipeline_urls_use_unique_network_aliases(example: str) -> None:
    """Multi-network workers reach the pipeline broker and database."""
    values = Path(example).read_text()
    assert '@pipeline-postgres:5432/' in values
    assert '@pipeline-rabbitmq:5672//' in values
    if example == '.env.staging.example':
        assert '@postgres:5432/alerta' in values
        assert '@alerta-postgres:' not in values
