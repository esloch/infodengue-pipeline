"""Static assertions for deployment topology and privilege boundaries."""

import re

from pathlib import Path

import pytest


def _service_section(path: str, name: str) -> str:
    """Return a service block from the small checked-in Compose contract."""
    text = (Path('containers') / path).read_text()
    return re.split(
        r'\n  [a-z][a-z0-9-]*:\n',
        text.split(f'  {name}:\n', 1)[1],
        maxsplit=1,
    )[0]


@pytest.mark.parametrize(
    'compose', ['compose.yaml', 'compose-staging.yaml', 'compose-prod.yaml']
)
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


def test_staging_e2e_writer_is_confined_to_finite_runner() -> None:
    """Only the finite E2E service receives the dedicated writer env file."""
    runner = _service_section('compose-staging.yaml', 'e2e-runner')
    validator = _service_section('compose-staging.yaml', 'validator')
    assert 'PIPELINE_E2E_ENV_FILE' in runner
    assert 'PIPELINE_WORKER_ENV_FILE' in runner
    assert 'staging_integrations' in runner
    assert 'STAGING_E2E_FIXTURE_HOST_PATH' in runner
    assert 'STAGING_ANALYSIS_OUTPUT_HOST_PATH' in runner
    assert 'restart: "no"' in runner
    assert 'PIPELINE_E2E_ENV_FILE' not in validator
    for name in (
        'worker-ingestion',
        'worker-events',
        'worker-orchestration',
        'worker-analysis',
        'worker-beat',
    ):
        assert 'PIPELINE_E2E_ENV_FILE' not in _service_section(
            'compose-staging.yaml', name
        )
    template = Path('.env.staging.e2e.example').read_text()
    assert 'STAGING_E2E_MINIO_ACCESS_KEY=' in template
    assert 'STAGING_E2E_MINIO_SECRET_KEY=' in template
    assert (
        'PIPELINE_E2E_ENV_FILE=../.env.staging.e2e.example'
        in Path('.makim.yaml').read_text()
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


@pytest.mark.parametrize(
    'example', ['.env.example', '.env.staging.example', '.env.prod.example']
)
def test_pipeline_urls_use_unique_network_aliases(example: str) -> None:
    """Multi-network workers reach the pipeline broker and database."""
    values = Path(example).read_text()
    assert '@pipeline-postgres:5432/' in values
    assert '@pipeline-rabbitmq:5672//' in values
    if example in ('.env.staging.example', '.env.prod.example'):
        assert '@postgres:5432/alerta' in values
        assert '@alerta-postgres:' not in values


def test_production_service_set_and_privilege_boundary() -> None:
    """Production has a profile-gated finite E2E runner."""
    text = Path('containers/compose-prod.yaml').read_text()
    services = text.split('services:\n', 1)[1].split('\nvolumes:\n', 1)[0]
    names = set(re.findall(r'^  ([a-z][a-z0-9-]*):$', services, re.M))
    assert names == {
        'postgres',
        'rabbitmq',
        'migrate',
        'worker-ingestion',
        'worker-events',
        'worker-orchestration',
        'worker-analysis',
        'worker-beat',
        'analysis-e2e-runner',
        'analysis-e2e-db',
    }
    assert names.isdisjoint({'validator', 'e2e-runner', 'minio', 'minio-init'})
    for forbidden in (
        'docker.sock',
        '/var/run/docker.sock',
        'privileged:',
        'network_mode:',
        'pid: host',
        '/opt/services/AlertaDengue',
        '/opt/services/AlertaDengueAnalise',
        '../AlertaDengue',
        '../AlertaDengueAnalise',
    ):
        assert forbidden not in text
    assert 'name: infodengue-pipeline-prod' in text
    assert '    build:' not in text
    for name in names - {'postgres', 'rabbitmq', 'analysis-e2e-db'}:
        assert 'INFODENGUE_PIPELINE_ENVIRONMENT: prod' in _service_section(
            'compose-prod.yaml', name
        )
    clone = _service_section('compose-prod.yaml', 'analysis-e2e-db')
    assert 'profiles: ["analysis-e2e-db"]' in clone
    assert 'pipeline_prod_analysis_e2e_pgdata' in clone
    assert '    ports:' not in clone


def test_production_workers_are_gated_by_successful_migration() -> None:
    """Compose owns one migration lifecycle before workers start."""
    migrate = _service_section('compose-prod.yaml', 'migrate')
    assert 'command: ["alembic", "upgrade", "head"]' in migrate
    assert 'restart: "no"' in migrate
    assert 'postgres:\n        condition: service_healthy' in migrate
    for name in (
        'worker-ingestion',
        'worker-events',
        'worker-orchestration',
        'worker-analysis',
    ):
        worker = _service_section('compose-prod.yaml', name)
        assert (
            'migrate:\n        condition: service_completed_successfully'
            in worker
        )
        assert 'postgres:\n        condition: service_healthy' in worker
        assert 'rabbitmq:\n        condition: service_healthy' in worker


def test_production_documented_startup_uses_compose_dependencies() -> None:
    """Normal startup does not launch a disposable second migration."""
    docs = Path('docs/production.md').read_text()
    startup = docs.split('## Starting the production profile\n', 1)[1].split(
        '\n## Migrations\n', 1
    )[0]
    assert startup.count('  up -d\n') == 1
    assert '  ps --all\n' in startup
    assert 'run --rm migrate' not in startup


def test_production_worker_env_mounts_and_networks() -> None:
    """Analysis has a separate env and output bind."""
    compose = 'compose-prod.yaml'
    analysis = _service_section(compose, 'worker-analysis')
    ingestion = _service_section(compose, 'worker-ingestion')
    orchestration = _service_section(compose, 'worker-orchestration')
    beat = _service_section(compose, 'worker-beat')
    assert '${PIPELINE_ANALYSIS_ENV_FILE:-../.env.prod.analysis}' in analysis
    assert 'PIPELINE_WORKER_ENV_FILE' not in analysis
    assert '${PIPELINE_WORKER_ENV_FILE:-../.env.prod}' in ingestion
    assert '${PIPELINE_WORKER_ENV_FILE:-../.env.prod}' in beat
    assert '${PIPELINE_ANALYSIS_IMAGE:' in analysis
    assert '    build:' not in analysis
    assert '${PROD_SINAN_INCOMING_HOST_PATH:' in ingestion
    assert ':/var/lib/pipeline/incoming' in ingestion
    assert '${PROD_ANALYSIS_OUTPUT_HOST_PATH:' in analysis
    assert ':/var/lib/pipeline/analysis-output' in analysis
    assert '    volumes:' not in orchestration
    assert '    networks:' not in beat.split('\nvolumes:', 1)[0]
    for name in (
        'worker-ingestion',
        'worker-events',
        'worker-orchestration',
        'worker-analysis',
    ):
        section = _service_section(compose, name)
        assert '      - default\n' in section
        assert '      - prod_integrations\n' in section
        assert 'inspect ping' in section
    output_root = (
        'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT: '
        '/var/lib/pipeline/analysis-output'
    )
    assert output_root in analysis
    assert output_root in orchestration


def test_production_env_roles_and_immutable_image() -> None:
    """Tracked examples keep source, analysis, and MinIO roles separate."""
    core = Path('.env.prod.example').read_text()
    analysis = Path('.env.prod.analysis.example').read_text()
    compose = Path('containers/compose-prod.yaml').read_text()
    assert 'PIPELINE_WORKER_ENV_FILE=../.env.prod\n' in core
    assert 'PIPELINE_ANALYSIS_ENV_FILE=../.env.prod.analysis\n' in core
    assert '${PIPELINE_WORKER_ENV_FILE:-../.env.prod}' in compose
    assert '${PIPELINE_ANALYSIS_ENV_FILE:-../.env.prod.analysis}' in compose
    assert 'INFODENGUE_PIPELINE_ENVIRONMENT=prod\n' in core
    assert 'INFODENGUE_PIPELINE_ENVIRONMENT=prod\n' in analysis
    assert 'INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL=' in core
    assert 'INFODENGUE_PIPELINE_ANALYSIS_DATABASE_URL=' not in core
    assert 'INFODENGUE_PIPELINE_ANALYSIS_DATABASE_URL=' in analysis
    assert 'INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL=' not in analysis
    assert 'pipeline_readonly:' in core
    assert 'pipeline_analysis_readonly:' in analysis
    assert 'INFODENGUE_PIPELINE_MINIO_ACCESS_KEY=' in core
    assert 'INFODENGUE_PIPELINE_MINIO_SECRET_KEY=' in core
    assert 'INFODENGUE_PIPELINE_MINIO_ACCESS_KEY=' not in analysis
    assert 'INFODENGUE_PIPELINE_MINIO_SECRET_KEY=' not in analysis
    assert 'STAGING_E2E_MINIO_ACCESS_KEY' not in core
    assert 'STAGING_E2E_MINIO_SECRET_KEY' not in core
    assert 'PIPELINE_E2E_ENV_FILE' not in core + analysis
    assert 'PIPELINE_E2E_ENV_FILE' in compose
    assert 'INFODENGUE_PIPELINE_ANALYSIS_LOAD=false\n' in core
    assert 'INFODENGUE_PIPELINE_ANALYSIS_LOAD=false\n' in analysis
    values = dict(
        line.split('=', 1)
        for line in core.splitlines()
        if line and not line.startswith('#')
    )
    assert '@sha256:' in values['PIPELINE_IMAGE']
    assert '@sha256:' in values['PIPELINE_ANALYSIS_IMAGE']
    assert (
        values['PIPELINE_ANALYSIS_IMAGE']
        == values['INFODENGUE_PIPELINE_ANALYSIS_IMAGE']
    )
    assert (
        f'INFODENGUE_PIPELINE_ANALYSIS_IMAGE={values["PIPELINE_ANALYSIS_IMAGE"]}'
        in analysis
    )


def test_production_state_ports_and_integration_network() -> None:
    """The profile isolates durable state and binds host ports to loopback."""
    text = Path('containers/compose-prod.yaml').read_text()
    core = Path('.env.prod.example').read_text()
    assert 'pipeline_prod_pgdata:/var/lib/postgresql/data' in text
    assert 'pipeline_prod_rabbitmq:/var/lib/rabbitmq' in text
    assert '  pipeline_prod_pgdata:\n' in text
    assert '  pipeline_prod_rabbitmq:\n' in text
    assert '127.0.0.1:${PIPELINE_POSTGRES_HOST_PORT:-5436}:5432' in text
    assert '127.0.0.1:${PIPELINE_RABBITMQ_HOST_PORT:-5675}:5672' in text
    assert 'PIPELINE_POSTGRES_HOST_PORT=5436\n' in core
    assert 'PIPELINE_RABBITMQ_HOST_PORT=5675\n' in core
    assert '    external: true' in text
    assert (
        '${PROD_INTEGRATION_NETWORK:?set existing integration network}' in text
    )
    assert 'infodengue-staging_infodengue' not in text
    assert 'pipeline-postgres' in text
    assert 'pipeline-rabbitmq' in text
