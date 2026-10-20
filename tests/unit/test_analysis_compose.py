"""Static topology checks for the dedicated scientific consumer."""

import re

from pathlib import Path

import pytest


@pytest.mark.parametrize('compose', ['compose.yaml', 'compose-staging.yaml'])
def test_analysis_worker_topology(compose: str) -> None:
    """Both deployments isolate analysis in a healthy single worker."""
    text = (Path('containers') / compose).read_text()
    section = re.split(
        r'\n  [a-z][a-z-]*:\n',
        text.split('  worker-analysis:\n', 1)[1],
        maxsplit=1,
    )[0]
    assert '--queues=pipeline.analysis' in section
    assert '--concurrency=1' in section
    assert 'worker-analysis@%h' in section
    assert 'inspect ping' in section
    assert 'docker.sock' not in text
    assert '/var/run/docker.sock' not in text
    assert 'pipeline-postgres' in text
    assert 'pipeline-rabbitmq' in text
    if compose == 'compose-staging.yaml':
        assert 'staging_integrations' in section
        assert 'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT' in section
        validator = text.split('  validator:\n', 1)[1]
        assert 'INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT' in validator
    else:
        assert 'dev_integrations' in section
        assert 'pipeline_analysis_output:' in section
        assert 'analysis-output' in section


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
