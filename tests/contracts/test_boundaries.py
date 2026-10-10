"""Executable dependency direction and infrastructure boundaries."""

import ast

from pathlib import Path

from infodengue_pipeline.adapters.rabbitmq.celery_app import celery_app
from infodengue_pipeline.ports.contracts import AnalysisRunner, ObjectStore

SOURCE = Path(__file__).parents[2] / 'src' / 'infodengue_pipeline'


def test_no_forbidden_framework_or_sibling_paths() -> None:
    """Runtime source cannot gain framework or sibling checkout coupling."""
    for path in SOURCE.rglob('*.py'):
        source = path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or '']
            else:
                continue
            assert not any(
                name.split('.')[0] in {'django', 'alertflow', 'airflow'}
                for name in names
            ), path
        assert '../AlertaDengue' not in source, path
        assert '/mnt/storagebox-infodengue' not in source, path
        assert 'shell=True' not in source, path


def test_domain_and_application_dependency_direction() -> None:
    """Domain and use cases cannot import concrete infrastructure."""
    for part in ('domain', 'application'):
        for path in (SOURCE / part).rglob('*.py'):
            source = path.read_text()
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    assert '.adapters' not in module
                    assert module not in {'celery', 'subprocess', 'docker'}
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert alias.name.split('.')[0] not in {
                            'celery',
                            'subprocess',
                            'docker',
                        }


def test_ports_are_protocols() -> None:
    """Scientific jobs and ingress are exposed through replaceable ports."""
    assert getattr(AnalysisRunner, '_is_protocol', False)
    assert getattr(ObjectStore, '_is_protocol', False)


def test_celery_routing_and_broker_configuration() -> None:
    """All work is explicitly routed over RabbitMQ without a result DB."""
    queues = {queue.name for queue in celery_app.conf.task_queues}
    assert queues == {
        'pipeline.ingestion',
        'pipeline.events',
        'pipeline.orchestration',
        'pipeline.analysis',
    }
    assert (
        celery_app.conf.task_routes['pipeline.events.ingestion_completed'][
            'queue'
        ]
        == 'pipeline.events'
    )
    assert celery_app.conf.broker_url.startswith('amqp://')
    assert celery_app.conf.broker_transport_options['confirm_publish']
    assert celery_app.conf.result_backend is None
