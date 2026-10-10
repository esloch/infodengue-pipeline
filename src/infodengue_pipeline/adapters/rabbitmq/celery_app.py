"""Standalone Celery app and explicit RabbitMQ queues."""

from celery import Celery
from infodengue_pipeline.config import get_settings
from kombu import Exchange, Queue


def create_celery_app() -> Celery:
    """Build a Django-free Celery application."""
    settings = get_settings()
    app = Celery(
        'infodengue_pipeline',
        broker=settings.rabbitmq_url.get_secret_value(),
        include=['infodengue_pipeline.workers.tasks'],
    )
    exchange = Exchange('pipeline', type='direct', durable=True)
    queues = (
        'pipeline.ingestion',
        'pipeline.events',
        'pipeline.orchestration',
        'pipeline.analysis',
    )
    app.conf.update(
        task_queues=tuple(
            Queue(name, exchange, routing_key=name, durable=True)
            for name in queues
        ),
        task_default_queue='pipeline.orchestration',
        task_default_exchange='pipeline',
        task_default_exchange_type='direct',
        task_default_routing_key='pipeline.orchestration',
        task_routes={
            'pipeline.events.ingestion_completed': {
                'queue': 'pipeline.events',
                'routing_key': 'pipeline.events',
            },
            'pipeline.events.publish_pending': {
                'queue': 'pipeline.events',
                'routing_key': 'pipeline.events',
            },
            'pipeline.health.ping': {
                'queue': 'pipeline.orchestration',
                'routing_key': 'pipeline.orchestration',
            },
        },
        task_serializer='json',
        accept_content=['json'],
        result_backend=None,
        task_ignore_result=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        broker_connection_retry_on_startup=True,
        broker_transport_options={'confirm_publish': True},
        task_create_missing_queues=False,
    )
    return app


celery_app = create_celery_app()
