"""Standalone Celery app and explicit RabbitMQ queues."""

from celery import Celery
from kombu import Exchange, Queue

from infodengue_pipeline.config import get_settings


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
            'pipeline.ingestion.scan': {
                'queue': 'pipeline.ingestion',
                'routing_key': 'pipeline.ingestion',
            },
            'pipeline.events.ingestion_completed': {
                'queue': 'pipeline.events',
                'routing_key': 'pipeline.events',
            },
            'pipeline.events.publish_pending': {
                'queue': 'pipeline.events',
                'routing_key': 'pipeline.events',
            },
            'pipeline.events.bridge_completed': {
                'queue': 'pipeline.events',
                'routing_key': 'pipeline.events',
            },
            'pipeline.health.ping': {
                'queue': 'pipeline.orchestration',
                'routing_key': 'pipeline.orchestration',
            },
            'pipeline.analysis.dispatch_ready': {
                'queue': 'pipeline.orchestration',
                'routing_key': 'pipeline.orchestration',
            },
            'pipeline.analysis.run': {
                'queue': 'pipeline.analysis',
                'routing_key': 'pipeline.analysis',
            },
            'pipeline.analysis.retry': {
                'queue': 'pipeline.analysis',
                'routing_key': 'pipeline.analysis',
            },
        },
        beat_schedule={
            'scan-minio-ingress': {
                'task': 'pipeline.ingestion.scan',
                'schedule': settings.minio_poll_seconds,
            },
            'bridge-completed-ingestion': {
                'task': 'pipeline.events.bridge_completed',
                'schedule': settings.bridge_poll_seconds,
            },
            'publish-pending-outbox': {
                'task': 'pipeline.events.publish_pending',
                'schedule': settings.outbox_poll_seconds,
            },
            'dispatch-ready-analysis': {
                'task': 'pipeline.analysis.dispatch_ready',
                'schedule': settings.analysis_poll_seconds,
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
