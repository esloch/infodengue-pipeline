"""Confirmed Celery publication of versioned domain events."""

from celery import Celery
from infodengue_pipeline.domain.events import IngestionCompleted


class CeleryEventPublisher:
    """Publish events through Celery's RabbitMQ confirmed transport."""

    def __init__(self, app: Celery) -> None:
        self._app = app

    def publish(self, event: IngestionCompleted) -> None:
        """Send an event task and propagate broker confirmation errors."""
        self._app.send_task(
            'pipeline.events.ingestion_completed',
            args=[event.model_dump(mode='json')],
            queue='pipeline.events',
            routing_key='pipeline.events',
            serializer='json',
            retry=True,
        )
