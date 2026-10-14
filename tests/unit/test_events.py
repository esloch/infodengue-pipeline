"""RabbitMQ event publication adapter tests."""

from unittest.mock import Mock
from uuid import uuid4

from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.messaging.publisher import (
    CeleryEventPublisher,
)
from infodengue_pipeline.models import Coverage, Disease, Epiweek


def test_celery_event_publisher_sends_event_to_events_queue() -> None:
    """The producer publishes a JSON event task without local registration."""
    app = Mock()
    event = IngestionCompleted.new(
        run_id=uuid4(),
        disease=Disease.DENGUE,
        coverage=Coverage.BR,
        epiweek=Epiweek.parse(202639),
        fingerprint='validation-test',
    )

    CeleryEventPublisher(app).publish(event)

    app.send_task.assert_called_once_with(
        'pipeline.events.ingestion_completed',
        args=[event.model_dump(mode='json')],
        queue='pipeline.events',
        routing_key='pipeline.events',
        serializer='json',
        retry=True,
    )
