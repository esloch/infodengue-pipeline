"""RabbitMQ event publication adapter tests."""

from unittest.mock import Mock
from uuid import uuid4

from infodengue_pipeline.adapters.rabbitmq.publisher import (
    CeleryEventPublisher,
)
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.events import IngestionCompleted


def test_celery_event_publisher_sends_event_to_events_queue() -> None:
    """The producer publishes a JSON event task without local registration."""
    app = Mock()
    event = IngestionCompleted.new(
        run_id=uuid4(),
        disease=Disease.DENGUE,
        epiweek=Epiweek.parse(202639),
        fingerprint='smoke-test',
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
