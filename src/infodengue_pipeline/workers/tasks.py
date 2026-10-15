"""Thin Celery adapters for the pipeline application services."""

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.engine import unit_of_work
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.logging import configure_logging
from infodengue_pipeline.messaging.celery import celery_app
from infodengue_pipeline.messaging.publisher import (
    CeleryEventPublisher,
)
from infodengue_pipeline.services import (
    CoordinateEpiweek,
    PublishOutbox,
)


@celery_app.task(name='pipeline.health.ping')  # type: ignore[untyped-decorator]
def ping() -> str:
    """Respond to development worker health checks."""
    return 'pong'


@celery_app.task(  # type: ignore[untyped-decorator]
    name='pipeline.events.ingestion_completed'
)
def ingestion_completed(payload: dict[str, object]) -> str:
    """Validate and record one ingested disease's epiweek readiness."""
    settings = get_settings()
    configure_logging(settings.log_level)
    event = IngestionCompleted.model_validate(payload)
    workflow = CoordinateEpiweek(
        unit_of_work,
        br_required_diseases=settings.br_required_diseases,
        uf_required_diseases=settings.uf_required_diseases,
    ).record(event)
    return workflow.status.value


@celery_app.task(  # type: ignore[untyped-decorator]
    name='pipeline.events.publish_pending'
)
def publish_pending(limit: int = 100) -> int:
    """Publish pending outbox rows through confirmed RabbitMQ transport."""
    configure_logging(get_settings().log_level)
    publisher = CeleryEventPublisher(celery_app)
    return PublishOutbox(unit_of_work, publisher).execute(limit)
