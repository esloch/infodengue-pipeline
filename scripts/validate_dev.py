"""Exercise local development services without starting scientific analysis."""

from __future__ import annotations

import time

from io import BytesIO
from uuid import uuid4

import structlog

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.db.models import WorkflowRow
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.messaging.celery import celery_app
from infodengue_pipeline.messaging.publisher import (
    CeleryEventPublisher,
)
from infodengue_pipeline.models import Coverage, Disease, Epiweek
from infodengue_pipeline.storage.minio import build_object_store
from minio import Minio
from sqlalchemy import select

logger = structlog.get_logger(__name__)


def main() -> None:
    """Write a temporary dev object and exercise the event worker path."""
    settings = get_settings()
    if settings.environment != 'dev':
        raise RuntimeError('validation test is restricted to environment=dev')
    if settings.minio_bucket != 'sinan-infodengue-dev':
        raise RuntimeError(
            'validation test requires the development ingress bucket'
        )
    if settings.minio_endpoint not in {'localhost:9002', '127.0.0.1:9002'}:
        raise RuntimeError('validation test requires local development MinIO')
    if 'localhost:5434' not in settings.database_url.get_secret_value():
        raise RuntimeError(
            'validation test requires the local development database'
        )
    if 'localhost:5673' not in settings.rabbitmq_url.get_secret_value():
        raise RuntimeError(
            'validation test requires local development RabbitMQ'
        )

    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        secure=settings.minio_secure,
    )
    if not client.bucket_exists(settings.minio_bucket):
        raise RuntimeError('development bucket has not been bootstrapped')
    key = f'validation/{uuid4()}/sinan-fixture.csv'
    body = b'NU_NOTIFIC,ID_AGRAVO,SEM_NOT\nvalidation,A90,202639\n'
    client.put_object(
        settings.minio_bucket,
        key,
        BytesIO(body),
        len(body),
        content_type='text/csv',
    )
    store = build_object_store(settings)
    metadata = store.stat(key)
    if metadata.size != len(body) or key not in {
        item.key for item in store.list('validation/')
    }:
        raise RuntimeError('MinIO validation fixture stat/list mismatch')
    destination = settings.incoming_root / key
    materialized = store.materialize(key, destination)
    if materialized.size != len(body) or destination.read_bytes() != body:
        raise RuntimeError('MinIO validation fixture materialization mismatch')
    client.remove_object(settings.minio_bucket, key)
    destination.unlink(missing_ok=True)
    logger.info('minio_validation_passed', object_key=key)

    week = Epiweek.parse(202639)
    publisher = CeleryEventPublisher(celery_app)
    for disease in (Disease.DENGUE, Disease.CHIKUNGUNYA):
        event = IngestionCompleted.new(
            run_id=uuid4(),
            disease=disease,
            coverage=Coverage.BR,
            epiweek=week,
            fingerprint=f'dev-validation-{disease.value}',
        )
        publisher.publish(event)

    factory = session_factory()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with factory() as session:
            row = session.scalar(
                select(WorkflowRow).where(
                    WorkflowRow.coverage == Coverage.BR,
                    WorkflowRow.epiweek == int(week),
                )
            )
            if row is not None and row.status.value == 'ready':
                logger.info('workflow_validation_passed', epiweek=str(week))
                logger.info('analysis_runner_not_invoked')
                return
        time.sleep(0.5)
    raise TimeoutError('worker did not record same-epiweek readiness in 30s')


if __name__ == '__main__':
    main()
