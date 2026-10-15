"""Thin Celery adapters for the pipeline application services."""

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from uuid import UUID

from infodengue_pipeline.analysis.runner import OCIAnalysisRunner
from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.db.engine import session_factory, unit_of_work
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    AnalysisJobStatus,
    WorkflowRow,
)
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.ingestion.completion import (
    CompletedIngestionSource,
    IngestionCompletionBridge,
    build_source,
)
from infodengue_pipeline.logging import configure_logging
from infodengue_pipeline.messaging.celery import celery_app
from infodengue_pipeline.messaging.publisher import (
    CeleryEventPublisher,
)
from infodengue_pipeline.models import Coverage, Epiweek, WorkflowStatus
from infodengue_pipeline.services import (
    CoordinateEpiweek,
    PublishOutbox,
)
from infodengue_pipeline.storage.ingress import IngressCoordinator
from infodengue_pipeline.storage.minio import build_object_store
from sqlalchemy import or_, select


def _analysis_jobs(settings: Settings | None = None) -> AnalysisJobService:
    """Build the persisted finite-job service from validated settings."""
    config = settings or get_settings()
    if not config.analysis_image:
        raise ValueError('analysis image is not configured')
    return AnalysisJobService(
        session_factory(),
        image=config.analysis_image,
        cores=config.analysis_cores,
        load=config.analysis_load,
        output_root=config.analysis_output_root,
    )


@lru_cache(maxsize=4)
def _completed_source(url: str) -> CompletedIngestionSource:
    return build_source(url)


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


@celery_app.task(name='pipeline.ingestion.scan')  # type: ignore[untyped-decorator]
def scan_ingress() -> int:
    """Discover MinIO files and atomically materialize supported objects."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return IngressCoordinator(
        session_factory(),
        build_object_store(settings),
        settings.incoming_root,
        settings.minio_prefix,
    ).scan()


@celery_app.task(name='pipeline.events.bridge_completed')  # type: ignore[untyped-decorator]
def bridge_completed(limit: int = 100) -> int:
    """Poll completed AlertaDengue runs and persist deterministic events."""
    settings = get_settings()
    configure_logging(settings.log_level)
    if settings.alerta_database_url is None:
        return 0
    return IngestionCompletionBridge(
        session_factory(),
        _completed_source(settings.alerta_database_url.get_secret_value()),
    ).poll(limit)


@celery_app.task(name='pipeline.analysis.dispatch_ready')  # type: ignore[untyped-decorator]
def dispatch_ready(limit: int = 100) -> int:
    """Create logical jobs for ready workflows and dispatch only job IDs."""
    settings = get_settings()
    configure_logging(settings.log_level)
    if not settings.analysis_image:
        return 0
    retry_before = datetime.now(timezone.utc) - timedelta(
        seconds=settings.analysis_dispatch_retry_seconds
    )
    with session_factory()() as session:
        workflows = session.scalars(
            select(WorkflowRow)
            .where(WorkflowRow.status == WorkflowStatus.READY)
            .order_by(WorkflowRow.created_at)
            .limit(limit)
        ).all()
        identities = [
            (row.coverage, Epiweek.parse(row.epiweek)) for row in workflows
        ]
        queued_job_ids = session.scalars(
            select(AnalysisJobRow.job_id)
            .where(
                AnalysisJobRow.status == AnalysisJobStatus.QUEUED,
                or_(
                    AnalysisJobRow.dispatched_at.is_(None),
                    AnalysisJobRow.dispatched_at <= retry_before,
                ),
            )
            .order_by(AnalysisJobRow.created_at)
            .limit(limit)
        ).all()
    jobs = _analysis_jobs(settings)
    job_ids = set(queued_job_ids)
    for coverage, epiweek in identities:
        job_id = jobs.claim_ready(Coverage(coverage), epiweek)
        if job_id is not None:
            job_ids.add(job_id)
    dispatched = 0
    for job_id in sorted(job_ids, key=str):
        celery_app.send_task(
            'pipeline.analysis.run',
            args=[str(job_id)],
            queue='pipeline.analysis',
            routing_key='pipeline.analysis',
            retry=True,
        )
        with session_factory()() as session, session.begin():
            job = session.get(AnalysisJobRow, job_id)
            if job is not None and job.status == AnalysisJobStatus.QUEUED:
                job.dispatched_at = datetime.now(timezone.utc)
        dispatched += 1
    return dispatched


@celery_app.task(name='pipeline.analysis.run')  # type: ignore[untyped-decorator]
def run_analysis(job_id: str) -> bool:
    """Atomically claim and execute one persisted analysis job."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return _analysis_jobs(settings).execute(
        UUID(job_id), OCIAnalysisRunner(settings)
    )


@celery_app.task(name='pipeline.analysis.retry')  # type: ignore[untyped-decorator]
def retry_analysis(job_id: str) -> bool:
    """Explicitly requeue a FAILED job; no automatic scientific retry."""
    settings = get_settings()
    configure_logging(settings.log_level)
    if not _analysis_jobs(settings).retry(UUID(job_id)):
        return False
    celery_app.send_task(
        'pipeline.analysis.run',
        args=[job_id],
        queue='pipeline.analysis',
        routing_key='pipeline.analysis',
        retry=True,
    )
    return True
