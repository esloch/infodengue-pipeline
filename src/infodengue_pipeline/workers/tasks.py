"""Thin Celery adapters for the pipeline application services."""

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from uuid import UUID

from infodengue_pipeline.analysis.runner import InContainerAnalysisRunner
from infodengue_pipeline.bulletin.authorization import bulletin_authorized
from infodengue_pipeline.bulletin.jobs import BulletinJobService
from infodengue_pipeline.bulletin.readiness import CheckBulletinReadiness
from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.db.engine import session_factory, unit_of_work
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
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
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    Coverage,
    Epiweek,
    WorkflowStatus,
)
from infodengue_pipeline.services import (
    CoordinateEpiweek,
    PublishOutbox,
)
from infodengue_pipeline.storage.ingress import IngressCoordinator
from infodengue_pipeline.storage.minio import build_object_store
from sqlalchemy import and_, or_, select, update


def _analysis_jobs(settings: Settings | None = None) -> AnalysisJobService:
    """Build the persisted finite-job service from validated settings."""
    config = settings or get_settings()
    if not config.analysis_image:
        raise ValueError('analysis image is not configured')
    return AnalysisJobService(
        session_factory(),
        worker_image=config.analysis_image,
        cores=config.analysis_cores,
        load=config.analysis_load,
        output_root=config.analysis_output_root,
        initial_analysis_week=(
            Epiweek.parse(config.analysis_initial_epiweek)
            if config.analysis_initial_epiweek is not None
            else None
        ),
    )


def _bulletin_jobs(settings: Settings) -> BulletinJobService:
    """Build the Recife service in its dedicated worker."""
    source_url = (
        settings.alerta_source_database_url.get_secret_value()
        if settings.alerta_source_database_url is not None
        else None
    )
    history_url = (
        settings.bulletin_history_database_url.get_secret_value()
        if settings.bulletin_history_database_url is not None
        else None
    )
    sessions = session_factory()
    return BulletinJobService(
        sessions,
        CheckBulletinReadiness(sessions, source_url, history_url),
        revision=settings.bulletin_renderer_revision,
        reference=settings.bulletin_reference,
        config=settings.bulletin_config,
        output_root=settings.bulletin_output_root,
        source_database_url=source_url,
        history_database_url=history_url,
    )


def _coordinator(settings: Settings) -> CoordinateEpiweek:
    """Build readiness coordination with the configured explicit bootstrap."""
    return CoordinateEpiweek(
        unit_of_work,
        br_required_diseases=settings.br_required_diseases,
        uf_required_diseases=settings.uf_required_diseases,
        initial_analysis_week=(
            Epiweek.parse(settings.analysis_initial_epiweek)
            if settings.analysis_initial_epiweek is not None
            else None
        ),
    )


def _claim_queued_dispatch(job_id: UUID, retry_before: datetime) -> bool:
    """Atomically claim one queued job before publishing its task message."""
    now = datetime.now(timezone.utc)
    with session_factory()() as session, session.begin():
        claimed = session.scalar(
            update(AnalysisJobRow)
            .where(
                AnalysisJobRow.job_id == job_id,
                AnalysisJobRow.status == AnalysisJobStatus.QUEUED,
                or_(
                    AnalysisJobRow.dispatched_at.is_(None),
                    AnalysisJobRow.dispatched_at <= retry_before,
                ),
            )
            .values(dispatched_at=now)
            .returning(AnalysisJobRow.job_id)
        )
    return claimed is not None


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
    workflow = _coordinator(settings).record(event)
    return workflow.status.value if workflow is not None else 'uninitialized'


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
    if settings.alerta_source_database_url is None:
        if settings.environment != 'dev':
            raise RuntimeError(
                'AlertaDengue source database is required for safe ingress'
            )
        completed_run_for_sha256 = None
    else:
        completed_run_for_sha256 = _completed_source(
            settings.alerta_source_database_url.get_secret_value()
        ).completed_run_id_by_sha256
    return IngressCoordinator(
        session_factory(),
        build_object_store(settings),
        settings.incoming_root,
        settings.minio_prefix,
        completed_run_for_sha256,
    ).scan()


@celery_app.task(name='pipeline.events.bridge_completed')  # type: ignore[untyped-decorator]
def bridge_completed(limit: int = 100) -> int:
    """Poll completed AlertaDengue runs and persist deterministic events."""
    settings = get_settings()
    configure_logging(settings.log_level)
    if settings.alerta_source_database_url is None:
        return 0
    return IngestionCompletionBridge(
        session_factory(),
        _completed_source(
            settings.alerta_source_database_url.get_secret_value()
        ),
    ).poll(limit)


@celery_app.task(name='pipeline.analysis.dispatch_ready')  # type: ignore[untyped-decorator]
def dispatch_ready(limit: int = 100) -> int:
    """Create logical jobs for ready workflows and dispatch only job IDs."""
    settings = get_settings()
    configure_logging(settings.log_level)
    if not settings.analysis_image:
        return 0
    candidates = {
        snapshot.readiness.coverage: snapshot.readiness.epiweek
        for snapshot in _coordinator(settings).reconcile_all()
    }
    if not candidates:
        return 0
    candidate_filter = or_(
        *(
            and_(
                WorkflowRow.coverage == coverage,
                WorkflowRow.epiweek == int(epiweek),
            )
            for coverage, epiweek in candidates.items()
        )
    )
    retry_before = datetime.now(timezone.utc) - timedelta(
        seconds=settings.analysis_dispatch_retry_seconds
    )
    with session_factory()() as session:
        workflows = session.scalars(
            select(WorkflowRow)
            .where(WorkflowRow.status == WorkflowStatus.READY)
            .where(candidate_filter)
            .order_by(WorkflowRow.created_at)
            .limit(limit)
        ).all()
        identities = [
            (row.coverage, Epiweek.parse(row.epiweek))
            for row in workflows
            if candidates.get(row.coverage) == Epiweek.parse(row.epiweek)
        ]
        queued_jobs = session.execute(
            select(
                AnalysisJobRow.job_id,
                WorkflowRow.coverage,
                WorkflowRow.epiweek,
            )
            .join(
                WorkflowRow,
                WorkflowRow.workflow_id == AnalysisJobRow.workflow_id,
            )
            .where(
                candidate_filter,
                AnalysisJobRow.status == AnalysisJobStatus.QUEUED,
                or_(
                    AnalysisJobRow.dispatched_at.is_(None),
                    AnalysisJobRow.dispatched_at <= retry_before,
                ),
            )
            .order_by(AnalysisJobRow.created_at)
            .limit(limit)
        ).all()
        queued_job_ids = [
            job_id for job_id, _coverage, _epiweek in queued_jobs
        ]
    jobs = _analysis_jobs(settings)
    job_ids = set(queued_job_ids)
    for coverage, epiweek in identities:
        job_id = jobs.claim_ready(Coverage(coverage), epiweek)
        if job_id is not None:
            job_ids.add(job_id)
    dispatched = 0
    for job_id in sorted(job_ids, key=str):
        if not _claim_queued_dispatch(job_id, retry_before):
            continue
        celery_app.send_task(
            'pipeline.analysis.run',
            args=[str(job_id)],
            queue='pipeline.analysis',
            routing_key='pipeline.analysis',
            retry=True,
        )
        dispatched += 1
    return dispatched


@celery_app.task(name='pipeline.analysis.run')  # type: ignore[untyped-decorator]
def run_analysis(job_id: str) -> bool:
    """Atomically claim and execute one persisted analysis job."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return _analysis_jobs(settings).execute(
        UUID(job_id), InContainerAnalysisRunner(settings)
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


@celery_app.task(name='pipeline.bulletin.dispatch_ready')  # type: ignore[untyped-decorator]
def dispatch_bulletins(limit: int = 100) -> int:
    """Publish ready Recife job IDs and recover expired broker leases."""
    settings = get_settings()
    if not bulletin_authorized(settings):
        return 0
    configure_logging(settings.log_level)
    jobs = _bulletin_jobs(settings)
    jobs.fail_interrupted(settings.bulletin_execution_timeout_seconds)
    created = jobs.dispatch_candidates(limit)
    queued = jobs.queued_candidates(limit)
    retry_before = datetime.now(timezone.utc) - timedelta(
        seconds=settings.bulletin_dispatch_retry_seconds
    )
    dispatched = 0
    for job_id in set(created + queued):
        if not jobs.claim_dispatch(job_id, retry_before):
            continue
        celery_app.send_task(
            'pipeline.bulletin.run',
            args=[str(job_id)],
            queue='pipeline.bulletin',
            routing_key='pipeline.bulletin',
            retry=True,
        )
        dispatched += 1
    return dispatched


@celery_app.task(name='pipeline.bulletin.run')  # type: ignore[untyped-decorator]
def run_bulletin(job_id: str) -> bool:
    """Run a queued bulletin once in the dedicated R/Quarto worker."""
    settings = get_settings()
    if not bulletin_authorized(settings):
        return False
    configure_logging(settings.log_level)
    return _bulletin_jobs(settings).execute(UUID(job_id))


@celery_app.task(name='pipeline.bulletin.retry')  # type: ignore[untyped-decorator]
def retry_bulletin(job_id: str) -> bool:
    """Explicitly requeue a failed bulletin for the next Beat poll."""
    settings = get_settings()
    if not bulletin_authorized(settings):
        return False
    configure_logging(settings.log_level)
    return _bulletin_jobs(settings).retry(UUID(job_id))
