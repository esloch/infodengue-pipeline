"""Human-facing local operator commands."""

import click

from minio import Minio

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.ingestion.completion import build_source
from infodengue_pipeline.storage.ingress import IngressCoordinator
from infodengue_pipeline.storage.minio import (
    build_object_store,
    configure_development_lifecycle,
)


@click.group()
@click.version_option(package_name='infodengue-pipeline')
def app() -> None:
    """Operate the development pipeline foundation."""


@app.command('bootstrap-minio')
def bootstrap_minio() -> None:
    """Create the development ingress bucket and its seven-day lifecycle."""
    settings = get_settings()
    if settings.environment != 'dev':
        raise click.ClickException(
            'MinIO bootstrap is restricted to the development environment'
        )
    if settings.minio_bucket != 'sinan-infodengue-dev':
        raise click.ClickException(
            'MinIO bootstrap is restricted to sinan-infodengue-dev'
        )
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        secure=settings.minio_secure,
    )
    if not client.bucket_exists(settings.minio_bucket):
        client.make_bucket(settings.minio_bucket)
    configure_development_lifecycle(
        client,
        bucket=settings.minio_bucket,
        environment=settings.environment,
    )
    click.echo(f'Development ingress ready: {settings.minio_bucket}')


@app.command('reconcile-historical')
@click.option(
    '--object-key',
    multiple=True,
    required=True,
    help='Exact MinIO object key. Repeat for each approved object.',
)
@click.option(
    '--confirm',
    type=click.Choice(['historical-only'], case_sensitive=True),
    required=True,
    help='Confirm this operation records history only; it emits no events.',
)
def reconcile_historical(object_key: tuple[str, ...], confirm: str) -> None:
    """Record approved completed objects without publication or events."""
    del confirm
    settings = get_settings()
    if settings.environment not in {'staging', 'prod'}:
        raise click.ClickException(
            'historical reconciliation is restricted to staging or prod'
        )
    if settings.alerta_source_database_url is None:
        raise click.ClickException('AlertaDengue source database is required')
    source = build_source(
        settings.alerta_source_database_url.get_secret_value()
    )
    try:
        count = IngressCoordinator(
            session_factory(),
            build_object_store(settings),
            settings.incoming_root,
            settings.minio_prefix,
            source.reconcile_by_sha256,
        ).reconcile_historical(list(object_key))
    finally:
        source.close()
    click.echo(
        f'Reconciled {count} historical object(s); '
        'no incoming files or outbox events were created.'
    )
