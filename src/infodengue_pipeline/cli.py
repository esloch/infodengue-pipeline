"""Human-facing local operator commands."""

import click

from minio import Minio

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.storage.minio import (
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
