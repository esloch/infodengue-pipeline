"""Probe production-profile dependencies from their respective workers."""

import argparse
import os

from collections.abc import Callable

from kombu import Connection
from sqlalchemy import create_engine, text

from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.storage.minio import build_object_store

_HEAD_REVISION = '0002_ingress_analysis_jobs'


def check_database(url: str, *, migration: bool = False) -> None:
    """Connect and issue only SELECT queries, closing the pool afterward."""
    engine = create_engine(url, connect_args={'connect_timeout': 5})
    try:
        with engine.connect() as connection:
            connection.execute(text('SELECT 1')).scalar_one()
            if migration:
                revision = connection.execute(
                    text('SELECT version_num FROM alembic_version')
                ).scalar_one()
                if revision != _HEAD_REVISION:
                    raise RuntimeError('pipeline migrations are not at head')
                connection.execute(
                    text('SELECT 1 FROM pipeline.analysis_job LIMIT 1')
                )
    finally:
        engine.dispose()


def validate(role: str, settings: Settings) -> list[str]:
    """Check the boundaries available to one existing worker process."""
    if settings.environment != 'prod':
        raise ValueError('environment must be prod')
    if settings.analysis_load:
        raise ValueError('analysis_load must be false')
    if (
        not settings.analysis_image
        or '@sha256:' not in settings.analysis_image
    ):
        raise ValueError('analysis image must have a reviewed digest')
    if role not in {'ingestion', 'analysis'}:
        raise ValueError('unknown validation role')
    source_url = settings.alerta_source_database_url
    analysis_url = settings.analysis_database_url
    if role == 'ingestion' and source_url is None:
        raise ValueError('AlertaDengue source database is not configured')
    if role == 'analysis' and analysis_url is None:
        raise ValueError('analysis database is not configured')

    results: list[str] = []

    def check(name: str, probe: Callable[[], None]) -> None:
        try:
            probe()
        except Exception as exc:
            raise RuntimeError(f'{name}: unavailable or invalid') from exc
        results.append(f'{name}: ok')

    if role == 'ingestion':
        check(
            'pipeline PostgreSQL and migrations',
            lambda: check_database(
                settings.database_url.get_secret_value(), migration=True
            ),
        )

        def broker() -> None:
            with Connection(
                settings.rabbitmq_url.get_secret_value(),
                connect_timeout=5,
            ) as connection:
                connection.connect()

        check('RabbitMQ', broker)

        def bucket() -> None:
            if not build_object_store(settings).bucket_exists():
                raise RuntimeError('bucket missing or inaccessible')

        check('MinIO bucket', bucket)
        if source_url is None:
            raise ValueError('AlertaDengue source database is not configured')
        check(
            'AlertaDengue read-only source connection',
            lambda: check_database(source_url.get_secret_value()),
        )
        if not settings.incoming_root.is_dir():
            raise RuntimeError('incoming directory: missing')
        results.append('incoming directory: ok')
    elif role == 'analysis':
        if analysis_url is None:
            raise ValueError('analysis database is not configured')
        check(
            'analysis database',
            lambda: check_database(analysis_url.get_secret_value()),
        )
        output = settings.analysis_output_root
        if not output.is_dir() or not os.access(output, os.W_OK | os.X_OK):
            raise RuntimeError('analysis output directory: not writable')
        results.append('analysis output directory: ok')
    results.append(f'analysis image: {settings.analysis_image}')
    results.append('analysis_load: false')
    return results


def main() -> None:
    """Run worker probes without printing exception details or secrets."""
    parser = argparse.ArgumentParser()
    parser.add_argument('role', choices=('ingestion', 'analysis'))
    role = parser.parse_args().role
    try:
        for result in validate(role, get_settings()):
            print(result, flush=True)
    except (ValueError, RuntimeError) as exc:
        print(f'production validation failed: {exc}', flush=True)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
