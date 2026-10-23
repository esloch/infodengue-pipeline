"""Probe production-profile dependencies from their respective workers."""

import argparse
import os
import re

from collections.abc import Callable

import urllib3

from alembic.config import Config
from alembic.script import ScriptDirectory
from kombu import Connection
from sqlalchemy import create_engine, text
from urllib3.util import Timeout

from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.storage.minio import build_object_store

_PROBE_TIMEOUT_SECONDS = 5
_IMAGE_PATTERN = re.compile(
    r'[a-z0-9](?:[a-z0-9._:/-]*[a-z0-9])?@sha256:[0-9a-f]{64}'
)


def valid_analysis_image(image: str | None) -> bool:
    """Require a complete SHA-256 digest on a nonempty image reference."""
    return bool(image and _IMAGE_PATTERN.fullmatch(image))


def expected_migration_heads() -> set[str]:
    """Read the checked-in Alembic heads without running migrations."""
    return set(ScriptDirectory.from_config(Config('alembic.ini')).get_heads())


def check_database(url: str, *, migration: bool = False) -> None:
    """Connect and issue only SELECT queries, closing the pool afterward."""
    engine = create_engine(
        url,
        connect_args={
            'connect_timeout': _PROBE_TIMEOUT_SECONDS,
            'options': f'-c statement_timeout={_PROBE_TIMEOUT_SECONDS * 1000}',
        },
    )
    try:
        with engine.connect() as connection:
            connection.execute(text('SELECT 1')).scalar_one()
            if migration:
                revisions = set(
                    connection.execute(
                        text('SELECT version_num FROM alembic_version')
                    )
                    .scalars()
                    .all()
                )
                heads = expected_migration_heads()
                if not heads or revisions != heads:
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
    if not valid_analysis_image(settings.analysis_image):
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
                connect_timeout=_PROBE_TIMEOUT_SECONDS,
                transport_options={
                    'read_timeout': _PROBE_TIMEOUT_SECONDS,
                    'write_timeout': _PROBE_TIMEOUT_SECONDS,
                },
            ) as connection:
                connection.connect()

        check('RabbitMQ', broker)

        def bucket() -> None:
            http_client = urllib3.PoolManager(
                timeout=Timeout(
                    connect=_PROBE_TIMEOUT_SECONDS,
                    read=_PROBE_TIMEOUT_SECONDS,
                ),
                retries=False,
            )
            try:
                if not build_object_store(
                    settings, http_client=http_client
                ).bucket_exists():
                    raise RuntimeError('bucket missing or inaccessible')
            finally:
                http_client.clear()

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
