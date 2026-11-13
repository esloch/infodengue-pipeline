"""Probe production-profile dependencies from their respective workers."""

import argparse
import json
import os
import re

from collections.abc import Callable
from pathlib import Path

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


def inspect_rehearsal_database(
    role: str, settings: Settings
) -> dict[str, object]:
    """Read the reached PostgreSQL identity and required table grants."""
    if role not in {
        'rehearsal-source',
        'rehearsal-analysis',
        'rehearsal-history',
    }:
        raise ValueError('unknown rehearsal database role')
    if settings.environment != 'prod':
        raise ValueError('environment must be prod')
    urls = {
        'rehearsal-source': settings.alerta_source_database_url,
        'rehearsal-analysis': settings.analysis_database_url,
        'rehearsal-history': settings.bulletin_history_database_url,
    }
    url = urls[role]
    if url is None:
        raise ValueError(f'{role}: database URL is not configured')
    try:
        engine = create_engine(
            url.get_secret_value(),
            connect_args={
                'connect_timeout': _PROBE_TIMEOUT_SECONDS,
                'options': (
                    f'-c statement_timeout={_PROBE_TIMEOUT_SECONDS * 1000}'
                ),
            },
        )
    except Exception as exc:
        raise RuntimeError(f'{role}: database unavailable') from exc
    try:
        with engine.connect() as connection:
            database, address, port, user, readonly, recovery = (
                connection.execute(
                    text(
                        'SELECT current_database(), inet_server_addr()::text, '
                        'inet_server_port(), current_user, '
                        "current_setting('transaction_read_only'), "
                        'pg_is_in_recovery()'
                    )
                ).one()
            )
            if not address or not port or recovery:
                raise ValueError('database server identity is unavailable')
            if role == 'rehearsal-analysis' and readonly != 'off':
                raise ValueError('analysis database is read-only')
            if role == 'rehearsal-history' and readonly != 'on':
                raise ValueError('history database session is not read-only')
            tables = ['Historico_alerta', 'Historico_alerta_chik']
            if role == 'rehearsal-source':
                tables.append('Notificacao')
            required = (
                {'SELECT', 'INSERT', 'UPDATE'}
                if role == 'rehearsal-analysis'
                else {'SELECT'}
            )
            for table in tables:
                relation = f'"Municipio"."{table}"'
                for privilege in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                    if role == 'rehearsal-analysis' and privilege == 'DELETE':
                        continue
                    granted = connection.scalar(
                        text(
                            'SELECT has_table_privilege('
                            'current_user, :relation, :privilege)'
                        ),
                        {'relation': relation, 'privilege': privilege},
                    )
                    if bool(granted) != (privilege in required):
                        raise ValueError(
                            f'{role}: unexpected {privilege} grant on {table}'
                        )
            return {
                'database': str(database),
                'server_address': str(address),
                'server_port': int(port),
                'current_user': str(user),
                'role': role,
            }
    except Exception as exc:
        raise RuntimeError(
            f'{role}: database identity or grants invalid'
        ) from exc
    finally:
        engine.dispose()


def verify_rehearsal_target(
    writer: dict[str, object],
    reader: dict[str, object],
    approved: tuple[str, str, int],
) -> None:
    """Match both live probes to the independently approved target."""
    if not isinstance(writer, dict) or not isinstance(reader, dict):
        raise ValueError('rehearsal probe output is invalid')
    writer_identity = (
        writer.get('database'),
        writer.get('server_address'),
        writer.get('server_port'),
    )
    reader_identity = (
        reader.get('database'),
        reader.get('server_address'),
        reader.get('server_port'),
    )
    if (
        writer.get('role') != 'rehearsal-analysis'
        or reader.get('role') != 'rehearsal-history'
        or writer_identity != approved
        or reader_identity != approved
    ):
        raise ValueError('rehearsal target identity does not match approval')


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
    parser.add_argument(
        'role',
        choices=(
            'ingestion',
            'analysis',
            'rehearsal-source',
            'rehearsal-analysis',
            'rehearsal-history',
            'rehearsal-target',
        ),
    )
    parser.add_argument('--writer-probe', type=Path)
    parser.add_argument('--history-probe', type=Path)
    parser.add_argument('--approved-database')
    parser.add_argument('--approved-address')
    parser.add_argument('--approved-port', type=int)
    args = parser.parse_args()
    role = args.role
    try:
        if role == 'rehearsal-target':
            if not all(
                (
                    args.writer_probe,
                    args.history_probe,
                    args.approved_database,
                    args.approved_address,
                    args.approved_port,
                )
            ):
                raise ValueError('rehearsal target inputs are required')
            verify_rehearsal_target(
                json.loads(args.writer_probe.read_text(encoding='utf-8')),
                json.loads(args.history_probe.read_text(encoding='utf-8')),
                (
                    args.approved_database,
                    args.approved_address,
                    args.approved_port,
                ),
            )
            print('rehearsal target identity: approved', flush=True)
            return
        if role.startswith('rehearsal-'):
            print(json.dumps(inspect_rehearsal_database(role, get_settings())))
            return
        for result in validate(role, get_settings()):
            print(result, flush=True)
    except (ValueError, RuntimeError) as exc:
        print(f'production validation failed: {exc}', flush=True)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
