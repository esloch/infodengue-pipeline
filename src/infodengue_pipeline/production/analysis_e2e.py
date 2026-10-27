"""Operator gated production-profile analysis rehearsal on staging only."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import struct

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from infodengue_pipeline.config import Settings
from infodengue_pipeline.ingestion.completion import build_source
from infodengue_pipeline.production.target import (
    ANALYSIS_E2E_DATABASE,
    ANALYSIS_E2E_HOST,
    ANALYSIS_E2E_ROLE,
    RESTORE_EVIDENCE_SQL,
)
from infodengue_pipeline.staging.e2e import ValidationResult, validate_e2e
from infodengue_pipeline.staging.runner import (
    Fixture,
    object_key,
    require_absent,
    submit,
    writer_client,
)
from infodengue_pipeline.storage.ingress import file_sha256


@dataclass(frozen=True)
class Target:
    """Non-secret identity checked against the live analysis connection."""

    host: str
    database: str
    role: str


_TARGET_FAILURE = 'production analysis E2E failed: target is not staging'
_BACKUP_FAILURE = 'production analysis E2E failed: backup not verified'


def validate_artifacts(result: ValidationResult) -> None:
    """Check structural scientific outputs after the real analysis job."""
    root = result.output_dir
    week = str(result.epiweek)
    for path in (
        root / 'alertas' / week / f'ale-{result.coverage.value}-{week}.RData',
        root / 'alertas' / 'BR' / f'ale-BR-{week}.RData',
    ):
        if not path.is_file():
            raise RuntimeError('production analysis E2E failed: RData missing')
        with path.open('rb') as stream:
            prefix = stream.read(2)
        if prefix == b'\x1f\x8b':
            with gzip.open(path, 'rb') as stream:
                signature = stream.read(4)
        else:
            with path.open('rb') as stream:
                signature = stream.read(4)
        if signature not in {b'RDX2', b'RDX3', b'RDA2', b'RDA3'}:
            raise RuntimeError('production analysis E2E failed: invalid RData')
    targets = {
        'dengue': 'Historico_alerta',
        'chik': 'Historico_alerta_chik',
        'zika': 'Historico_alerta_zika',
    }
    for disease, table in targets.items():
        path = root / 'sql' / f'output_{disease}.sql'
        if disease == 'zika' and not path.exists():
            continue
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(
                'production analysis E2E failed: expected '
                f'{disease} SQL missing'
            )
        sql = path.read_text()
        names = re.findall(
            r'INSERT\s+INTO\s+"Municipio"\."([^"]+)"',
            sql,
            flags=re.IGNORECASE,
        )
        if not names or set(names) != {table}:
            raise RuntimeError(
                f'production analysis E2E failed: invalid {disease} SQL target'
            )
        if re.search(r'\b(?:DELETE|TRUNCATE|ALTER|DROP)\b', sql, re.I):
            raise RuntimeError(
                f'production analysis E2E failed: invalid {disease} SQL'
            )
    maps = list((root / 'incidence_maps').rglob('*.png'))
    if not maps:
        raise RuntimeError('production analysis E2E failed: maps missing')
    for path in maps:
        with path.open('rb') as stream:
            header = stream.read(24)
        if (
            len(header) != 24
            or not header.startswith(b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR')
            or min(struct.unpack('>II', header[16:24])) <= 0
        ):
            raise RuntimeError('production analysis E2E failed: invalid map')


def require_profile(settings: Settings) -> None:
    """Refuse any profile other than the deliberate mutating rehearsal."""
    if settings.environment != 'prod':
        raise ValueError(
            'production analysis E2E failed: environment is not prod'
        )
    if not settings.analysis_load:
        raise ValueError('production analysis E2E failed: analysis_load=false')
    if not settings.minio_prefix or not settings.minio_prefix.endswith('/'):
        raise ValueError('production analysis E2E failed: unsafe MinIO prefix')
    if (
        os.environ.get('PROD_INTEGRATION_NETWORK')
        != 'infodengue-staging_infodengue'
        or settings.minio_endpoint != 'minio:9000'
        or settings.minio_secure
        or not settings.minio_bucket.startswith('issue10-')
        or not settings.minio_prefix.startswith('issue10/')
    ):
        raise ValueError(
            'production analysis E2E failed: external staging scope unverified'
        )


def verify_source_staging(settings: Settings) -> None:
    """Check the AlertaDengue read connection on the staging network."""
    secret = settings.alerta_source_database_url
    if secret is None:
        raise ValueError('production analysis E2E failed: source DB absent')
    url = secret.get_secret_value()
    parsed = make_url(url)
    if parsed.host != 'postgres' or parsed.database != 'alerta':
        raise ValueError(
            'production analysis E2E failed: source database is not staging'
        )
    engine = create_engine(url, hide_parameters=True)
    try:
        with engine.connect() as connection:
            actual = connection.execute(
                text('SELECT current_database(), current_user')
            ).one()
            if tuple(actual) != ('alerta', parsed.username):
                raise ValueError(
                    'production analysis E2E failed: source identity differs'
                )
            writable = connection.execute(
                text(
                    'SELECT has_table_privilege(current_user, '
                    "'ingestion.run', "
                    "'INSERT') OR has_table_privilege(current_user, "
                    "'ingestion.run', 'UPDATE') OR has_table_privilege("
                    "current_user, 'ingestion.run', 'DELETE')"
                )
            ).scalar_one()
            if writable:
                raise ValueError(
                    'production analysis E2E failed: source role is writable'
                )
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(
            'production analysis E2E failed: source database probe failed'
        ) from exc
    finally:
        engine.dispose()


def verify_target(settings: Settings, expected: Target) -> None:
    """Require the fixed isolated clone and its effective DB identity."""
    secret = settings.analysis_database_url
    if secret is None:
        raise ValueError('production analysis E2E failed: analysis DB absent')
    url = secret.get_secret_value()
    parsed = make_url(url)
    if (expected.host, expected.database, expected.role) != (
        ANALYSIS_E2E_HOST,
        ANALYSIS_E2E_DATABASE,
        ANALYSIS_E2E_ROLE,
    ) or (parsed.host, parsed.database, parsed.username) != (
        expected.host,
        expected.database,
        expected.role,
    ):
        raise ValueError(_TARGET_FAILURE)
    engine = create_engine(url, hide_parameters=True)
    try:
        with engine.connect() as connection:
            actual = connection.execute(
                text('SELECT current_database(), current_user')
            ).one()
            if tuple(actual) != (expected.database, expected.role):
                raise ValueError(_TARGET_FAILURE)
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(
            'production analysis E2E failed: target database probe failed'
        ) from exc
    finally:
        engine.dispose()


def verify_restore_marker(settings: Settings, digest: str) -> None:
    """Tie the verified dump to an independently restored clone marker."""
    secret = settings.analysis_database_url
    if secret is None:
        raise ValueError(_BACKUP_FAILURE)
    engine = create_engine(secret.get_secret_value(), hide_parameters=True)
    try:
        with engine.connect() as connection:
            actual = connection.execute(
                text(RESTORE_EVIDENCE_SQL), {'environment': 'staging'}
            ).scalar_one()
            if actual != digest:
                raise ValueError(_BACKUP_FAILURE)
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(
            'production analysis E2E failed: restore marker query failed'
        ) from exc
    finally:
        engine.dispose()


def verify_backup(
    path: Path, digest: str, restore_proof: Path, expected: Target
) -> None:
    """Require a checked dump and separately recorded restore test."""
    if (
        not path.is_file()
        or not restore_proof.is_file()
        or len(digest) != 64
        or any(c not in '0123456789abcdef' for c in digest)
    ):
        raise ValueError(_BACKUP_FAILURE)
    with path.open('rb') as stream:
        if stream.read(5) != b'PGDMP':
            raise ValueError(_BACKUP_FAILURE)
    hasher = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            hasher.update(chunk)
    if hasher.hexdigest() != digest:
        raise ValueError(_BACKUP_FAILURE)
    try:
        proof = json.loads(restore_proof.read_text())
        verified = (
            proof['backup_sha256'] == digest
            and proof['source_environment'] == 'staging'
            and bool(proof['source_host'])
            and proof['source_host'] != expected.host
            and bool(proof['source_database'])
            and proof['source_database'] != expected.database
            and bool(proof['source_role'])
            and proof['restore_test_succeeded'] is True
            and proof['restored_database'] == expected.database
        )
    except (OSError, ValueError, KeyError, TypeError):
        verified = False
    if not verified:
        raise ValueError(_BACKUP_FAILURE)


def run(
    settings: Settings,
    fixture_path: Path,
    expected: Target,
    backup: Path,
    backup_sha256: str,
    restore_proof: Path,
    *,
    execute: bool,
    timeout: float = 7200,
) -> ValidationResult:
    """Preflight all operator gates before uploading exactly one object."""
    require_profile(settings)
    if not execute:
        raise ValueError(
            'production analysis E2E failed: execution gate missing'
        )
    if timeout <= 0:
        raise ValueError('production analysis E2E failed: invalid timeout')
    verify_target(settings, expected)
    verify_backup(backup, backup_sha256, restore_proof, expected)
    verify_restore_marker(settings, backup_sha256)
    verify_source_staging(settings)
    if not fixture_path.is_file() or fixture_path.suffix.lower() not in {
        '.csv',
        '.dbf',
    }:
        raise ValueError('production analysis E2E failed: invalid fixture')
    size = fixture_path.stat().st_size
    if size <= 0:
        raise ValueError('production analysis E2E failed: empty fixture')
    digest = file_sha256(fixture_path)
    source_secret = settings.alerta_source_database_url
    if source_secret is None:
        raise ValueError('production analysis E2E failed: source DB absent')
    try:
        fresh = not build_source(
            source_secret.get_secret_value()
        ).exists_sha256(digest)
    except Exception as exc:
        raise RuntimeError(
            'production analysis E2E failed: fixture freshness probe failed'
        ) from exc
    if not fresh:
        raise ValueError(
            'production analysis E2E failed: fixture SHA already ingested'
        )
    fixture = Fixture(fixture_path, size, digest, fixture_path.suffix.lower())
    key = object_key(settings.minio_prefix, fixture)
    client = writer_client(settings)
    require_absent(client, settings.minio_bucket, key)
    submit(settings, fixture, client, key)
    result = validate_e2e(
        key, timeout, settings, environment='prod', load=True
    )
    if result.attempts != 1:
        raise RuntimeError(
            'production analysis E2E failed: analysis job '
            f'attempts={result.attempts}'
        )
    validate_artifacts(result)
    return result
