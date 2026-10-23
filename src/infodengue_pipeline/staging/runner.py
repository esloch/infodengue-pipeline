"""Guarded submission of a fresh staging fixture."""

from __future__ import annotations

import os

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from minio import Minio
from minio.error import S3Error

from infodengue_pipeline.config import Settings
from infodengue_pipeline.ingestion.completion import build_source
from infodengue_pipeline.staging.e2e import ValidationResult, validate_e2e
from infodengue_pipeline.storage.ingress import file_sha256


@dataclass(frozen=True)
class Fixture:
    """Validated local fixture metadata."""

    path: Path
    size: int
    sha256: str
    suffix: str


def preflight(settings: Settings, path: Path, timeout: float) -> Fixture:
    """Validate settings and stream the local file hash before any upload."""
    if settings.environment != 'staging':
        raise ValueError(
            'fixture preflight: staging E2E requires environment=staging'
        )
    if settings.analysis_load:
        raise ValueError(
            'fixture preflight: staging E2E requires analysis_load=false'
        )
    if settings.alerta_source_database_url is None:
        raise ValueError(
            'fixture preflight: AlertaDengue source database is not configured'
        )
    if not settings.minio_prefix:
        raise ValueError('fixture preflight: configured MinIO prefix is empty')
    if (
        not settings.minio_prefix.endswith('/')
        or settings.minio_prefix.startswith('/')
        or '..' in settings.minio_prefix.split('/')
    ):
        raise ValueError('fixture preflight: unsafe MinIO prefix')
    if timeout <= 0:
        raise ValueError('fixture preflight: timeout-seconds must be positive')
    if not path.is_file():
        raise ValueError(
            'fixture preflight: fixture must exist and be a regular file'
        )
    suffix = path.suffix.lower()
    if suffix not in {'.csv', '.dbf'}:
        raise ValueError(
            'fixture preflight: fixture must have .csv or .dbf suffix'
        )
    fixture = Fixture(path, path.stat().st_size, file_sha256(path), suffix)
    print(f'[OK] fixture sha256={fixture.sha256}', flush=True)
    return fixture


def object_key(
    prefix: str,
    fixture: Fixture,
    *,
    now: datetime | None = None,
    nonce: str | None = None,
) -> str:
    """Generate a contained key without using filename parts."""
    if not prefix or prefix.startswith('/') or '..' in prefix.split('/'):
        raise ValueError('fixture preflight: unsafe MinIO prefix')
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    token = nonce or uuid4().hex[:12]
    return (
        f'{prefix}e2e-{timestamp:%Y%m%dT%H%M%SZ}-'
        f'{fixture.sha256[:12]}-{token}{fixture.suffix}'
    )


def writer_client(settings: Settings) -> Minio:
    """Create an upload client from dedicated writer credentials."""
    access_key = os.environ.get('STAGING_E2E_MINIO_ACCESS_KEY')
    secret_key = os.environ.get('STAGING_E2E_MINIO_SECRET_KEY')
    if not access_key or not secret_key:
        raise ValueError(
            'writer credentials: STAGING_E2E_MINIO_ACCESS_KEY and '
            'STAGING_E2E_MINIO_SECRET_KEY are required'
        )
    return Minio(
        settings.minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=settings.minio_secure,
    )


def require_absent(client: Minio, bucket: str, key: str) -> None:
    """Inspect only the exact target key and never overwrite it."""
    try:
        client.stat_object(bucket, key)
    except S3Error as exc:
        if exc.code in {'NoSuchKey', 'NoSuchObject'}:
            return
        raise
    raise RuntimeError(f'object key already exists: {key}')


def submit(
    settings: Settings, fixture: Fixture, client: Minio, key: str
) -> None:
    """Upload from disk and verify the exact object's resulting size."""
    require_absent(client, settings.minio_bucket, key)
    print(f'[II] uploading fixture object_key={key}', flush=True)
    client.fput_object(settings.minio_bucket, key, str(fixture.path))
    remote = client.stat_object(settings.minio_bucket, key)
    if remote.size != fixture.size:
        raise RuntimeError(
            'fixture upload: uploaded object size does not match local fixture'
        )
    print('[OK] fixture uploaded and size verified', flush=True)


def require_clean_result(result: ValidationResult) -> None:
    """Require one successful scientific execution attempt."""
    if result.attempts != 1:
        raise RuntimeError(
            'analysis acceptance: run was not clean '
            f'(attempts={result.attempts})'
        )


def run(
    settings: Settings, path: Path, timeout: float = 7200
) -> ValidationResult:
    """Preflight, check upstream freshness, submit, and reuse validation."""
    fixture = preflight(settings, path, timeout)
    source_url = settings.alerta_source_database_url
    if source_url is None:
        raise ValueError(
            'fixture preflight: source database is not configured'
        )
    client = writer_client(settings)
    source = build_source(source_url.get_secret_value())
    print('[II] checking AlertaDengue ingestion.run SHA freshness', flush=True)
    try:
        exists = source.exists_sha256(fixture.sha256)
    except Exception as exc:
        raise RuntimeError(
            'fixture freshness: upstream SHA query failed'
        ) from exc
    if exists:
        raise RuntimeError(
            'fixture freshness: fixture SHA256 already exists in '
            'AlertaDengue ingestion.run; use a fresh fixture'
        )
    print('[OK] fixture SHA256 is fresh', flush=True)
    key = object_key(settings.minio_prefix, fixture)
    try:
        submit(settings, fixture, client, key)
    except Exception as exc:
        raise RuntimeError(f'fixture upload: {exc}') from exc
    result = validate_e2e(key, timeout, settings)
    require_clean_result(result)
    return result
