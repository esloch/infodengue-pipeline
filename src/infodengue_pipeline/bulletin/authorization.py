"""Explicit resource-bound authorization for bulletin scheduling."""

import json
import re

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from infodengue_pipeline.config import Settings


def _database_identity(url: str) -> str:
    parsed = make_url(url)
    if not parsed.host or not parsed.database:
        raise ValueError('database identity is incomplete')
    return f'{parsed.host}:{parsed.port or 5432}/{parsed.database}'


def bulletin_authorized(settings: Settings) -> bool:
    """Permit dev or an exactly matched staging/live production approval."""
    if not settings.bulletin_enabled:
        return False
    if settings.environment == 'dev':
        return True
    if settings.environment != 'prod':
        return False
    if settings.bulletin_authorization_file is None:
        return False
    try:
        source = settings.alerta_source_database_url
        history = settings.bulletin_history_database_url
        if source is None or history is None:
            return False
        source_id = _database_identity(source.get_secret_value())
        target_id = _database_identity(history.get_secret_value())
        approved = json.loads(
            settings.bulletin_authorization_file.read_text(encoding='utf-8')
        )
        if not isinstance(approved, dict):
            return False
        pipeline_id = _database_identity(
            settings.database_url.get_secret_value()
        )
        if approved.get('profile') == 'recife-live-production':
            image = settings.bulletin_image
            reference_sha256 = settings.bulletin_reference_sha256
            if image is None or not re.fullmatch(
                r'[A-Za-z0-9][A-Za-z0-9._/-]*@sha256:[0-9a-f]{64}', image
            ):
                return False
            if reference_sha256 is None or not re.fullmatch(
                r'[0-9a-f]{64}', reference_sha256
            ):
                return False
            expected = {
                'profile': 'recife-live-production',
                'pipeline_database': pipeline_id,
                'source_database': source_id,
                'history_database': target_id,
                'reference': str(settings.bulletin_reference),
                'reference_sha256': reference_sha256,
                'config': str(settings.bulletin_config),
                'renderer_revision': settings.bulletin_renderer_revision,
                'bulletin_image': image,
                'output_root': str(settings.bulletin_output_root),
            }
            return (
                approved == expected
                and source_id != pipeline_id
                and target_id != pipeline_id
                and settings.bulletin_reference.is_absolute()
                and settings.bulletin_config.is_absolute()
                and settings.bulletin_output_root.is_absolute()
                and settings.bulletin_output_root
                != settings.analysis_output_root
            )

        if not settings.analysis_load:
            return False

        expected = {
            'profile': 'isolated-staging-rehearsal',
            'pipeline_database': pipeline_id,
            'source_database': source_id,
            'analysis_database': target_id,
            'minio_endpoint': settings.minio_endpoint,
            'minio_bucket': settings.minio_bucket,
            'minio_prefix': settings.minio_prefix,
            'renderer_revision': settings.bulletin_renderer_revision,
        }
        return (
            approved == expected
            and source_id != target_id
            and bool(settings.minio_prefix)
            and settings.minio_prefix.endswith('/')
            and not settings.minio_prefix.startswith('/')
            and '..' not in settings.minio_prefix.split('/')
            and source_id != expected['pipeline_database']
            and target_id != expected['pipeline_database']
        )
    except (OSError, ValueError, TypeError, KeyError, ArgumentError):
        return False
