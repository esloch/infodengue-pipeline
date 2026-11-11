"""Explicit resource-bound authorization for a staging rehearsal."""

import json

from sqlalchemy.engine import make_url

from infodengue_pipeline.config import Settings


def _database_identity(url: str) -> str:
    parsed = make_url(url)
    if not parsed.host or not parsed.database:
        raise ValueError('database identity is incomplete')
    return f'{parsed.host}:{parsed.port or 5432}/{parsed.database}'


def bulletin_authorized(settings: Settings) -> bool:
    """Permit dev or an exactly matched, independently approved rehearsal."""
    if not settings.bulletin_enabled:
        return False
    if settings.environment == 'dev':
        return True
    if settings.environment != 'prod' or not settings.analysis_load:
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
        if source_id == target_id:
            return False
        expected = {
            'profile': 'isolated-staging-rehearsal',
            'pipeline_database': _database_identity(
                settings.database_url.get_secret_value()
            ),
            'source_database': source_id,
            'analysis_database': target_id,
            'minio_endpoint': settings.minio_endpoint,
            'minio_bucket': settings.minio_bucket,
            'minio_prefix': settings.minio_prefix,
            'renderer_revision': settings.bulletin_renderer_revision,
        }
        approved = json.loads(
            settings.bulletin_authorization_file.read_text(encoding='utf-8')
        )
        return (
            approved == expected
            and bool(settings.minio_prefix)
            and settings.minio_prefix.endswith('/')
            and not settings.minio_prefix.startswith('/')
            and '..' not in settings.minio_prefix.split('/')
            and source_id != expected['pipeline_database']
            and target_id != expected['pipeline_database']
        )
    except (OSError, ValueError, TypeError, KeyError):
        return False
