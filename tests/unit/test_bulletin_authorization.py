"""Resource-bound bulletin rehearsal authorization."""

import json

from pathlib import Path

import pytest

from infodengue_pipeline.bulletin.authorization import bulletin_authorized
from infodengue_pipeline.config import Settings
from infodengue_pipeline.messaging.celery import create_celery_app
from infodengue_pipeline.staging.e2e import validate_rehearsal_e2e


def _settings(path: Path, **changes: object) -> Settings:
    values: dict[str, object] = {
        'environment': 'prod',
        'bulletin_enabled': True,
        'analysis_load': True,
        'bulletin_authorization_file': path,
        'database_url': 'postgresql+psycopg://user:secret@pipeline-postgres/pipeline_prod',
        'alerta_source_database_url': 'postgresql://reader:secret@staging-db/dengue',
        'bulletin_history_database_url': (
            'postgresql://history:secret@staging-analysis/dengue'
        ),
        'minio_endpoint': 'staging-minio:9000',
        'minio_bucket': 'staging-sinan',
        'minio_prefix': 'recife-rehearsal/',
    }
    values.update(changes)
    return Settings(_env_file=None, **values)


def _manifest(settings: Settings) -> dict[str, str]:
    return {
        'profile': 'isolated-staging-rehearsal',
        'pipeline_database': 'pipeline-postgres:5432/pipeline_prod',
        'source_database': 'staging-db:5432/dengue',
        'analysis_database': 'staging-analysis:5432/dengue',
        'minio_endpoint': settings.minio_endpoint,
        'minio_bucket': settings.minio_bucket,
        'minio_prefix': settings.minio_prefix,
        'renderer_revision': settings.bulletin_renderer_revision,
    }


def test_approved_resources_enable_rehearsal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Beat and worker share the same manifest-bound authorization."""
    path = tmp_path / 'authorization.json'
    settings = _settings(path)
    path.write_text(json.dumps(_manifest(settings)))
    assert bulletin_authorized(settings)
    monkeypatch.setattr(
        'infodengue_pipeline.messaging.celery.get_settings',
        lambda: settings,
    )
    assert 'dispatch-ready-bulletins' in create_celery_app().conf.beat_schedule


@pytest.mark.parametrize(
    'change',
    [
        {'bulletin_enabled': False},
        {'analysis_load': False},
        {'environment': 'staging'},
        {'bulletin_authorization_file': None},
        {'bulletin_history_database_url': None},
        {
            'bulletin_history_database_url': 'postgresql://history@staging-db/dengue'
        },
        {'minio_prefix': ''},
        {'minio_prefix': '../unsafe/'},
        {'minio_bucket': 'production-bucket'},
        {
            'bulletin_history_database_url': 'postgresql://history@production-db/dengue'
        },
        {'bulletin_renderer_revision': 'other-revision'},
    ],
)
def test_missing_or_mismatched_authorization_fails_closed(
    tmp_path: Path, change: dict[str, object]
) -> None:
    """A copied flag cannot authorize another resource or deployment."""
    path = tmp_path / 'authorization.json'
    approved = _settings(path)
    path.write_text(json.dumps(_manifest(approved)))
    assert not bulletin_authorized(_settings(path, **change))


def test_invalid_manifest_fails_closed(tmp_path: Path) -> None:
    """A missing or malformed manifest cannot enable scheduling."""
    path = tmp_path / 'authorization.json'
    assert not bulletin_authorized(_settings(path))
    path.write_text('{bad json')
    assert not bulletin_authorized(_settings(path))
    assert not bulletin_authorized(
        _settings(Path('/dev/null'), bulletin_enabled=True)
    )


def test_normal_production_never_schedules_bulletins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An enable switch without a reviewed file is insufficient."""
    settings = _settings(Path('/dev/null'))
    monkeypatch.setattr(
        'infodengue_pipeline.messaging.celery.get_settings',
        lambda: settings,
    )
    assert (
        'dispatch-ready-bulletins'
        not in create_celery_app().conf.beat_schedule
    )


def test_rehearsal_acceptance_rejects_normal_production(
    tmp_path: Path,
) -> None:
    """The validator never polls resources without authorization."""
    with pytest.raises(ValueError, match='not authorized'):
        validate_rehearsal_e2e(
            'recife/object.dbf', 'a' * 64, 30, _settings(tmp_path / 'missing')
        )
