"""Completion reads use the source role and never the analysis role."""

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.workers import tasks


def test_completion_bridge_uses_only_source_database(monkeypatch) -> None:
    """The configured read-only source URL constructs the bridge source."""
    source_url = 'postgresql://source_user:secret@source-db/alerta'
    analysis_url = 'postgresql://analysis_user:secret@analysis-db/alerta'
    settings = Settings(
        _env_file=None,
        alerta_source_database_url=source_url,
        analysis_database_url=analysis_url,
    )
    marker = object()
    seen: dict[str, object] = {}

    class Bridge:
        def __init__(self, sessions, source):
            seen['sessions'] = sessions
            seen['source'] = source

        def poll(self, limit):
            seen['limit'] = limit
            return 2

    def source_factory(url):
        seen['source_url'] = url
        return marker

    monkeypatch.setattr(tasks, 'get_settings', lambda: settings)
    monkeypatch.setattr(tasks, 'session_factory', lambda: marker)
    monkeypatch.setattr(
        tasks,
        '_completed_source',
        source_factory,
    )
    monkeypatch.setattr(tasks, 'IngestionCompletionBridge', Bridge)
    assert tasks.bridge_completed.run(limit=7) == 2
    assert seen['source_url'] == source_url
    assert seen['sessions'] is marker
    assert seen['source'] is marker
    assert seen['limit'] == 7


def test_analysis_only_settings_do_not_create_completion_source(
    monkeypatch,
) -> None:
    """An analysis-only worker has no completion-source database access."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql://analysis:secret@db/alerta',
    )
    monkeypatch.setattr(tasks, 'get_settings', lambda: settings)
    monkeypatch.setattr(
        tasks,
        '_completed_source',
        lambda _url: pytest.fail('completion source unexpectedly created'),
    )
    assert tasks.bridge_completed.run() == 0
