"""Development-only bulletin task scheduling and broker recovery."""

from pathlib import Path
from uuid import uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.messaging.celery import create_celery_app
from infodengue_pipeline.workers import tasks


def test_bulletin_beat_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ordinary worker configuration never schedules bulletins."""
    monkeypatch.setattr(
        'infodengue_pipeline.messaging.celery.get_settings',
        lambda: Settings(_env_file=None),
    )
    assert (
        'dispatch-ready-bulletins'
        not in create_celery_app().conf.beat_schedule
    )


def test_compose_shares_bulletin_opt_in() -> None:
    """Beat and the dedicated worker read the same explicit switch."""
    compose = Path('containers/compose.yaml').read_text()
    switch = (
        'INFODENGUE_PIPELINE_BULLETIN_ENABLED: '
        '${INFODENGUE_PIPELINE_BULLETIN_ENABLED:-false}'
    )
    assert compose.count(switch) == 2


def test_broker_failure_leaves_queued_job_for_later_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed publish does not mark the persisted job terminal."""
    job_id = uuid4()
    calls: list[str] = []

    class Jobs:
        def fail_interrupted(self, _seconds: int) -> int:
            return 0

        def dispatch_candidates(self, _limit: int) -> list[object]:
            return [job_id]

        def queued_candidates(self, _limit: int) -> list[object]:
            return [job_id]

        def claim_dispatch(self, _job_id: object, _before: object) -> bool:
            calls.append('claim')
            return True

    monkeypatch.setattr(
        tasks,
        'get_settings',
        lambda: Settings(bulletin_enabled=True, _env_file=None),
    )
    monkeypatch.setattr(tasks, '_bulletin_jobs', lambda _settings: Jobs())

    def fail(*_args: object, **_kwargs: object) -> None:
        raise ConnectionError('broker unavailable')

    monkeypatch.setattr(tasks.celery_app, 'send_task', fail)
    with pytest.raises(ConnectionError, match='broker unavailable'):
        tasks.dispatch_bulletins.run()
    assert calls == ['claim']
