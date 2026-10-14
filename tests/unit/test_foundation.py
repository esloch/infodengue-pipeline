"""Small infrastructure boundaries used by the foundation."""

from pathlib import Path
from uuid import uuid4

import pytest

from infodengue_pipeline.analysis.runner import (
    AnalysisDisabledError,
    DisabledAnalysisRunner,
)
from infodengue_pipeline.db import engine as db_engine
from infodengue_pipeline.logging import configure_logging
from infodengue_pipeline.models import Coverage, Epiweek
from infodengue_pipeline.protocols import AnalysisJobRequest
from sqlalchemy import text


def test_disabled_analysis_runner_never_executes() -> None:
    """The Phase 0 runner rejects requests clearly."""
    request = AnalysisJobRequest(
        uuid4(), Coverage.BR, Epiweek.parse(202639), 'hash'
    )
    with pytest.raises(AnalysisDisabledError, match=str(request.job_id)):
        DisabledAnalysisRunner().run(request)


def test_session_factory_creates_and_caches_engine(
    monkeypatch, tmp_path: Path
) -> None:
    """Database sessions use configured pipeline DB and cache the pool."""
    from infodengue_pipeline.config import Settings

    settings = Settings(
        _env_file=None, database_url=f'sqlite:///{tmp_path / "pipeline.db"}'
    )
    monkeypatch.setattr(db_engine, 'get_settings', lambda: settings)
    db_engine.session_factory.cache_clear()
    first = db_engine.session_factory()
    assert first is db_engine.session_factory()
    with first() as session:
        assert session.scalar(text('select 1')) == 1
    db_engine.session_factory.cache_clear()


def test_structured_logging_configuration() -> None:
    """Logging configuration accepts the configured level."""
    configure_logging('INFO')
