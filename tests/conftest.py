"""Fixtures for isolated pipeline state tests."""

from collections.abc import Iterator
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from infodengue_pipeline.adapters.postgres.models import Base
from infodengue_pipeline.adapters.postgres.unit_of_work import SqlUnitOfWork
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.events import IngestionCompleted
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


@pytest.fixture
def sessions() -> Iterator[sessionmaker[Session]]:
    """Use an isolated SQLite database with the pipeline schema translated."""
    engine = create_engine(
        'sqlite+pysqlite:///:memory:',
        execution_options={'schema_translate_map': {'pipeline': None}},
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(engine, expire_on_commit=False)
    engine.dispose()


@pytest.fixture
def work_factory(sessions: sessionmaker[Session]):
    """Create a fresh unit of work per application operation."""
    return lambda: SqlUnitOfWork(sessions)


@pytest.fixture
def completed_event():
    """Create valid dengue ingestion events for one epiweek."""

    def build(
        disease: Disease = Disease.DENGUE,
        week: int = 202639,
        fingerprint: str = 'source-sha256',
    ) -> IngestionCompleted:
        return IngestionCompleted(
            event_id=uuid4(),
            run_id=uuid4(),
            disease=disease,
            epiweek=week,
            fingerprint=fingerprint,
            completed_at=datetime.now(timezone.utc),
        )

    return build
