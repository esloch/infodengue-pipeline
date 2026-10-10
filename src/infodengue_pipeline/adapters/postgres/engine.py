"""Database engine and unit-of-work composition."""

from functools import lru_cache

from infodengue_pipeline.adapters.postgres.unit_of_work import SqlUnitOfWork
from infodengue_pipeline.config import get_settings
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


@lru_cache(maxsize=1)
def session_factory() -> sessionmaker[Session]:
    """Create one connection pool per worker process."""
    settings = get_settings()
    engine: Engine = create_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
    )
    return sessionmaker(engine, expire_on_commit=False)


def unit_of_work() -> SqlUnitOfWork:
    """Construct one transaction boundary for an application operation."""
    return SqlUnitOfWork(session_factory())
