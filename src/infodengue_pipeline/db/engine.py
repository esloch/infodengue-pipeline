"""Database engine and unit-of-work composition."""

from functools import lru_cache
from types import TracebackType
from typing import Self

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.repositories import (
    SqlOutboxRepository,
    SqlWorkflowRepository,
)
from infodengue_pipeline.protocols import (
    OutboxRepository,
    WorkflowRepository,
)


class SqlUnitOfWork:
    """Own one SQLAlchemy session and its pipeline repositories."""

    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self._sessions = sessions
        self._session: Session | None = None
        self.workflows: WorkflowRepository
        self.outbox: OutboxRepository
        self._committed = False

    def __enter__(self) -> Self:
        """Open the transaction and repositories."""
        self._session = self._sessions()
        self.workflows = SqlWorkflowRepository(self._session)
        self.outbox = SqlOutboxRepository(self._session)
        return self

    def commit(self) -> None:
        """Commit application changes once."""
        if self._session is None:
            raise RuntimeError('Unit of work is not active')
        self._session.commit()
        self._committed = True

    def rollback(self) -> None:
        """Discard uncommitted changes."""
        if self._session is not None:
            self._session.rollback()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the session and roll back incomplete transactions."""
        if self._session is None:
            return
        try:
            if exc_type is not None or not self._committed:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None


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
