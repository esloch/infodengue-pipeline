"""SQLAlchemy transaction boundary for application services."""

from types import TracebackType
from typing import Self

from infodengue_pipeline.adapters.postgres.repositories import (
    SqlOutboxRepository,
    SqlWorkflowRepository,
)
from infodengue_pipeline.ports.contracts import (
    OutboxRepository,
    WorkflowRepository,
)
from sqlalchemy.orm import Session, sessionmaker


class SqlUnitOfWork:
    """Own one SQLAlchemy session and transaction."""

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
