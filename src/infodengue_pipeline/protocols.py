"""Framework independent boundaries for infrastructure adapters."""

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Protocol, Self
from uuid import UUID

from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobResult,
    Coverage,
    Epiweek,
    ObjectMetadata,
    WorkflowSnapshot,
)


class ObjectStore(Protocol):
    """Transient object ingress operations."""

    def bucket_exists(self) -> bool:
        """Report whether the configured development bucket exists."""
        ...

    def stat(self, _key: str) -> ObjectMetadata:
        """Fetch metadata for an ingress object."""
        ...

    def list(self, _prefix: str = '') -> list[ObjectMetadata]:
        """List objects under a prefix."""
        ...

    def materialize(
        self,
        _key: str,
        _destination: Path,
        _version_id: str | None = None,
    ) -> ObjectMetadata:
        """Download an ingress object to a local path."""
        ...


class EventPublisher(Protocol):
    """Confirmed domain event publication."""

    def publish(self, event: IngestionCompleted) -> None:
        """Return only after the broker confirms publication."""
        ...


class WorkflowRepository(Protocol):
    """Transactional workflow state persistence."""

    def get_for_update(
        self, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Lock and load one coverage and epiweek state."""
        ...

    def save(self, workflow: WorkflowSnapshot) -> None:
        """Insert or update one coverage and epiweek state."""
        ...


class OutboxRepository(Protocol):
    """Transactional event outbox persistence."""

    def add(self, event: IngestionCompleted) -> bool:
        """Insert once by event identity; return whether inserted."""
        ...

    def pending(self, limit: int) -> list[IngestionCompleted]:
        """Lock pending events for one publisher batch."""
        ...

    def mark_published(self, event_id: UUID) -> None:
        """Mark an event published after broker confirmation."""
        ...


class UnitOfWork(Protocol, AbstractContextManager['UnitOfWork']):
    """One database transaction with workflow and outbox repositories."""

    workflows: WorkflowRepository
    outbox: OutboxRepository

    def __enter__(self) -> Self:
        """Begin or join a transaction."""
        ...

    def commit(self) -> None:
        """Persist all changes atomically."""
        ...

    def rollback(self) -> None:
        """Discard uncommitted changes."""
        ...


class AnalysisRunner(Protocol):
    """Run AlertaDengueAnalise through its finite job contract."""

    def run(self, _request: AnalysisJobRequest) -> AnalysisJobResult:
        """Execute one finite analysis job."""
        ...
