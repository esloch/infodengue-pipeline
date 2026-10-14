"""Framework independent boundaries for infrastructure adapters."""

from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self
from uuid import UUID

from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    Coverage,
    Epiweek,
    Readiness,
    SinanMetadata,
    WorkflowStatus,
)


@dataclass(frozen=True)
class ObjectMetadata:
    """Object metadata without a MinIO SDK dependency."""

    bucket: str
    key: str
    size: int
    etag: str
    version_id: str | None = None


class ObjectStore(Protocol):
    """Transient object ingress operations."""

    def bucket_exists(self) -> bool:
        """Report whether the configured development bucket exists."""

    def stat(self, key: str) -> ObjectMetadata:
        """Fetch metadata for an ingress object."""

    def list(self, prefix: str = '') -> list[ObjectMetadata]:
        """List objects under a prefix."""

    def materialize(self, key: str, destination: Path) -> ObjectMetadata:
        """Download an ingress object to a local path."""


class CanonicalStorage(Protocol):
    """Canonical imported source storage."""

    def destination(self, metadata: SinanMetadata) -> Path:
        """Return a contained canonical destination path."""


class EventPublisher(Protocol):
    """Confirmed domain event publication."""

    def publish(self, event: IngestionCompleted) -> None:
        """Return only after the broker confirms publication."""


@dataclass(frozen=True)
class WorkflowSnapshot:
    """Persisted coordination state for one epiweek."""

    workflow_id: UUID
    readiness: Readiness
    status: WorkflowStatus
    queued_fingerprint: str | None = None


class WorkflowRepository(Protocol):
    """Transactional workflow state persistence."""

    def get_for_update(
        self, coverage: Coverage, epiweek: Epiweek
    ) -> WorkflowSnapshot | None:
        """Lock and load one coverage and epiweek state."""

    def save(self, workflow: WorkflowSnapshot) -> None:
        """Insert or update one epiweek state."""


class OutboxRepository(Protocol):
    """Transactional event outbox persistence."""

    def add(self, event: IngestionCompleted) -> bool:
        """Insert once by event identity; return whether inserted."""

    def pending(self, limit: int) -> list[IngestionCompleted]:
        """Lock pending events for one publisher batch."""

    def mark_published(self, event_id: UUID) -> None:
        """Mark an event published after broker confirmation."""


class UnitOfWork(Protocol, AbstractContextManager['UnitOfWork']):
    """One database transaction with workflow and outbox repositories."""

    workflows: WorkflowRepository
    outbox: OutboxRepository

    def __enter__(self) -> Self:
        """Begin or join a transaction."""

    def commit(self) -> None:
        """Persist all changes atomically."""

    def rollback(self) -> None:
        """Discard uncommitted changes."""


@dataclass(frozen=True)
class AnalysisJobRequest:
    """Validated parameters for a future finite scientific OCI job."""

    job_id: UUID
    coverage: Coverage
    epiweek: Epiweek
    input_fingerprint: str
    cores: int = 1
    load: bool = True

    def __post_init__(self) -> None:
        """Reject unsupported or unsafe job arguments."""
        if not self.input_fingerprint:
            raise ValueError('input_fingerprint must not be empty')
        if self.cores < 1 or self.cores > 64:
            raise ValueError('cores must be between 1 and 64')

    @property
    def states(self) -> tuple[str, ...]:
        """Derive the analysis scope from delivery coverage."""
        return (
            ('ALL',) if self.coverage.is_national else (self.coverage.value,)
        )


@dataclass(frozen=True)
class AnalysisJobResult:
    """Finite scientific job outcome and output location."""

    job_id: UUID
    output_dir: Path
    succeeded: bool


class AnalysisRunner(Protocol):
    """Run AlertaDengueAnalise through its external OCI contract."""

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Execute one finite analysis job."""


class ArtifactPublisher(Protocol):
    """Publish validated scientific output artifacts."""

    def publish(self, _result: AnalysisJobResult) -> None:
        """Publish artifacts after validation."""


class RuntimeRefresher(Protocol):
    """Invoke AlertaDengue's stable runtime refresh contract."""

    def refresh(self, epiweek: Epiweek) -> None:
        """Refresh the public runtime after artifact publication."""
