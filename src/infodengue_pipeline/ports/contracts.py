"""Framework independent boundaries for infrastructure adapters."""

from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self
from uuid import UUID

from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.events import IngestionCompleted
from infodengue_pipeline.domain.workflow import (
    Readiness,
    WorkflowStatus,
)


@dataclass(frozen=True)
class ObjectMetadata:
    """Object metadata without a MinIO SDK dependency."""

    key: str
    size: int
    etag: str


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

    def destination(
        self,
        *,
        country: str,
        file_type: str,
        disease: Disease,
        epiweek: Epiweek,
        filename: str,
    ) -> Path:
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

    def get_for_update(self, epiweek: Epiweek) -> WorkflowSnapshot | None:
        """Lock and load one epiweek state."""

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
    epiweek: Epiweek
    input_fingerprint: str
    states: tuple[str, ...] = ('ALL',)
    cores: int = 1
    load: bool = True

    def __post_init__(self) -> None:
        """Reject unsupported or unsafe job arguments."""
        if not self.input_fingerprint:
            raise ValueError('input_fingerprint must not be empty')
        if self.cores < 1 or self.cores > 64:
            raise ValueError('cores must be between 1 and 64')
        if not self.states or any(
            state != 'ALL'
            and (len(state) != 2 or not state.isalpha() or not state.isupper())
            for state in self.states
        ):
            raise ValueError('states must be ALL or uppercase UF codes')
        if 'ALL' in self.states and len(self.states) != 1:
            raise ValueError('ALL cannot be combined with UF codes')


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

    def publish(self, result: AnalysisJobResult) -> None:
        """Publish artifacts after validation."""


class RuntimeRefresher(Protocol):
    """Invoke AlertaDengue's stable runtime refresh contract."""

    def refresh(self, epiweek: Epiweek) -> None:
        """Refresh the public runtime after artifact publication."""
