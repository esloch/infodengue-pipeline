"""Epiweek readiness and analysis input identity."""

import hashlib

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek


class WorkflowStatus(StrEnum):
    """Pipeline owned analysis workflow states."""

    WAITING = 'waiting'
    READY = 'ready'
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


@dataclass(frozen=True)
class IngestionInput:
    """Immutable successful ingestion identity for one disease."""

    run_id: UUID
    fingerprint: str


@dataclass(frozen=True)
class Readiness:
    """Inputs seen for one epiweek."""

    epiweek: Epiweek
    dengue: IngestionInput | None = None
    chikungunya: IngestionInput | None = None

    def with_input(
        self, disease: Disease, incoming: IngestionInput
    ) -> 'Readiness':
        """Record the latest successful input for a disease."""
        if disease is Disease.DENGUE:
            return Readiness(self.epiweek, incoming, self.chikungunya)
        return Readiness(self.epiweek, self.dengue, incoming)

    @property
    def ready(self) -> bool:
        """Report whether both diseases exist for the same epiweek."""
        return self.dengue is not None and self.chikungunya is not None

    @property
    def input_fingerprint(self) -> str | None:
        """Hash stable input identities without timestamps."""
        if self.dengue is None or self.chikungunya is None:
            return None
        parts = (
            str(self.epiweek),
            str(self.dengue.run_id),
            self.dengue.fingerprint,
            str(self.chikungunya.run_id),
            self.chikungunya.fingerprint,
        )
        return hashlib.sha256('\x00'.join(parts).encode()).hexdigest()
