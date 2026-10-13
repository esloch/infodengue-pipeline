"""Epiweek readiness and analysis input identity."""

import hashlib

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek

DEFAULT_REQUIRED_DISEASES = frozenset({Disease.DENGUE, Disease.CHIKUNGUNYA})


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
    """Inputs seen for an epiweek and diseases required for analysis."""

    epiweek: Epiweek
    inputs: Mapping[Disease, IngestionInput] = field(default_factory=dict)
    required_diseases: frozenset[Disease] = DEFAULT_REQUIRED_DISEASES

    def __post_init__(self) -> None:
        """Copy inputs and validate that requirements are explicit."""
        if not self.required_diseases:
            raise ValueError('required_diseases must not be empty')
        if any(not isinstance(disease, Disease) for disease in self.inputs):
            raise ValueError(
                'inputs must be keyed by supported Disease values'
            )
        if any(
            not isinstance(disease, Disease)
            for disease in self.required_diseases
        ):
            raise ValueError(
                'required_diseases must contain supported Disease values'
            )
        object.__setattr__(self, 'inputs', MappingProxyType(dict(self.inputs)))
        object.__setattr__(
            self, 'required_diseases', frozenset(self.required_diseases)
        )

    def with_input(
        self, disease: Disease, incoming: IngestionInput
    ) -> 'Readiness':
        """Record the latest successful input for a supported disease."""
        if not isinstance(disease, Disease):
            raise ValueError(f'unsupported disease: {disease!r}')
        updated_inputs = dict(self.inputs)
        updated_inputs[disease] = incoming
        return Readiness(self.epiweek, updated_inputs, self.required_diseases)

    def with_required_diseases(
        self, diseases: frozenset[Disease]
    ) -> 'Readiness':
        """Return readiness evaluated against the configured requirements."""
        return Readiness(self.epiweek, self.inputs, diseases)

    @property
    def received_diseases(self) -> frozenset[Disease]:
        """Return the diseases with a persisted ingestion input."""
        return frozenset(self.inputs)

    @property
    def ready(self) -> bool:
        """Report whether all explicitly required diseases are present."""
        return self.required_diseases <= self.received_diseases

    @property
    def input_fingerprint(self) -> str | None:
        """Hash all available analysis inputs in stable CID-10 order."""
        if not self.ready:
            return None
        parts = [str(self.epiweek)]
        for disease, incoming in sorted(
            self.inputs.items(), key=lambda item: item[0].canonical_cid10
        ):
            parts.extend(
                (
                    disease.canonical_cid10,
                    str(incoming.run_id),
                    incoming.fingerprint,
                )
            )
        return hashlib.sha256('\x00'.join(parts).encode()).hexdigest()
