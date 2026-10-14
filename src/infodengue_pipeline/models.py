"""Framework independent SINAN and workflow models."""

import hashlib

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import UUID

from epiweeks import Week


class Disease(StrEnum):
    """Supported diseases keyed by canonical CID-10 code."""

    DENGUE = 'A90'
    CHIKUNGUNYA = 'A92.0'
    ZIKA = 'A928'

    @property
    def canonical_cid10(self) -> str:
        """Return the canonical CID-10 code."""
        return self.value

    @property
    def storage_name(self) -> str:
        """Return the legacy canonical storage directory."""
        return _DISEASE_METADATA[self][0]

    @property
    def filename_prefix(self) -> str:
        """Return the legacy canonical filename prefix."""
        return _DISEASE_METADATA[self][1]


_DISEASE_METADATA = {
    Disease.DENGUE: ('dengue', 'DenInfodengue'),
    Disease.CHIKUNGUNYA: ('chik', 'ChikInfodengue'),
    Disease.ZIKA: ('zika', 'Infodengue'),
}


def disease_from_cid10(code: str) -> Disease:
    """Resolve a supported canonical CID-10 code."""
    try:
        return Disease(code)
    except ValueError as exc:
        supported = ', '.join(disease.value for disease in Disease)
        raise ValueError(
            f'unsupported disease CID-10 code {code!r}; expected {supported}'
        ) from exc


class Coverage(StrEnum):
    """A national delivery or one Brazilian state delivery."""

    BR = 'BR'
    AC = 'AC'
    AL = 'AL'
    AP = 'AP'
    AM = 'AM'
    BA = 'BA'
    CE = 'CE'
    DF = 'DF'
    ES = 'ES'
    GO = 'GO'
    MA = 'MA'
    MG = 'MG'
    MS = 'MS'
    MT = 'MT'
    PA = 'PA'
    PB = 'PB'
    PE = 'PE'
    PI = 'PI'
    PR = 'PR'
    RJ = 'RJ'
    RN = 'RN'
    RO = 'RO'
    RR = 'RR'
    RS = 'RS'
    SC = 'SC'
    SE = 'SE'
    SP = 'SP'
    TO = 'TO'

    @property
    def is_national(self) -> bool:
        """Whether this coverage represents the national delivery."""
        return self is Coverage.BR


class FileFormat(StrEnum):
    """Supported canonical SINAN source formats."""

    CSV = 'csv'
    DBF = 'dbf'


@dataclass(frozen=True, order=True)
class Epiweek:
    """A CDC epidemiological week serialized as ``YYYYWW``."""

    year: int
    week: int

    def __post_init__(self) -> None:
        """Reject impossible weeks, including invalid year boundaries."""
        if not 1900 <= self.year <= 9999 or not 1 <= self.week <= 53:
            raise ValueError('Invalid epidemiological week')
        week = Week(self.year, self.week)
        if Week.fromdate(week.startdate()) != week:
            raise ValueError('Invalid epidemiological week for year')

    @classmethod
    def parse(cls, value: int | str) -> 'Epiweek':
        """Parse and validate a six digit epiweek."""
        raw = str(value)
        if len(raw) != 6 or not raw.isascii() or not raw.isdigit():
            raise ValueError('Epiweek must use YYYYWW')
        return cls(int(raw[:4]), int(raw[4:]))

    def __int__(self) -> int:
        """Return the stable six digit integer representation."""
        return self.year * 100 + self.week

    def __str__(self) -> str:
        """Return the stable six digit string representation."""
        return f'{self.year:04d}{self.week:02d}'


@dataclass(frozen=True)
class SinanMetadata:
    """Normalized content inspection result; source_name is provenance only."""

    disease: Disease
    coverage: Coverage
    epiweek: Epiweek
    file_format: FileFormat
    source_name: str

    def __post_init__(self) -> None:
        """Require normalized enum values and a textual provenance name."""
        if not isinstance(self.disease, Disease):
            raise ValueError('disease must be a supported Disease')
        if not isinstance(self.coverage, Coverage):
            raise ValueError('coverage must be a supported Coverage')
        if not isinstance(self.epiweek, Epiweek):
            raise ValueError('epiweek must be a validated Epiweek')
        if not isinstance(self.file_format, FileFormat):
            raise ValueError('file_format must be CSV or DBF')
        if not isinstance(self.source_name, str):
            raise ValueError('source_name must be text')


class WorkflowStatus(StrEnum):
    """Pipeline owned analysis workflow states."""

    WAITING = 'waiting'
    READY = 'ready'
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


DEFAULT_BR_REQUIRED_DISEASES = frozenset({Disease.DENGUE, Disease.CHIKUNGUNYA})
DEFAULT_UF_REQUIRED_DISEASES = frozenset({Disease.DENGUE})


@dataclass(frozen=True)
class IngestionInput:
    """Immutable successful ingestion identity for one disease."""

    run_id: UUID
    fingerprint: str
    completed_at: datetime

    def __post_init__(self) -> None:
        """Require an aware timestamp for deterministic input ordering."""
        if (
            self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
        ):
            raise ValueError('completed_at must be timezone aware')


@dataclass(frozen=True)
class Readiness:
    """Inputs seen for one coverage and epiweek."""

    coverage: Coverage
    epiweek: Epiweek
    inputs: Mapping[Disease, IngestionInput] = field(default_factory=dict)
    required_diseases: frozenset[Disease] | None = None

    def __post_init__(self) -> None:
        """Copy inputs and validate explicit requirements."""
        required_diseases = self.required_diseases
        if required_diseases is None:
            required_diseases = (
                DEFAULT_BR_REQUIRED_DISEASES
                if self.coverage.is_national
                else DEFAULT_UF_REQUIRED_DISEASES
            )
        if not required_diseases:
            raise ValueError('required_diseases must not be empty')
        if any(not isinstance(d, Disease) for d in self.inputs):
            raise ValueError(
                'inputs must be keyed by supported Disease values'
            )
        if any(not isinstance(d, Disease) for d in required_diseases):
            raise ValueError(
                'required_diseases must contain supported Disease values'
            )
        object.__setattr__(self, 'inputs', MappingProxyType(dict(self.inputs)))
        object.__setattr__(
            self, 'required_diseases', frozenset(required_diseases)
        )

    def with_input(
        self, disease: Disease, incoming: IngestionInput
    ) -> 'Readiness':
        """Keep the newest successful input for a supported disease."""
        existing = self.inputs.get(disease)
        if existing is not None and (
            incoming.completed_at,
            incoming.run_id.int,
        ) <= (existing.completed_at, existing.run_id.int):
            return self
        updated = dict(self.inputs)
        updated[disease] = incoming
        if self.required_diseases is None:
            raise RuntimeError('readiness requirements were not initialized')
        return Readiness(
            self.coverage, self.epiweek, updated, self.required_diseases
        )

    def with_required_diseases(
        self, diseases: frozenset[Disease]
    ) -> 'Readiness':
        """Return readiness evaluated against the configured requirements."""
        return Readiness(self.coverage, self.epiweek, self.inputs, diseases)

    @property
    def received_diseases(self) -> frozenset[Disease]:
        """Return diseases with a persisted ingestion input."""
        return frozenset(self.inputs)

    @property
    def ready(self) -> bool:
        """Whether all required disease inputs are present."""
        if self.required_diseases is None:
            raise RuntimeError('readiness requirements were not initialized')
        return self.required_diseases <= self.received_diseases

    @property
    def input_fingerprint(self) -> str | None:
        """Hash coverage and all inputs in stable disease order when ready."""
        if not self.ready:
            return None
        parts = [self.coverage.value, str(self.epiweek)]
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
