"""Versioned ingestion event contract."""

from datetime import datetime
from typing import Literal, Self
from uuid import NAMESPACE_OID, UUID, uuid5

from pydantic import BaseModel, ConfigDict, field_validator

from infodengue_pipeline.models import Coverage, Disease, Epiweek


class IngestionCompleted(BaseModel):
    """A successful SINAN merge for one disease and epiweek."""

    model_config = ConfigDict(frozen=True)

    event_type: Literal['sinan.ingestion.completed'] = (
        'sinan.ingestion.completed'
    )
    version: Literal[1] = 1
    event_id: UUID
    run_id: UUID
    disease: Disease
    coverage: Coverage
    epiweek: int
    fingerprint: str
    completed_at: datetime

    @field_validator('epiweek')
    @classmethod
    def validate_epiweek(cls, value: int) -> int:
        """Ensure event payloads cannot carry invalid epidemiological weeks."""
        return int(Epiweek.parse(value))

    @field_validator('fingerprint')
    @classmethod
    def validate_fingerprint(cls, value: str) -> str:
        """Require an input identity that fits the persisted column."""
        if not value.strip() or len(value) > 128:
            raise ValueError(
                'fingerprint must contain 1 to 128 nonblank characters'
            )
        return value

    @field_validator('completed_at')
    @classmethod
    def validate_completed_at(cls, value: datetime) -> datetime:
        """Require an aware completion timestamp."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('completed_at must be timezone aware')
        return value

    @classmethod
    def new(
        cls,
        *,
        run_id: UUID,
        disease: Disease,
        coverage: Coverage,
        epiweek: Epiweek,
        fingerprint: str,
        completed_at: datetime,
    ) -> Self:
        """Create a stable event for a completed ingestion run."""
        return cls(
            event_id=uuid5(
                NAMESPACE_OID,
                f'infodengue:sinan.ingestion.completed:v1:{run_id}',
            ),
            run_id=run_id,
            disease=disease,
            coverage=coverage,
            epiweek=int(epiweek),
            fingerprint=fingerprint,
            completed_at=completed_at,
        )
