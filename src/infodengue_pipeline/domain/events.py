"""Versioned ingestion event contract."""

from datetime import datetime, timezone
from typing import Literal, Self
from uuid import UUID, uuid4

from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek
from pydantic import BaseModel, ConfigDict, field_validator


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
        """Require a nonempty immutable input identity."""
        if not value.strip():
            raise ValueError('fingerprint must not be empty')
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
        epiweek: Epiweek,
        fingerprint: str,
    ) -> Self:
        """Create a new event at the successful transaction boundary."""
        return cls(
            event_id=uuid4(),
            run_id=run_id,
            disease=disease,
            epiweek=int(epiweek),
            fingerprint=fingerprint,
            completed_at=datetime.now(timezone.utc),
        )
