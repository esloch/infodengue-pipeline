"""SQLAlchemy mappings for pipeline owned state."""

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from infodengue_pipeline.domain.workflow import WorkflowStatus
from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for pipeline owned tables."""

    metadata = MetaData()


class OutboxStatus(StrEnum):
    """Durable publication state."""

    PENDING = 'pending'
    PUBLISHED = 'published'


def enum_values(enum_type: type[StrEnum]) -> list[str]:
    """Persist enum values rather than Python member names."""
    return [member.value for member in enum_type]


class WorkflowRow(Base):
    """Per epiweek readiness and analysis scheduling state."""

    __tablename__ = 'workflow'
    __table_args__ = (
        UniqueConstraint('epiweek', name='uq_workflow_epiweek'),
        {'schema': 'pipeline'},
    )

    workflow_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    epiweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    dengue_run_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    dengue_fingerprint: Mapped[str | None] = mapped_column(String(128))
    chikungunya_run_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    chikungunya_fingerprint: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[WorkflowStatus] = mapped_column(
        Enum(
            WorkflowStatus,
            values_callable=enum_values,
            native_enum=False,
            length=16,
        ),
        nullable=False,
        default=WorkflowStatus.WAITING,
    )
    queued_fingerprint: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class OutboxEventRow(Base):
    """Domain event persisted in the ingestion transaction."""

    __tablename__ = 'outbox_event'
    __table_args__ = ({'schema': 'pipeline'},)

    event_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(
        JSON().with_variant(JSONB, 'postgresql'), nullable=False
    )
    status: Mapped[OutboxStatus] = mapped_column(
        Enum(
            OutboxStatus,
            values_callable=enum_values,
            native_enum=False,
            length=16,
        ),
        nullable=False,
        default=OutboxStatus.PENDING,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
