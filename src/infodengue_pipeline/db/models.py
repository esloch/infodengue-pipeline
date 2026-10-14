"""SQLAlchemy mappings for pipeline owned state."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)

from infodengue_pipeline.models import Coverage, Disease, WorkflowStatus


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
        UniqueConstraint(
            'coverage', 'epiweek', name='uq_workflow_coverage_epiweek'
        ),
        {'schema': 'pipeline'},
    )

    workflow_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    coverage: Mapped[Coverage] = mapped_column(
        Enum(
            Coverage, values_callable=enum_values, native_enum=False, length=2
        ),
        nullable=False,
    )
    epiweek: Mapped[int] = mapped_column(Integer, nullable=False)
    inputs: Mapped[list[WorkflowInputRow]] = relationship(
        back_populates='workflow',
        cascade='all, delete-orphan',
        lazy='selectin',
    )
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


class WorkflowInputRow(Base):
    """One persisted ingestion identity for a workflow disease."""

    __tablename__ = 'workflow_input'
    __table_args__ = (
        UniqueConstraint(
            'workflow_id', 'disease', name='uq_workflow_input_disease'
        ),
        {'schema': 'pipeline'},
    )

    workflow_input_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid4
    )
    workflow_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey('pipeline.workflow.workflow_id', ondelete='CASCADE'),
        nullable=False,
    )
    disease: Mapped[Disease] = mapped_column(
        Enum(
            Disease,
            values_callable=enum_values,
            native_enum=False,
            length=8,
        ),
        nullable=False,
    )
    run_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    workflow: Mapped[WorkflowRow] = relationship(back_populates='inputs')


class OutboxEventRow(Base):
    """Domain event persisted in the ingestion transaction."""

    __tablename__ = 'outbox_event'
    __table_args__ = (
        Index('ix_pipeline_outbox_status_created', 'status', 'created_at'),
        {'schema': 'pipeline'},
    )

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
