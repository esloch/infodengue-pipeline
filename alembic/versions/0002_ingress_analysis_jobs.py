"""Add durable ingress objects and finite analysis jobs.

Revision ID: 0002_ingress_analysis_jobs
Revises: 0001_pipeline_foundation
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '0002_ingress_analysis_jobs'
down_revision: str | None = '0001_pipeline_foundation'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create pipeline-owned ingress and analysis execution state."""
    op.create_table(
        'ingress_object',
        sa.Column('object_id', sa.Uuid(), primary_key=True),
        sa.Column('bucket', sa.String(128), nullable=False),
        sa.Column('object_key', sa.String(1024), nullable=False),
        sa.Column('object_identity', sa.String(64), nullable=False),
        sa.Column('etag', sa.String(128), nullable=False),
        sa.Column('version_id', sa.String(256)),
        sa.Column('size', sa.BigInteger(), nullable=False),
        sa.Column('sha256', sa.String(64)),
        sa.Column('destination', sa.String(2048), nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('run_id', sa.Uuid()),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('materialized_at', sa.DateTime(timezone=True)),
        sa.Column('last_error', sa.String(512)),
        sa.UniqueConstraint(
            'object_identity', name='uq_ingress_object_identity'
        ),
        schema='pipeline',
    )
    op.create_index(
        'ix_pipeline_ingress_status_created',
        'ingress_object',
        ['status', 'created_at'],
        schema='pipeline',
    )
    op.create_table(
        'analysis_job',
        sa.Column('job_id', sa.Uuid(), primary_key=True),
        sa.Column(
            'workflow_id',
            sa.Uuid(),
            sa.ForeignKey('pipeline.workflow.workflow_id'),
            nullable=False,
        ),
        sa.Column('input_fingerprint', sa.String(64), nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('image', sa.String(512), nullable=False),
        sa.Column('cores', sa.Integer(), nullable=False),
        sa.Column('load', sa.Boolean(), nullable=False),
        sa.Column('output_dir', sa.String(2048), nullable=False),
        sa.Column(
            'attempts', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('dispatched_at', sa.DateTime(timezone=True)),
        sa.Column('started_at', sa.DateTime(timezone=True)),
        sa.Column('finished_at', sa.DateTime(timezone=True)),
        sa.Column('exit_code', sa.Integer()),
        sa.Column('last_error', sa.String(512)),
        sa.UniqueConstraint(
            'workflow_id',
            'input_fingerprint',
            name='uq_analysis_job_workflow_fingerprint',
        ),
        schema='pipeline',
    )
    op.create_index(
        'ix_pipeline_analysis_job_status_created',
        'analysis_job',
        ['status', 'created_at'],
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove only the new pipeline-owned tables."""
    op.drop_index(
        'ix_pipeline_analysis_job_status_created',
        table_name='analysis_job',
        schema='pipeline',
    )
    op.drop_table('analysis_job', schema='pipeline')
    op.drop_index(
        'ix_pipeline_ingress_status_created',
        table_name='ingress_object',
        schema='pipeline',
    )
    op.drop_table('ingress_object', schema='pipeline')
