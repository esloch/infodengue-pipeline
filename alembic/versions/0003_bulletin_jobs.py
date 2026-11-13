"""Add durable Recife intramunicipal bulletin jobs.

Revision ID: 0003_bulletin_jobs
Revises: 0002_ingress_analysis_jobs
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '0003_bulletin_jobs'
down_revision: str | None = '0002_ingress_analysis_jobs'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create only the pipeline-owned bulletin job table."""
    op.create_table(
        'bulletin_job',
        sa.Column('job_id', sa.Uuid(), primary_key=True),
        sa.Column('kind', sa.String(32), nullable=False),
        sa.Column('municipality_geocode', sa.Integer(), nullable=False),
        sa.Column(
            'analysis_job_id',
            sa.Uuid(),
            sa.ForeignKey('pipeline.analysis_job.job_id'),
            nullable=False,
        ),
        sa.Column('renderer_revision', sa.String(128), nullable=False),
        sa.Column(
            'status',
            sa.String(16),
            server_default='queued',
            nullable=False,
        ),
        sa.Column(
            'attempts', sa.Integer(), server_default='0', nullable=False
        ),
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
        sa.Column('started_at', sa.DateTime(timezone=True)),
        sa.Column('finished_at', sa.DateTime(timezone=True)),
        sa.Column('artifact_path', sa.String(2048)),
        sa.Column('exit_code', sa.Integer()),
        sa.Column('last_error', sa.String(512)),
        sa.UniqueConstraint(
            'kind',
            'municipality_geocode',
            'analysis_job_id',
            'renderer_revision',
            name='uq_bulletin_job_identity',
        ),
        sa.CheckConstraint(
            'municipality_geocode = 2611606',
            name='ck_bulletin_job_recife_geocode',
        ),
        sa.CheckConstraint(
            "kind = 'recife_intramunicipal'",
            name='ck_bulletin_job_kind',
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name='ck_bulletin_job_status',
        ),
        sa.CheckConstraint('attempts >= 0', name='ck_bulletin_job_attempts'),
        schema='pipeline',
    )
    op.create_index(
        'ix_pipeline_bulletin_job_status_created',
        'bulletin_job',
        ['status', 'created_at'],
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove only bulletin job persistence."""
    op.drop_index(
        'ix_pipeline_bulletin_job_status_created',
        table_name='bulletin_job',
        schema='pipeline',
    )
    op.drop_table('bulletin_job', schema='pipeline')
