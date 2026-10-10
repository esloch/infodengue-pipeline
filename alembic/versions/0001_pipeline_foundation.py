"""Create pipeline owned workflow and outbox state.

Revision ID: 0001_pipeline_foundation
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = '0001_pipeline_foundation'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create only new tables under the pipeline schema."""
    op.execute('CREATE SCHEMA IF NOT EXISTS pipeline')
    op.create_table(
        'workflow',
        sa.Column('workflow_id', sa.Uuid(), primary_key=True),
        sa.Column('epiweek', sa.Integer(), nullable=False),
        sa.Column('dengue_run_id', sa.Uuid()),
        sa.Column('dengue_fingerprint', sa.String(128)),
        sa.Column('chikungunya_run_id', sa.Uuid()),
        sa.Column('chikungunya_fingerprint', sa.String(128)),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('queued_fingerprint', sa.String(64)),
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
        sa.UniqueConstraint('epiweek', name='uq_workflow_epiweek'),
        schema='pipeline',
    )
    op.create_index(
        'ix_pipeline_workflow_epiweek',
        'workflow',
        ['epiweek'],
        schema='pipeline',
    )
    op.create_table(
        'outbox_event',
        sa.Column('event_id', sa.Uuid(), primary_key=True),
        sa.Column('event_type', sa.String(128), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('payload', postgresql.JSONB(), nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('published_at', sa.DateTime(timezone=True)),
        schema='pipeline',
    )
    op.create_index(
        'ix_pipeline_outbox_status_created',
        'outbox_event',
        ['status', 'created_at'],
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove only pipeline foundation tables from a development DB."""
    op.drop_index(
        'ix_pipeline_outbox_status_created',
        table_name='outbox_event',
        schema='pipeline',
    )
    op.drop_table('outbox_event', schema='pipeline')
    op.drop_index(
        'ix_pipeline_workflow_epiweek',
        table_name='workflow',
        schema='pipeline',
    )
    op.drop_table('workflow', schema='pipeline')
    op.execute('DROP SCHEMA pipeline')
