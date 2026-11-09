"""Persist validated historical ingestion reconciliation.

Revision ID: 0003_historical_ingress_reconciliation
Revises: 0002_ingress_analysis_jobs
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '0003_historical_ingress_reconciliation'
down_revision: str | None = '0002_ingress_analysis_jobs'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add explicit reconciliation state and validated upstream metadata."""
    op.add_column(
        'ingress_object',
        sa.Column('canonical_cid10', sa.String(8)),
        schema='pipeline',
    )
    op.add_column(
        'ingress_object',
        sa.Column('delivery_se', sa.Integer()),
        schema='pipeline',
    )
    op.add_column(
        'ingress_object',
        sa.Column('run_finished_at', sa.DateTime(timezone=True)),
        schema='pipeline',
    )
    op.add_column(
        'ingress_object',
        sa.Column('reconciled_at', sa.DateTime(timezone=True)),
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove the historical reconciliation evidence columns."""
    op.drop_column('ingress_object', 'reconciled_at', schema='pipeline')
    op.drop_column('ingress_object', 'run_finished_at', schema='pipeline')
    op.drop_column('ingress_object', 'delivery_se', schema='pipeline')
    op.drop_column('ingress_object', 'canonical_cid10', schema='pipeline')
