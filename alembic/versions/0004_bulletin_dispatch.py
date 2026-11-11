"""Track bulletin broker publication leases.

Revision ID: 0004_bulletin_dispatch
Revises: 0003_bulletin_jobs
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '0004_bulletin_dispatch'
down_revision: str | None = '0003_bulletin_jobs'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Record when a queued bulletin was last published."""
    op.add_column(
        'bulletin_job',
        sa.Column('dispatched_at', sa.DateTime(timezone=True)),
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove the bulletin dispatch lease."""
    op.drop_column('bulletin_job', 'dispatched_at', schema='pipeline')
