"""Record independently published scientific artifacts.

Revision ID: 0005_analysis_publication
Revises: 0004_bulletin_dispatch
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '0005_analysis_publication'
down_revision: str | None = '0004_bulletin_dispatch'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable, auditable publication evidence to analysis jobs."""
    op.add_column(
        'analysis_job',
        sa.Column('published_at', sa.DateTime(timezone=True)),
        schema='pipeline',
    )
    op.add_column(
        'analysis_job',
        sa.Column('published_dengue_sha256', sa.String(64)),
        schema='pipeline',
    )
    op.add_column(
        'analysis_job',
        sa.Column('published_chik_sha256', sa.String(64)),
        schema='pipeline',
    )
    op.create_check_constraint(
        'ck_analysis_job_publication_complete',
        'analysis_job',
        '(published_at IS NULL AND '
        'published_dengue_sha256 IS NULL AND '
        'published_chik_sha256 IS NULL) OR '
        '(published_at IS NOT NULL AND '
        'published_dengue_sha256 IS NOT NULL AND '
        'published_chik_sha256 IS NOT NULL)',
        schema='pipeline',
    )


def downgrade() -> None:
    """Remove publication metadata without changing analysis job history."""
    op.drop_constraint(
        'ck_analysis_job_publication_complete',
        'analysis_job',
        schema='pipeline',
    )
    for column in (
        'published_chik_sha256',
        'published_dengue_sha256',
        'published_at',
    ):
        op.drop_column('analysis_job', column, schema='pipeline')
