"""PostgreSQL guarantees for durable Recife bulletin job identity."""

import os

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from uuid import UUID, uuid4

import pytest

from infodengue_pipeline.db.engine import SqlUnitOfWork
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    BulletinJobRow,
    WorkflowRow,
)
from infodengue_pipeline.db.repositories import SqlBulletinJobRepository
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    BulletinJobStatus,
    BulletinKind,
    Coverage,
    WorkflowStatus,
)
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

KIND = BulletinKind.RECIFE_INTRAMUNICIPAL
RECIFE = 2611606
REVISION = 'fe82d1d5f6caf0a91aca585f0385cf1abea2e953+quarto-1.11.9'


@pytest.fixture(scope='module')
def postgres_sessions() -> Iterator[sessionmaker[Session]]:
    """Connect only to a migrated disposable PostgreSQL test database."""
    url = os.environ.get('INFODENGUE_PIPELINE_TEST_POSTGRES_URL')
    if not url:
        pytest.skip('disposable PostgreSQL test URL is not configured')
    database = make_url(url).database
    if not database or 'test' not in database:
        pytest.fail('bulletin persistence tests require a disposable test DB')
    engine = create_engine(url, pool_pre_ping=True)
    try:
        if engine.dialect.name != 'postgresql':
            pytest.fail('bulletin persistence tests require PostgreSQL')
        with engine.connect() as connection:
            current = connection.scalar(
                text('SELECT version_num FROM alembic_version')
            )
            if current != '0003_bulletin_jobs':
                pytest.fail(
                    'disposable PostgreSQL database must be migrated to '
                    '0003_bulletin_jobs'
                )
        yield sessionmaker(engine, expire_on_commit=False)
    finally:
        engine.dispose()


def seed_analysis(
    sessions: sessionmaker[Session],
    *,
    week: int,
    fingerprint: str,
    workflow_id: UUID | None = None,
) -> tuple[UUID, UUID]:
    """Persist an analysis identity with PE coverage for repository tests."""
    job_id = uuid4()
    workflow_id = workflow_id or uuid4()
    with sessions.begin() as session:
        if session.get(WorkflowRow, workflow_id) is None:
            session.add(
                WorkflowRow(
                    workflow_id=workflow_id,
                    coverage=Coverage.PE,
                    epiweek=week,
                    status=WorkflowStatus.SUCCEEDED,
                    queued_fingerprint=fingerprint,
                )
            )
            session.flush()
        session.add(
            AnalysisJobRow(
                job_id=job_id,
                workflow_id=workflow_id,
                input_fingerprint=fingerprint,
                status=AnalysisJobStatus.SUCCEEDED,
                image='analysis:test',
                cores=1,
                load=True,
                output_dir='/tmp/disposable-analysis',
                attempts=1,
                finished_at=datetime.now(timezone.utc),
                exit_code=0,
            )
        )
    return job_id, workflow_id


def create(
    session: Session,
    analysis_job_id: UUID,
    revision: str = REVISION,
) -> BulletinJobRow:
    """Create or retrieve one identity inside the caller's transaction."""
    return SqlBulletinJobRepository(session).get_or_create(
        kind=KIND,
        municipality_geocode=RECIFE,
        analysis_job_id=analysis_job_id,
        renderer_revision=revision,
    )


def test_creation_defaults_retrieval_and_duplicate_reuse(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """A committed identity has one queued row and stable retrieval."""
    analysis_job_id, _ = seed_analysis(
        postgres_sessions, week=209901, fingerprint='a' * 64
    )
    with SqlUnitOfWork(postgres_sessions) as work:
        created = work.bulletin_jobs.get_or_create(
            kind=KIND,
            municipality_geocode=RECIFE,
            analysis_job_id=analysis_job_id,
            renderer_revision=REVISION,
        )
        created_id = created.job_id
        work.commit()

    with postgres_sessions.begin() as session:
        repository = SqlBulletinJobRepository(session)
        row = repository.get(created_id)
        assert row is not None
        assert row.kind is KIND
        assert row.municipality_geocode == RECIFE
        assert row.analysis_job_id == analysis_job_id
        assert row.renderer_revision == REVISION
        assert row.status is BulletinJobStatus.QUEUED
        assert row.attempts == 0
        assert row.created_at is not None and row.updated_at is not None
        assert row.started_at is None and row.finished_at is None
        assert row.artifact_path is None
        assert row.exit_code is None and row.last_error is None
        assert create(session, analysis_job_id).job_id == created_id
        assert (
            repository.get_by_identity(
                kind=KIND,
                municipality_geocode=RECIFE,
                analysis_job_id=analysis_job_id,
                renderer_revision=REVISION,
            ).job_id
            == created_id
        )
        count = session.scalar(
            select(func.count())
            .select_from(BulletinJobRow)
            .where(BulletinJobRow.analysis_job_id == analysis_job_id)
        )
        assert count == 1


def test_postgres_unique_identity_rejects_direct_duplicate(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """The database constraint protects identity outside the repository."""
    analysis_job_id, _ = seed_analysis(
        postgres_sessions, week=209902, fingerprint='b' * 64
    )
    with postgres_sessions.begin() as session:
        create(session, analysis_job_id)
    with postgres_sessions() as session:
        session.add(
            BulletinJobRow(
                job_id=uuid4(),
                kind=KIND,
                municipality_geocode=RECIFE,
                analysis_job_id=analysis_job_id,
                renderer_revision=REVISION,
                status=BulletinJobStatus.QUEUED,
                attempts=0,
            )
        )
        with pytest.raises(IntegrityError) as error:
            session.commit()
        assert error.value.orig.sqlstate == '23505'
        assert (
            error.value.orig.diag.constraint_name == 'uq_bulletin_job_identity'
        )
        session.rollback()


def test_new_fingerprint_and_renderer_revision_create_distinct_jobs(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """A new analysis identity or immutable renderer revision gets a row."""
    first_analysis, workflow_id = seed_analysis(
        postgres_sessions, week=209903, fingerprint='c' * 64
    )
    second_analysis, _ = seed_analysis(
        postgres_sessions,
        week=209903,
        fingerprint='d' * 64,
        workflow_id=workflow_id,
    )
    with postgres_sessions.begin() as session:
        first = create(session, first_analysis)
        changed_input = create(session, second_analysis)
        changed_renderer = create(session, first_analysis, 'renderer-next')
        assert (
            len(
                {
                    first.job_id,
                    changed_input.job_id,
                    changed_renderer.job_id,
                }
            )
            == 3
        )


def test_rollback_discards_uncommitted_bulletin_job(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """The existing Unit of Work rolls back a job without commit."""
    analysis_job_id, _ = seed_analysis(
        postgres_sessions, week=209904, fingerprint='e' * 64
    )
    with SqlUnitOfWork(postgres_sessions) as work:
        created_id = work.bulletin_jobs.get_or_create(
            kind=KIND,
            municipality_geocode=RECIFE,
            analysis_job_id=analysis_job_id,
            renderer_revision=REVISION,
        ).job_id
    with postgres_sessions() as session:
        assert SqlBulletinJobRepository(session).get(created_id) is None


def test_concurrent_duplicate_creation_returns_one_job(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """ON CONFLICT and the unique constraint converge concurrent callers."""
    analysis_job_id, _ = seed_analysis(
        postgres_sessions, week=209905, fingerprint='f' * 64
    )
    barrier = Barrier(2)

    def insert_once() -> UUID:
        barrier.wait(timeout=10)
        with postgres_sessions.begin() as session:
            return create(session, analysis_job_id).job_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        identifiers = list(
            executor.map(lambda _index: insert_once(), range(2))
        )
    assert identifiers[0] == identifiers[1]
    with postgres_sessions() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(BulletinJobRow)
                .where(BulletinJobRow.analysis_job_id == analysis_job_id)
            )
            == 1
        )
