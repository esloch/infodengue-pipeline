"""PostgreSQL guarantees for durable Recife bulletin job identity."""

import json
import os
import threading

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from uuid import UUID, uuid4

import pytest

from infodengue_pipeline.bulletin.jobs import BulletinJobService
from infodengue_pipeline.bulletin.readiness import BulletinReadinessResult
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
            if current != '0004_bulletin_dispatch':
                pytest.fail(
                    'disposable PostgreSQL database must be migrated to '
                    '0004_bulletin_dispatch'
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


class Ready:
    """Controllable scientific prerequisite for job lifecycle tests."""

    def __init__(self, analysis_id: UUID) -> None:
        self.analysis_id = analysis_id
        self.eligible = True

    def check(
        self, _week: object, _reference: Path, _config: Path
    ) -> BulletinReadinessResult:
        """Return the chosen analysis identity without external data access."""
        return BulletinReadinessResult(
            self.eligible, analysis_job_id=self.analysis_id
        )


def _service(
    sessions: sessionmaker[Session],
    analysis_id: UUID,
    tmp_path: Path,
    render: object,
) -> tuple[BulletinJobService, Ready]:
    readiness = Ready(analysis_id)
    service = BulletinJobService(
        sessions,
        readiness,
        revision=REVISION,
        reference=tmp_path / 'reference.rds',
        config=tmp_path / 'recife.json',
        output_root=tmp_path,
        source_database_url='postgresql://readonly.test/source',
        render=render,
    )
    return service, readiness


def test_bulletin_lifecycle_duplicate_delivery_and_retry(
    postgres_sessions: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    """One claimant runs; failure requires an explicit retry."""
    analysis_id, _ = seed_analysis(
        postgres_sessions, week=209920, fingerprint=uuid4().hex
    )
    entered = threading.Event()
    release = threading.Event()
    fail = True

    def render(**_kwargs: object) -> Path:
        nonlocal fail
        entered.set()
        release.wait(timeout=10)
        if fail:
            raise RuntimeError('secret source URL must not persist')
        pdf = tmp_path / 'bulletin.pdf'
        pdf.write_bytes(b'%PDF-test')
        manifest = tmp_path / 'manifest.json'
        manifest.write_text(
            json.dumps({'status': 'succeeded', 'outputs': {'pdf': str(pdf)}})
        )
        return manifest

    service, readiness = _service(
        postgres_sessions, analysis_id, tmp_path, render
    )
    ids = service.dispatch_candidates(100)
    assert len(ids) == 1
    assert service.dispatch_candidates(100) == ids
    job_id = ids[0]
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(service.execute, job_id)
        assert entered.wait(timeout=10)
        assert service.fail_interrupted(0) == 0
        second = executor.submit(service.execute, job_id)
        release.set()
        assert first.result(timeout=10) is False
        assert second.result(timeout=10) is False
    with postgres_sessions() as session:
        row = session.get(BulletinJobRow, job_id)
        assert row.status is BulletinJobStatus.FAILED
        assert row.attempts == 1
        assert 'secret' not in row.last_error
    assert service.execute(job_id) is False
    assert service.retry(job_id)
    fail = False
    assert service.execute(job_id)
    assert service.execute(job_id) is False
    with postgres_sessions() as session:
        row = session.get(BulletinJobRow, job_id)
        assert row.status is BulletinJobStatus.SUCCEEDED
        assert row.attempts == 2
        assert row.artifact_path == str(tmp_path / 'bulletin.pdf')
        assert row.exit_code == 0
        assert row.finished_at is not None
    readiness.eligible = False
    assert service.dispatch_candidates(100) == []


def test_bulletin_dispatch_recovery_and_interrupted_execution(
    postgres_sessions: sessionmaker[Session],
    tmp_path: Path,
) -> None:
    """Expired publication leases recover; stale running jobs fail once."""
    analysis_id, _ = seed_analysis(
        postgres_sessions, week=209921, fingerprint=uuid4().hex
    )
    service, readiness = _service(
        postgres_sessions, analysis_id, tmp_path, lambda **_kwargs: tmp_path
    )
    job_id = service.dispatch_candidates(100)[0]
    now = datetime.now(timezone.utc)
    assert service.claim_dispatch(job_id, now)
    assert not service.claim_dispatch(job_id, now)
    assert service.claim_dispatch(job_id, now + timedelta(seconds=1))
    readiness.eligible = False
    assert not service.claim_dispatch(job_id, now + timedelta(seconds=2))
    with postgres_sessions.begin() as session:
        row = session.get(BulletinJobRow, job_id)
        row.status = BulletinJobStatus.RUNNING
        row.attempts = 1
        row.started_at = now - timedelta(hours=3)
    assert service.fail_interrupted(7200) == 1
    assert service.fail_interrupted(7200) == 0
    with postgres_sessions() as session:
        row = session.get(BulletinJobRow, job_id)
        assert row.status is BulletinJobStatus.FAILED
        assert row.attempts == 1


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
