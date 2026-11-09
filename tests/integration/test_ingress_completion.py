"""Ingress and AlertaDengue completion bridge integration tests."""

import hashlib

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from stat import S_IMODE
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest

from infodengue_pipeline.db.engine import SqlUnitOfWork
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    IngressObjectRow,
    IngressStatus,
    OutboxEventRow,
    WorkflowInputRow,
)
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.ingestion.completion import (
    CompletedRun,
    IngestionCompletionBridge,
)
from infodengue_pipeline.models import (
    AnalysisJobResult,
    AnalysisJobStatus,
    Coverage,
    Epiweek,
    ObjectMetadata,
    WorkflowStatus,
)
from infodengue_pipeline.services import CoordinateEpiweek, PublishOutbox
from infodengue_pipeline.storage.ingress import (
    IngressCoordinator,
    file_sha256,
    object_identity,
    safe_destination,
)
from sqlalchemy import event, select
from sqlalchemy.orm import Session, sessionmaker


class FakeStore:
    """Small store double that writes complete test payloads."""

    def __init__(self, objects: list[ObjectMetadata]) -> None:
        self.objects = objects
        self.downloads: list[str] = []
        self.payload = b'fixture payload'
        self.fail = False
        self.fail_keys: set[str] = set()

    def bucket_exists(self) -> bool:
        """Report that the fake bucket exists."""
        return True

    def stat(self, _key: str) -> ObjectMetadata:
        """Return one configured fake object's metadata."""
        return self.objects[0]

    def list(self, _prefix: str = '') -> list[ObjectMetadata]:
        """Return fake objects regardless of requested prefix."""
        return self.objects

    def materialize(
        self,
        key: str,
        destination: Path,
        version_id: str | None = None,
    ) -> ObjectMetadata:
        """Write deterministic bytes and report the requested version."""
        self.downloads.append(key)
        if self.fail or key in self.fail_keys:
            raise OSError('temporary object store failure')
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.payload)
        return next(
            item
            for item in self.objects
            if item.key == key and item.version_id == version_id
        )


class FakeCompletedSource:
    """Return a selected completed Run contract row."""

    def __init__(self, run: CompletedRun | None) -> None:
        self.run = run
        self.lookups: list[str] = []

    def by_sha256(self, digest: str) -> CompletedRun | None:
        """Return the configured source result."""
        self.lookups.append(digest)
        return self.run

    def reconcile_by_sha256(self, digest: str) -> CompletedRun | None:
        """Return a completed record or fail closed when lookup fails."""
        return self.by_sha256(digest)


class RecordingPublisher:
    """Collect outbox events as a broker confirmation test double."""

    def __init__(self) -> None:
        self.events = []

    def publish(self, event) -> None:
        """Record an event after simulated broker confirmation."""
        self.events.append(event)


class SuccessfulAnalysisRunner:
    """Complete one fake analysis invocation."""

    def run(self, request):
        """Return a successful fake job result."""
        return AnalysisJobResult(request.job_id, Path('/tmp/output'), True)


def test_ingress_identity_destination_and_deduplication(
    sessions, tmp_path: Path
) -> None:
    """Immutable object identity controls safe, one-time materialization."""
    metadata = ObjectMetadata(
        'sinan', 'unsafe/../name.csv', 15, 'etag-1', 'v1'
    )
    store = FakeStore([metadata])
    root = tmp_path / 'incoming'
    target = safe_destination(root, metadata)
    assert target.parent == root.resolve()
    assert target.suffix == '.csv'
    assert 'name' in target.name
    assert object_identity(metadata) != object_identity(
        ObjectMetadata('sinan', metadata.key, 15, 'etag-1', 'v2')
    )
    coordinator = IngressCoordinator(sessions, store, root)
    assert coordinator.scan() == 1
    target.unlink()
    assert coordinator.scan() == 0
    assert store.downloads == [metadata.key]
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.sha256 is not None
        assert row.status is IngressStatus.MATERIALIZED
        assert len(row.sha256) == 64


def test_historical_completed_object_is_not_published(
    sessions, tmp_path: Path
) -> None:
    """Verified historical bytes are reconciled without publication."""
    metadata = ObjectMetadata('sinan', 'DENGUE (3).csv', 15, 'etag')
    store = FakeStore([metadata])
    digest = hashlib.sha256(store.payload).hexdigest()
    run_id = uuid4()
    run = CompletedRun(
        run_id=run_id,
        status='completed',
        uf='MG',
        disease='A90',
        delivery_se=202644,
        sha256=digest,
        finished_at=datetime.now(timezone.utc),
        canonical_cid10='A90',
    )
    root = tmp_path / 'incoming'
    coordinator = IngressCoordinator(
        sessions,
        store,
        root,
        completed_run_for_sha256=lambda value: (
            run if value == digest else None
        ),
    )

    assert coordinator.scan() == 0
    assert not safe_destination(root, metadata).exists()
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.RECONCILED
        assert row.sha256 == digest
        assert row.run_id == run_id
        assert row.canonical_cid10 == 'A90'
        assert row.delivery_se == 202644
        assert row.run_finished_at == run.finished_at.replace(tzinfo=None)
        assert row.reconciled_at is not None
        assert row.last_error is None
        assert session.scalar(select(OutboxEventRow)) is None
    assert coordinator.scan() == 0
    assert store.downloads == [metadata.key]


def test_explicit_historical_reconciliation_never_publishes_or_emits(
    sessions, tmp_path: Path
) -> None:
    """The operator operation records history only and is idempotent."""
    metadata = ObjectMetadata('sinan', 'CHIKUNGUNYA (4).csv', 15, 'etag')
    store = FakeStore([metadata])
    digest = hashlib.sha256(store.payload).hexdigest()
    run = CompletedRun(
        run_id=uuid4(),
        status='completed',
        uf='BR',
        disease='A92',
        delivery_se=202644,
        sha256=digest,
        finished_at=datetime.now(timezone.utc),
        canonical_cid10='A92.0',
    )
    incoming = tmp_path / 'incoming'
    coordinator = IngressCoordinator(
        sessions,
        store,
        incoming,
        completed_run_for_sha256=lambda _digest: run,
    )

    assert coordinator.reconcile_historical([metadata.key]) == 1
    assert coordinator.reconcile_historical([metadata.key]) == 0
    assert not incoming.exists()
    assert list(tmp_path.glob('.incoming-staging/*')) == []
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.RECONCILED
        assert row.canonical_cid10 == 'A92.0'
        assert row.delivery_se == 202644
        assert row.run_id == run.run_id
        assert session.scalar(select(OutboxEventRow)) is None


def test_unknown_reconciliation_preserves_interrupted_publication(
    sessions, tmp_path: Path
) -> None:
    """A history-only scan cannot discard another scan's prepared file."""
    metadata = ObjectMetadata('sinan', 'pending.csv', 15, 'etag')
    store = FakeStore([metadata])
    digest = hashlib.sha256(store.payload).hexdigest()
    incoming = tmp_path / 'incoming'
    destination = safe_destination(incoming, metadata)
    prepared = tmp_path / '.incoming-staging' / destination.name
    prepared.parent.mkdir()
    prepared.write_bytes(store.payload)
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=uuid4(),
                bucket=metadata.bucket,
                object_key=metadata.key,
                object_identity=object_identity(metadata),
                etag=metadata.etag,
                version_id=metadata.version_id,
                size=metadata.size,
                sha256=digest,
                destination=str(destination),
                status=IngressStatus.DISCOVERED,
            )
        )
    coordinator = IngressCoordinator(
        sessions,
        store,
        incoming,
        completed_run_for_sha256=lambda _digest: None,
    )

    assert coordinator.reconcile_historical([metadata.key]) == 0
    assert prepared.read_bytes() == store.payload
    assert not incoming.exists()
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.DISCOVERED


def test_ingress_fails_closed_when_historical_lookup_is_unavailable(
    sessions, tmp_path: Path
) -> None:
    """A source failure never publishes a verified download."""
    metadata = ObjectMetadata('sinan', 'historical.csv', 15, 'etag')
    store = FakeStore([metadata])

    def unavailable(_digest: str):
        raise OSError('source database unavailable')

    root = tmp_path / 'incoming'
    assert (
        IngressCoordinator(
            sessions, store, root, completed_run_for_sha256=unavailable
        ).scan()
        == 0
    )
    assert not safe_destination(root, metadata).exists()
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.FAILED


def test_same_key_replacement_and_unchanged_content(
    sessions, tmp_path: Path
) -> None:
    """A replacement is published once; a new ETag for old bytes is skipped."""
    first = ObjectMetadata('sinan', 'chik.csv', 15, 'etag-1')
    store = FakeStore([first])
    root = tmp_path / 'incoming'
    coordinator = IngressCoordinator(sessions, store, root)
    assert coordinator.scan() == 1
    safe_destination(root, first).unlink()

    second = ObjectMetadata('sinan', 'chik.csv', 16, 'etag-2')
    store.objects = [second]
    store.payload = b'replacement data'
    assert coordinator.scan() == 1
    assert safe_destination(root, second).read_bytes() == store.payload
    safe_destination(root, second).unlink()
    assert IngressCoordinator(sessions, store, root).scan() == 0

    third = ObjectMetadata('sinan', 'chik.csv', 16, 'etag-3')
    store.objects = [third]
    assert coordinator.scan() == 0
    assert not safe_destination(root, third).exists()
    assert len(store.downloads) == 3
    with sessions() as session:
        rows = session.scalars(select(IngressObjectRow)).all()
        assert [row.status for row in rows] == [
            IngressStatus.MATERIALIZED,
            IngressStatus.MATERIALIZED,
            IngressStatus.SKIPPED,
        ]


def test_staging_is_outside_watched_directory_and_retryable(
    sessions, tmp_path: Path
) -> None:
    """An interrupted staged download never appears in incoming."""
    metadata = ObjectMetadata('sinan', 'zika.dbf', 15, 'etag')
    root = tmp_path / 'incoming'

    class InterruptedStore(FakeStore):
        def materialize(
            self, key: str, destination: Path, version_id: str | None = None
        ) -> ObjectMetadata:
            assert not destination.is_relative_to(root)
            assert list(root.iterdir()) == []
            destination.write_bytes(b'partial')
            if self.fail:
                raise OSError('interrupted')
            return super().materialize(key, destination, version_id)

    store = InterruptedStore([metadata])
    store.fail = True
    coordinator = IngressCoordinator(sessions, store, root)
    assert coordinator.scan() == 0
    assert list(root.iterdir()) == []
    assert list(tmp_path.glob('.incoming-staging/*')) == []
    store.fail = False
    assert coordinator.scan() == 1
    assert safe_destination(root, metadata).read_bytes() == store.payload


def test_prepared_file_recovers_after_publication_failure(
    sessions, tmp_path: Path, monkeypatch
) -> None:
    """A committed staged file is retried without another download."""
    from infodengue_pipeline.storage import ingress

    metadata = ObjectMetadata('sinan', 'dengue.csv', 15, 'etag')
    store = FakeStore([metadata])
    root = tmp_path / 'incoming'
    staged = (
        tmp_path / '.incoming-staging' / safe_destination(root, metadata).name
    )
    replace = ingress.os.replace
    interrupted = True

    def fail_once(source, target) -> None:
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise OSError('interrupted before publication')
        replace(source, target)

    monkeypatch.setattr(ingress.os, 'replace', fail_once)
    coordinator = IngressCoordinator(sessions, store, root)
    assert coordinator.scan() == 0
    assert staged.read_bytes() == store.payload
    assert not safe_destination(root, metadata).exists()
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.DISCOVERED
        assert row.sha256 == file_sha256(staged)
    assert IngressCoordinator(sessions, store, root).scan() == 1
    assert store.downloads == [metadata.key]
    assert S_IMODE(safe_destination(root, metadata).stat().st_mode) == 0o640
    assert S_IMODE(staged.parent.stat().st_mode) == 0o700


@pytest.mark.parametrize('watcher_moves', [False, True])
def test_interrupted_commit_recovers_without_republication(
    sessions, tmp_path: Path, watcher_moves: bool
) -> None:
    """A failed commit after publication cannot trigger a duplicate."""
    metadata = ObjectMetadata('sinan', 'chik.csv', 15, 'etag')
    store = FakeStore([metadata])
    root = tmp_path / 'incoming'
    destination = safe_destination(root, metadata)
    imported = tmp_path / 'imported.csv'

    class InterruptingSession(Session):
        pass

    interrupted = True

    @event.listens_for(InterruptingSession, 'before_commit')
    def consume_then_interrupt(session: Session) -> None:
        nonlocal interrupted
        if not interrupted or not any(
            isinstance(row, IngressObjectRow)
            and row.status is IngressStatus.MATERIALIZED
            for row in session.dirty
        ):
            return
        interrupted = False
        if watcher_moves:
            destination.replace(imported)
        raise OSError('database commit interrupted after publication')

    guarded = sessionmaker(
        sessions.kw['bind'], class_=InterruptingSession, expire_on_commit=False
    )
    with pytest.raises(OSError, match='database commit interrupted'):
        IngressCoordinator(guarded, store, root).scan()
    if watcher_moves:
        assert imported.read_bytes() == store.payload
        assert not destination.exists()
    else:
        assert destination.read_bytes() == store.payload
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.DISCOVERED
        assert row.sha256 == file_sha256(
            imported if watcher_moves else destination
        )
    assert IngressCoordinator(sessions, store, root).scan() == 0
    assert store.downloads == [metadata.key]
    assert destination.exists() is not watcher_moves
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.MATERIALIZED


def test_staging_rejects_symlink_and_cross_filesystem(
    sessions, tmp_path: Path, monkeypatch
) -> None:
    """Staging cannot redirect or require a non-atomic cross-device move."""
    metadata = ObjectMetadata('sinan', 'dengue.csv', 15, 'etag')
    store = FakeStore([metadata])
    root = tmp_path / 'incoming'
    staging = tmp_path / '.incoming-staging'
    other = tmp_path / 'other'
    other.mkdir()
    staging.symlink_to(other, target_is_directory=True)
    with pytest.raises(RuntimeError, match='must not be a symlink'):
        IngressCoordinator(sessions, store, root).scan()
    staging.unlink()

    original_stat = Path.stat

    def different_device(path: Path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == staging:
            return SimpleNamespace(
                st_dev=result.st_dev + 1, st_uid=result.st_uid
            )
        return result

    monkeypatch.setattr(Path, 'stat', different_device)
    with pytest.raises(RuntimeError, match='share a filesystem'):
        IngressCoordinator(sessions, store, root).scan()
    assert store.downloads == []


def test_staging_rejects_incoming_bind_mount(
    sessions, tmp_path: Path, monkeypatch
) -> None:
    """An incoming bind mount can prevent rename despite matching devices."""
    root = tmp_path / 'incoming'
    metadata = ObjectMetadata('sinan', 'dengue.csv', 15, 'etag')
    monkeypatch.setattr(Path, 'is_mount', lambda path: path == root)
    with pytest.raises(RuntimeError, match='separate mountpoint'):
        IngressCoordinator(sessions, FakeStore([metadata]), root).scan()


def test_concurrent_postgres_scans_publish_once(
    postgres_sessions: sessionmaker[Session], tmp_path: Path
) -> None:
    """The per-key PostgreSQL lock spans download and publication."""
    metadata = ObjectMetadata(
        'sinan', f'concurrency-{uuid4()}.csv', 15, 'etag'
    )
    store = FakeStore([metadata])
    coordinator = IngressCoordinator(
        postgres_sessions, store, tmp_path / 'incoming'
    )
    barrier = Barrier(2)

    def scan() -> int:
        barrier.wait(timeout=5)
        return coordinator.scan()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: scan(), range(2)))
    assert sorted(results) == [0, 1]
    assert store.downloads == [metadata.key]


def test_concurrent_postgres_historical_reconciliation_is_idempotent(
    postgres_sessions: sessionmaker[Session], tmp_path: Path
) -> None:
    """Session advisory locks serialize two real PostgreSQL reconciliations."""
    metadata = ObjectMetadata('sinan', f'historical-{uuid4()}.csv', 15, 'etag')
    store = FakeStore([metadata])
    digest = hashlib.sha256(store.payload).hexdigest()
    run = CompletedRun(
        uuid4(),
        'completed',
        'MG',
        'A90',
        202644,
        digest,
        datetime.now(timezone.utc),
        'A90',
    )
    coordinator = IngressCoordinator(
        postgres_sessions,
        store,
        tmp_path / 'incoming',
        completed_run_for_sha256=lambda _digest: run,
    )
    barrier = Barrier(2)

    def reconcile() -> int:
        barrier.wait(timeout=5)
        return coordinator.reconcile_historical([metadata.key])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: reconcile(), range(2)))

    assert sorted(results) == [0, 1]
    assert store.downloads == [metadata.key]
    with postgres_sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.RECONCILED
        assert row.sha256 == digest
        assert row.run_id == run.run_id
        assert session.scalar(select(OutboxEventRow)) is None


def test_postgres_ingress_commits_before_external_source_lookup(
    postgres_sessions: sessionmaker[Session], tmp_path: Path
) -> None:
    """Source lookup runs after the row-creation transaction has committed."""
    metadata = ObjectMetadata('sinan', f'fresh-{uuid4()}.csv', 15, 'etag')
    store = FakeStore([metadata])
    identity = object_identity(metadata)
    observed_statuses: list[IngressStatus | None] = []

    def unknown_source(_digest: str):
        with postgres_sessions() as session:
            row = session.scalar(
                select(IngressObjectRow).where(
                    IngressObjectRow.object_identity == identity
                )
            )
            observed_statuses.append(row.status if row is not None else None)
        return None

    coordinator = IngressCoordinator(
        postgres_sessions,
        store,
        tmp_path / 'incoming',
        completed_run_for_sha256=unknown_source,
    )
    assert coordinator.scan() == 1
    assert observed_statuses == [IngressStatus.DISCOVERED]
    with postgres_sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None
        assert row.status is IngressStatus.MATERIALIZED


def test_ingress_retries_failed_materialization_and_skips_other_formats(
    sessions, tmp_path: Path
) -> None:
    """A transient failure is durable and retried; unsupported objects skip."""
    metadata = ObjectMetadata('sinan', 'file.dbf', 15, 'etag')
    unsupported = ObjectMetadata('sinan', 'notes.txt', 4, 'other')
    store = FakeStore([metadata, unsupported])
    coordinator = IngressCoordinator(sessions, store, tmp_path / 'incoming')
    store.fail_keys.add(metadata.key)
    assert coordinator.scan() == 0
    with sessions() as session:
        failed = session.scalar(select(IngressObjectRow))
        assert failed is not None and failed.status is IngressStatus.FAILED
    store.fail_keys.clear()
    assert coordinator.scan() == 1
    assert store.downloads == [metadata.key, metadata.key]
    with sessions() as session:
        retried = session.scalar(select(IngressObjectRow))
        assert retried is not None
        assert retried.status is IngressStatus.MATERIALIZED
        assert retried.last_error is None


def test_ingress_failure_does_not_block_later_objects(
    sessions, tmp_path: Path
) -> None:
    """A failed object is isolated while later objects still materialize."""
    failed = ObjectMetadata('sinan', 'failed.csv', 15, 'etag-a')
    succeeding = ObjectMetadata('sinan', 'succeeding.csv', 15, 'etag-b')
    store = FakeStore([failed, succeeding])
    store.fail_keys.add(failed.key)

    assert (
        IngressCoordinator(sessions, store, tmp_path / 'incoming').scan() == 1
    )
    with sessions() as session:
        rows = session.scalars(
            select(IngressObjectRow).order_by(IngressObjectRow.object_key)
        ).all()
        assert [row.status for row in rows] == [
            IngressStatus.FAILED,
            IngressStatus.MATERIALIZED,
        ]


def test_ingress_rejects_object_change_during_materialization(
    sessions, tmp_path: Path
) -> None:
    """The SHA record cannot be attached to a changed MinIO object version."""
    metadata = ObjectMetadata('sinan', 'file.csv', 15, 'etag', 'old-version')

    class ChangedStore(FakeStore):
        def materialize(
            self,
            key: str,
            destination: Path,
            version_id: str | None = None,
        ) -> ObjectMetadata:
            super().materialize(key, destination, version_id)
            return ObjectMetadata(
                metadata.bucket,
                metadata.key,
                metadata.size,
                metadata.etag,
                'new-version',
            )

    store = ChangedStore([metadata])
    root = tmp_path / 'incoming'
    destination = safe_destination(root, metadata)
    unrelated = root / 'unrelated.dbf'
    root.mkdir()
    unrelated.write_bytes(b'keep')
    coordinator = IngressCoordinator(sessions, store, root)
    assert coordinator.scan() == 0
    assert not destination.exists()
    assert unrelated.read_bytes() == b'keep'
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.FAILED
        assert row.sha256 is None


def test_ingress_preserves_existing_destination_if_materialize_raises(
    sessions, tmp_path: Path
) -> None:
    """A failure before atomic exposure must not remove an existing file."""
    metadata = ObjectMetadata('sinan', 'file.csv', 15, 'etag')
    store = FakeStore([metadata])
    store.fail = True
    root = tmp_path / 'incoming'
    root.mkdir()
    destination = safe_destination(root, metadata)
    destination.write_bytes(b'pre-existing')

    assert IngressCoordinator(sessions, store, root).scan() == 0
    assert destination.read_bytes() == b'pre-existing'
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.FAILED
        assert row.sha256 is None


def test_completion_bridge_maps_completed_run_to_one_outbox_event(
    sessions,
) -> None:
    """SHA correlation, validation, and repeat polling are idempotent."""
    digest = 'a' * 64
    metadata = ObjectMetadata('sinan', 'file.csv', 15, 'etag')
    object_id = uuid4()
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=object_id,
                bucket=metadata.bucket,
                object_key=metadata.key,
                object_identity=object_identity(metadata),
                etag=metadata.etag,
                version_id=None,
                size=metadata.size,
                sha256=digest,
                destination='/tmp/materialized.csv',
                status=IngressStatus.MATERIALIZED,
            )
        )
    run = CompletedRun(
        run_id=uuid4(),
        status='completed',
        uf='MG',
        disease='A90',
        delivery_se=202639,
        sha256=digest,
        finished_at=datetime.now(timezone.utc),
        canonical_cid10='A90',
    )
    source = FakeCompletedSource(run)
    bridge = IngestionCompletionBridge(sessions, source)
    assert bridge.poll() == 1
    assert bridge.poll() == 0
    with sessions() as session:
        ingress = session.get(IngressObjectRow, object_id)
        events = session.scalars(select(OutboxEventRow)).all()
        assert ingress is not None
        assert ingress.status is IngressStatus.INGESTED
        assert ingress.run_id == run.run_id
        assert len(events) == 1
        assert events[0].payload['coverage'] == 'MG'
        assert events[0].payload['disease'] == 'A90'
        assert events[0].payload['fingerprint'] == digest
        assert events[0].payload['event_id']


def test_bridge_ignores_noncompleted_source_result(sessions) -> None:
    """A missing or failed completed-run lookup produces no event."""
    source = FakeCompletedSource(None)
    assert IngestionCompletionBridge(sessions, source).poll() == 0
    with sessions() as session:
        assert session.scalars(select(OutboxEventRow)).all() == []


def test_bridge_does_not_emit_for_failed_run(sessions) -> None:
    """A source row with a failed status cannot become a success event."""
    metadata = ObjectMetadata('sinan', 'failed.csv', 15, 'etag')
    object_id = uuid4()
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=object_id,
                bucket=metadata.bucket,
                object_key=metadata.key,
                object_identity=object_identity(metadata),
                etag=metadata.etag,
                version_id=None,
                size=metadata.size,
                sha256='f' * 64,
                destination='/tmp/failed.csv',
                status=IngressStatus.MATERIALIZED,
            )
        )
    failed_run = CompletedRun(
        uuid4(),
        'failed',
        'MG',
        'A90',
        202639,
        'f' * 64,
        datetime.now(timezone.utc),
    )
    assert (
        IngestionCompletionBridge(
            sessions, FakeCompletedSource(failed_run)
        ).poll()
        == 0
    )
    with sessions() as session:
        assert session.scalars(select(OutboxEventRow)).all() == []


def test_bridge_rejects_ambiguous_external_disease_code(sessions) -> None:
    """Collapsed external A92 is recorded as invalid, never guessed."""
    metadata = ObjectMetadata('sinan', 'ambiguous.csv', 15, 'etag')
    object_id = uuid4()
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=object_id,
                bucket=metadata.bucket,
                object_key=metadata.key,
                object_identity=object_identity(metadata),
                etag=metadata.etag,
                version_id=None,
                size=metadata.size,
                sha256='d' * 64,
                destination='/tmp/ambiguous.csv',
                status=IngressStatus.MATERIALIZED,
            )
        )
    run = CompletedRun(
        uuid4(),
        'completed',
        'BR',
        'A92',
        202639,
        'd' * 64,
        datetime.now(timezone.utc),
    )
    source = FakeCompletedSource(run)
    bridge = IngestionCompletionBridge(sessions, source)
    assert bridge.poll() == 0
    assert bridge.poll() == 0
    assert len(source.lookups) == 1
    with sessions() as session:
        ingress = session.get(IngressObjectRow, object_id)
        assert ingress is not None
        assert 'ambiguous A92 disease without canonical CID' in (
            ingress.last_error or ''
        )
        assert ingress.status is IngressStatus.MATERIALIZED
        assert session.scalars(select(OutboxEventRow)).all() == []
        assert session.scalars(select(WorkflowInputRow)).all() == []


@pytest.mark.parametrize(
    ('canonical_cid10', 'expected_disease'),
    [('A92.0', 'A92.0'), ('A928', 'A928')],
)
def test_bridge_uses_canonical_disease_for_coarse_a92(
    sessions, canonical_cid10, expected_disease
) -> None:
    """Canonical A92 variants bridge with their exact disease identity."""
    digest = 'e' * 64
    metadata = ObjectMetadata('sinan', f'{expected_disease}.csv', 15, 'etag')
    object_id = uuid4()
    with sessions.begin() as session:
        session.add(
            IngressObjectRow(
                object_id=object_id,
                bucket=metadata.bucket,
                object_key=metadata.key,
                object_identity=object_identity(metadata),
                etag=metadata.etag,
                version_id=None,
                size=metadata.size,
                sha256=digest,
                destination='/tmp/canonical.csv',
                status=IngressStatus.MATERIALIZED,
            )
        )
    run = CompletedRun(
        uuid4(),
        'completed',
        'BR',
        'A92',
        202639,
        digest,
        datetime.now(timezone.utc),
        canonical_cid10,
    )
    assert (
        IngestionCompletionBridge(sessions, FakeCompletedSource(run)).poll()
        == 1
    )
    with sessions() as session:
        ingress = session.get(IngressObjectRow, object_id)
        event = session.scalar(select(OutboxEventRow))
        assert ingress is not None and ingress.run_id == run.run_id
        assert event is not None
        assert event.payload['disease'] == expected_disease
        assert event.payload['fingerprint'] == digest
    workflow = CoordinateEpiweek(
        lambda: SqlUnitOfWork(sessions),
        initial_analysis_week=Epiweek.parse(202639),
    ).record(IngestionCompleted.model_validate(event.payload))
    with sessions() as session:
        input_row = session.scalar(
            select(WorkflowInputRow).where(
                WorkflowInputRow.run_id == run.run_id
            )
        )
        assert input_row is not None
        assert input_row.workflow_id == workflow.workflow_id
        assert input_row.disease.value == expected_disease
        assert input_row.run_id == run.run_id
        assert input_row.fingerprint == digest


def test_automated_orchestration_e2e(sessions, tmp_path: Path) -> None:
    """Materialization through outbox, readiness, job claim, and success."""
    metadata = ObjectMetadata('sinan', 'approved/input.csv', 15, 'etag')
    store = FakeStore([metadata])
    root = tmp_path / 'incoming'
    assert IngressCoordinator(sessions, store, root).scan() == 1
    destination = safe_destination(root, metadata)
    digest = file_sha256(destination)
    run = CompletedRun(
        uuid4(),
        'completed',
        'MG',
        'A90',
        202639,
        digest,
        datetime.now(timezone.utc),
    )
    bridge = IngestionCompletionBridge(sessions, FakeCompletedSource(run))
    assert bridge.poll() == 1
    publisher = RecordingPublisher()
    assert (
        PublishOutbox(
            lambda: SqlUnitOfWork(sessions),
            publisher,
        ).execute()
        == 1
    )
    coordinator = CoordinateEpiweek(
        lambda: SqlUnitOfWork(sessions),
        initial_analysis_week=Epiweek.parse(202639),
    )
    workflow = coordinator.record(publisher.events[0])
    assert workflow.status is WorkflowStatus.READY
    job_service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=1,
        load=False,
        output_root=tmp_path / 'analysis',
        initial_analysis_week=Epiweek.parse(202639),
    )
    job_id = job_service.claim_ready(Coverage.MG, Epiweek.parse(202639))
    assert job_id is not None
    assert job_service.execute(job_id, SuccessfulAnalysisRunner())
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.SUCCEEDED
