"""Ingress and AlertaDengue completion bridge integration tests."""

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from infodengue_pipeline.db.engine import SqlUnitOfWork
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    IngressObjectRow,
    IngressStatus,
    OutboxEventRow,
)
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
from sqlalchemy import select


class FakeStore:
    """Small store double that writes complete test payloads."""

    def __init__(self, objects: list[ObjectMetadata]) -> None:
        self.objects = objects
        self.downloads: list[str] = []
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
        destination.write_bytes(b'fixture payload')
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
    coordinator = IngressCoordinator(sessions, store, tmp_path / 'incoming')
    assert coordinator.scan() == 0
    with sessions() as session:
        row = session.scalar(select(IngressObjectRow))
        assert row is not None and row.status is IngressStatus.FAILED


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
        assert 'ambiguous disease code A92' in (ingress.last_error or '')
        assert 'canonical A92.0 or A928 is required' in (
            ingress.last_error or ''
        )
        assert ingress.status is IngressStatus.MATERIALIZED
        assert session.scalars(select(OutboxEventRow)).all() == []


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
    coordinator = CoordinateEpiweek(lambda: SqlUnitOfWork(sessions))
    workflow = coordinator.record(publisher.events[0])
    assert workflow.status is WorkflowStatus.READY
    job_service = AnalysisJobService(
        sessions,
        image='analysis:test',
        cores=1,
        load=False,
        output_root=tmp_path / 'analysis',
    )
    job_id = job_service.claim_ready(Coverage.MG, Epiweek.parse(202639))
    assert job_id is not None
    assert job_service.execute(job_id, SuccessfulAnalysisRunner())
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.status is AnalysisJobStatus.SUCCEEDED
