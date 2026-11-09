"""Durable MinIO discovery and atomic incoming materialization."""

import hashlib
import os
import re

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import structlog

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.db.models import IngressObjectRow, IngressStatus
from infodengue_pipeline.models import ObjectMetadata
from infodengue_pipeline.protocols import ObjectStore

logger = structlog.get_logger(__name__)


class HistoricalCompletion(Protocol):
    """Validated AlertaDengue completion metadata for one SHA-256."""

    @property
    def run_id(self) -> UUID:
        """Stable upstream ingestion UUID."""
        ...

    @property
    def sha256(self) -> str:
        """Verified source content digest."""
        ...

    @property
    def canonical_cid10(self) -> str | None:
        """Canonical upstream disease identifier."""
        ...

    @property
    def delivery_se(self) -> int:
        """Validated six digit epidemiological week."""
        ...

    @property
    def finished_at(self) -> datetime:
        """Timezone-aware source completion timestamp."""
        ...


def object_identity(metadata: ObjectMetadata) -> str:
    """Hash immutable object metadata into a stable idempotency key."""
    raw = '\0'.join(
        (
            metadata.bucket,
            metadata.key,
            metadata.etag,
            metadata.version_id or '',
            str(metadata.size),
        )
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def safe_destination(root: Path, metadata: ObjectMetadata) -> Path:
    """Build a contained, collision-free path preserving CSV/DBF suffix."""
    suffix = Path(metadata.key).suffix.lower()
    if suffix not in {'.csv', '.dbf'}:
        raise ValueError('unsupported ingestion file extension')
    identity = object_identity(metadata)
    source_stem = Path(metadata.key).stem
    safe_stem = re.sub(r'[^A-Za-z0-9._-]+', '_', source_stem).strip('._-')
    safe_stem = (safe_stem or 'sinan-object')[:120]
    filename = f'{identity}-{safe_stem}{suffix}'
    destination = (root.resolve() / filename).resolve()
    if not destination.is_relative_to(root.resolve()):
        raise ValueError('incoming destination escapes configured root')
    return destination


def file_sha256(path: Path) -> str:
    """Hash a local file without loading it all into memory."""
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


class IngressCoordinator:
    """Discover supported objects and materialize each object at most once."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        store: ObjectStore,
        incoming_root: Path,
        prefix: str = '',
        completed_run_for_sha256: (
            Callable[[str], HistoricalCompletion | None] | None
        ) = None,
    ) -> None:
        self._sessions = sessions
        self._store = store
        self._root = incoming_root.resolve()
        self._prefix = prefix
        self._completed_run_for_sha256 = completed_run_for_sha256

    def scan(self) -> int:
        """Materialize new and retryable supported objects."""
        staging_root = self._prepare_staging(create_incoming=True)
        completed = 0
        for metadata in self._store.list(self._prefix):
            if Path(metadata.key).suffix.lower() not in {'.csv', '.dbf'}:
                continue
            identity = object_identity(metadata)
            destination = safe_destination(self._root, metadata)
            object_id = uuid5(NAMESPACE_URL, identity)
            staged = staging_root / destination.name
            with self._object_lock(metadata):
                row = self._load_or_create(metadata, destination, object_id)
                if row.status in {
                    IngressStatus.MATERIALIZED,
                    IngressStatus.INGESTED,
                    IngressStatus.SKIPPED,
                    IngressStatus.RECONCILED,
                }:
                    continue
                if row.status is IngressStatus.DISCOVERED and row.sha256:
                    logger.info(
                        'ingress_prepared_recovery',
                        object_id=str(object_id),
                        object_key=metadata.key,
                    )
                else:
                    try:
                        digest = self._download_and_hash(
                            metadata, identity, staged
                        )
                        run = (
                            self._completed_run_for_sha256(digest)
                            if self._completed_run_for_sha256 is not None
                            else None
                        )
                        if run is not None:
                            self._persist_reconciliation(
                                object_id, digest, run
                            )
                            staged.unlink(missing_ok=True)
                            logger.info(
                                'ingress_historical_completion_reconciled',
                                object_id=str(object_id),
                                object_key=metadata.key,
                                sha256=digest,
                                run_id=str(run.run_id),
                            )
                            continue
                        if self._record_duplicate_or_prepared(
                            metadata, object_id, digest, staged
                        ):
                            continue
                    except Exception as exc:
                        staged.unlink(missing_ok=True)
                        self._mark_failed(object_id, exc)
                        logger.error(
                            'ingress_materialization_failed',
                            object_id=str(object_id),
                            object_key=metadata.key,
                            error_type=type(exc).__name__,
                        )
                        continue
                published = self._publish_prepared(
                    metadata, object_id, staged, destination
                )
            if published:
                logger.info(
                    'ingress_object_materialized',
                    object_id=str(object_id),
                    object_key=metadata.key,
                    sha256=row.sha256,
                )
                completed += 1
        return completed

    def reconcile_historical(self, object_keys: list[str]) -> int:
        """Verify and record selected completed objects without publishing.

        Parameters
        ----------
        object_keys
            Exact immutable object keys to reconcile. Unknown objects are
            left untouched for normal ingress.

        Returns
        -------
        int
            Number of newly reconciled historical objects.
        """
        if not object_keys or len(set(object_keys)) != len(object_keys):
            raise ValueError('provide unique historical object keys')
        if self._completed_run_for_sha256 is None:
            raise RuntimeError('historical reconciliation source is required')
        staging_root = self._prepare_staging(create_incoming=False)
        requested = set(object_keys)
        objects = {
            metadata.key: metadata
            for metadata in self._store.list(self._prefix)
            if metadata.key in requested
        }
        missing = requested - objects.keys()
        if missing:
            raise ValueError(
                'requested MinIO object keys not found: '
                + ', '.join(sorted(missing))
            )
        reconciled = 0
        for object_key in object_keys:
            metadata = objects[object_key]
            if Path(metadata.key).suffix.lower() not in {'.csv', '.dbf'}:
                raise ValueError(
                    f'unsupported historical object key: {metadata.key}'
                )
            identity = object_identity(metadata)
            destination = safe_destination(self._root, metadata)
            object_id = uuid5(NAMESPACE_URL, identity)
            staged = staging_root / f'{destination.name}.reconcile'
            prepared = staging_root / destination.name
            with self._object_lock(metadata):
                with self._sessions.begin() as session:
                    row = session.scalar(
                        select(IngressObjectRow)
                        .where(IngressObjectRow.object_identity == identity)
                        .with_for_update()
                    )
                    if row is not None and row.status in {
                        IngressStatus.RECONCILED,
                        IngressStatus.INGESTED,
                        IngressStatus.MATERIALIZED,
                    }:
                        continue
                try:
                    digest = self._download_and_hash(
                        metadata, identity, staged
                    )
                    run = self._completed_run_for_sha256(digest)
                    if run is None:
                        logger.info(
                            'historical_object_not_completed',
                            object_key=metadata.key,
                            sha256=digest,
                        )
                        continue
                    self._load_or_create(metadata, destination, object_id)
                    self._persist_reconciliation(object_id, digest, run)
                    prepared.unlink(missing_ok=True)
                    reconciled += 1
                    logger.info(
                        'historical_object_reconciled',
                        object_id=str(object_id),
                        object_key=metadata.key,
                        sha256=digest,
                        run_id=str(run.run_id),
                        canonical_cid10=run.canonical_cid10,
                        delivery_se=run.delivery_se,
                    )
                finally:
                    staged.unlink(missing_ok=True)
        return reconciled

    def _prepare_staging(self, *, create_incoming: bool) -> Path:
        """Prepare staging without touching watched files."""
        if create_incoming:
            self._root.mkdir(parents=True, exist_ok=True)
        staging_root = self._root.parent / f'.{self._root.name}-staging'
        if staging_root.is_symlink():
            raise RuntimeError(
                'ingress staging directory must not be a symlink'
            )
        staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if staging_root.stat().st_uid != os.geteuid():
            raise PermissionError(
                'ingress staging directory has another owner'
            )
        staging_root.chmod(0o700)
        if create_incoming:
            if self._root.is_mount():
                raise RuntimeError(
                    'incoming must not be a separate mountpoint'
                )
            if staging_root.stat().st_dev != self._root.stat().st_dev:
                raise RuntimeError(
                    'ingress staging and incoming must share a filesystem'
                )
        return staging_root

    @contextmanager
    def _object_lock(self, metadata: ObjectMetadata) -> Iterator[None]:
        """Hold a PostgreSQL session advisory lock without an open txn."""
        with self._sessions() as session:
            bind = session.get_bind()
        if bind.dialect.name != 'postgresql':
            yield
            return
        if not isinstance(bind, Engine):
            raise RuntimeError('PostgreSQL advisory lock requires an Engine')
        lock_key = int.from_bytes(
            hashlib.sha256(
                f'{metadata.bucket}\0{metadata.key}'.encode()
            ).digest()[:8],
            'big',
            signed=True,
        )
        with bind.connect() as connection:
            connection.execute(select(func.pg_advisory_lock(lock_key)))
            connection.commit()
            try:
                yield
            finally:
                connection.execute(select(func.pg_advisory_unlock(lock_key)))
                connection.commit()

    def _load_or_create(
        self,
        metadata: ObjectMetadata,
        destination: Path,
        object_id: UUID,
    ) -> IngressObjectRow:
        """Load or create the durable immutable object row in a short txn."""
        identity = object_identity(metadata)
        with self._sessions.begin() as session:
            row = session.scalar(
                select(IngressObjectRow)
                .where(IngressObjectRow.object_identity == identity)
                .with_for_update()
            )
            if row is None:
                row = IngressObjectRow(
                    object_id=object_id,
                    bucket=metadata.bucket,
                    object_key=metadata.key,
                    object_identity=identity,
                    etag=metadata.etag,
                    version_id=metadata.version_id,
                    size=metadata.size,
                    destination=str(destination),
                    status=IngressStatus.DISCOVERED,
                )
                session.add(row)
                session.flush()
            session.expunge(row)
            return row

    def _download_and_hash(
        self, metadata: ObjectMetadata, identity: str, staged: Path
    ) -> str:
        """Download and verify content outside database transactions."""
        materialized = self._store.materialize(
            metadata.key, staged, metadata.version_id
        )
        if object_identity(materialized) != identity:
            raise IOError(
                f'MinIO object changed during materialization: {metadata.key}'
            )
        if staged.stat().st_size != metadata.size:
            raise IOError(f'incomplete download for {metadata.key}')
        return file_sha256(staged)

    def _record_duplicate_or_prepared(
        self,
        metadata: ObjectMetadata,
        object_id: UUID,
        digest: str,
        staged: Path,
    ) -> bool:
        """Persist verified content or a same-key same-byte duplicate."""
        with self._sessions.begin() as session:
            row = session.get(
                IngressObjectRow, object_id, with_for_update=True
            )
            if row is None:
                raise RuntimeError('ingress row disappeared during scan')
            previous = session.scalar(
                select(IngressObjectRow.object_id).where(
                    IngressObjectRow.bucket == metadata.bucket,
                    IngressObjectRow.object_key == metadata.key,
                    IngressObjectRow.sha256 == digest,
                    IngressObjectRow.status.in_(
                        (
                            IngressStatus.MATERIALIZED,
                            IngressStatus.INGESTED,
                            IngressStatus.SKIPPED,
                            IngressStatus.RECONCILED,
                        )
                    ),
                    IngressObjectRow.object_id != object_id,
                )
            )
            row.sha256 = digest
            row.last_error = None
            if previous is not None:
                row.status = IngressStatus.SKIPPED
                staged.unlink(missing_ok=True)
                logger.info(
                    'ingress_unchanged_content',
                    object_id=str(object_id),
                    object_key=metadata.key,
                    sha256=digest,
                )
                return True
            staged.chmod(0o640)
            row.status = IngressStatus.DISCOVERED
        return False

    def _persist_reconciliation(
        self, object_id: UUID, digest: str, run: HistoricalCompletion
    ) -> None:
        """Commit validated historical metadata without an outbox event."""
        if run.sha256 != digest or run.canonical_cid10 is None:
            raise ValueError('historical completion metadata is inconsistent')
        with self._sessions.begin() as session:
            row = session.get(
                IngressObjectRow, object_id, with_for_update=True
            )
            if row is None:
                raise RuntimeError('ingress row disappeared during reconcile')
            row.sha256 = digest
            row.run_id = run.run_id
            row.canonical_cid10 = run.canonical_cid10.upper()
            row.delivery_se = run.delivery_se
            row.run_finished_at = run.finished_at
            row.reconciled_at = datetime.now(timezone.utc)
            row.status = IngressStatus.RECONCILED
            row.last_error = None

    def _mark_failed(self, object_id: UUID, exc: Exception) -> None:
        """Persist a retryable fail-closed ingress error."""
        with self._sessions.begin() as session:
            row = session.get(
                IngressObjectRow, object_id, with_for_update=True
            )
            if row is not None:
                row.sha256 = None
                row.status = IngressStatus.FAILED
                row.last_error = str(exc)[:512]

    def _publish_prepared(
        self,
        metadata: ObjectMetadata,
        object_id: UUID,
        staged: Path,
        destination: Path,
    ) -> bool:
        """Atomically expose a verified fresh object and commit its state."""
        with self._sessions() as session:
            row = session.get(IngressObjectRow, object_id)
            if row is None or row.status is not IngressStatus.DISCOVERED:
                return False
            digest = row.sha256
        if digest is None:
            raise RuntimeError('prepared ingress has no SHA-256')
        published = False
        try:
            if staged.exists():
                if (
                    staged.stat().st_size != metadata.size
                    or file_sha256(staged) != digest
                ):
                    raise IOError('prepared ingress file changed')
                os.replace(staged, destination)
                published = True
            else:
                if destination.exists() and (
                    destination.stat().st_size != metadata.size
                    or file_sha256(destination) != digest
                ):
                    raise IOError('published ingress file changed')
                logger.info(
                    'ingress_publication_recovered',
                    object_id=str(object_id),
                    object_key=metadata.key,
                )
        except Exception as exc:
            with self._sessions.begin() as session:
                row = session.get(
                    IngressObjectRow, object_id, with_for_update=True
                )
                if row is not None:
                    row.last_error = str(exc)[:512]
            logger.error(
                'ingress_publication_failed',
                object_id=str(object_id),
                object_key=metadata.key,
                error_type=type(exc).__name__,
            )
            return False
        with self._sessions.begin() as session:
            row = session.get(
                IngressObjectRow, object_id, with_for_update=True
            )
            if row is None or row.status is not IngressStatus.DISCOVERED:
                return False
            row.status = IngressStatus.MATERIALIZED
            row.materialized_at = datetime.now(timezone.utc)
            row.last_error = None
        return published
