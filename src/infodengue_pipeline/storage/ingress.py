"""Durable MinIO discovery and atomic incoming materialization."""

import hashlib
import re

from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import structlog

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.db.models import IngressObjectRow, IngressStatus
from infodengue_pipeline.models import ObjectMetadata
from infodengue_pipeline.protocols import ObjectStore

logger = structlog.get_logger(__name__)


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
    ) -> None:
        self._sessions = sessions
        self._store = store
        self._root = incoming_root.resolve()
        self._prefix = prefix

    def scan(self) -> int:
        """Materialize new and retryable supported objects."""
        self._root.mkdir(parents=True, exist_ok=True)
        completed = 0
        for metadata in self._store.list(self._prefix):
            if Path(metadata.key).suffix.lower() not in {'.csv', '.dbf'}:
                continue
            identity = object_identity(metadata)
            destination = safe_destination(self._root, metadata)
            object_id = uuid5(NAMESPACE_URL, identity)
            with self._sessions.begin() as session:
                if session.bind is not None and (
                    session.bind.dialect.name == 'postgresql'
                ):
                    lock_key = int.from_bytes(
                        bytes.fromhex(identity[:16]), 'big', signed=True
                    )
                    session.execute(
                        select(func.pg_advisory_xact_lock(lock_key))
                    )
                row = session.scalar(
                    select(IngressObjectRow)
                    .where(IngressObjectRow.object_identity == identity)
                    .with_for_update()
                )
                if row is not None and row.status in {
                    IngressStatus.MATERIALIZED,
                    IngressStatus.INGESTED,
                }:
                    continue
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
            try:
                materialized = self._store.materialize(
                    metadata.key, destination, metadata.version_id
                )
                if object_identity(materialized) != identity:
                    raise IOError(
                        f'MinIO object changed during materialization: '
                        f'{metadata.key}'
                    )
                digest = file_sha256(destination)
                with self._sessions.begin() as session:
                    row = session.get(IngressObjectRow, object_id)
                    if row is None:
                        raise RuntimeError('ingress record disappeared')
                    row.sha256 = digest
                    row.status = IngressStatus.MATERIALIZED
                    row.materialized_at = datetime.now(timezone.utc)
                    row.last_error = None
                logger.info(
                    'ingress_object_materialized',
                    object_id=str(object_id),
                    object_key=metadata.key,
                    sha256=digest,
                )
                completed += 1
            except Exception as exc:
                with self._sessions.begin() as session:
                    row = session.get(IngressObjectRow, object_id)
                    if row is not None:
                        row.status = IngressStatus.FAILED
                        row.last_error = str(exc)[:512]
                logger.error(
                    'ingress_materialization_failed',
                    object_id=str(object_id),
                    object_key=metadata.key,
                    error_type=type(exc).__name__,
                )
                raise
        return completed
