"""Durable MinIO discovery and atomic incoming materialization."""

import hashlib
import os
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
        if self._root.is_mount():
            raise RuntimeError('incoming must not be a separate mountpoint')
        if staging_root.stat().st_dev != self._root.stat().st_dev:
            raise RuntimeError(
                'ingress staging and incoming must share a filesystem'
            )
        completed = 0
        for metadata in self._store.list(self._prefix):
            if Path(metadata.key).suffix.lower() not in {'.csv', '.dbf'}:
                continue
            identity = object_identity(metadata)
            destination = safe_destination(self._root, metadata)
            object_id = uuid5(NAMESPACE_URL, identity)
            staged = staging_root / destination.name
            with self._sessions.begin() as session:
                self._lock_key(session, metadata)
                row = session.scalar(
                    select(IngressObjectRow)
                    .where(IngressObjectRow.object_identity == identity)
                    .with_for_update()
                )
                if row is not None and row.status in {
                    IngressStatus.MATERIALIZED,
                    IngressStatus.INGESTED,
                    IngressStatus.SKIPPED,
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
                    session.flush()
                if row.status is IngressStatus.DISCOVERED and row.sha256:
                    logger.info(
                        'ingress_prepared_recovery',
                        object_id=str(object_id),
                        object_key=metadata.key,
                    )
                else:
                    try:
                        materialized = self._store.materialize(
                            metadata.key, staged, metadata.version_id
                        )
                        if object_identity(materialized) != identity:
                            raise IOError(
                                'MinIO object changed during materialization: '
                                f'{metadata.key}'
                            )
                        if staged.stat().st_size != metadata.size:
                            raise IOError(
                                f'incomplete download for {metadata.key}'
                            )
                        digest = file_sha256(staged)
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
                            continue
                        staged.chmod(0o640)
                        row.status = IngressStatus.DISCOVERED
                    except Exception as exc:
                        staged.unlink(missing_ok=True)
                        row.sha256 = None
                        row.status = IngressStatus.FAILED
                        row.last_error = str(exc)[:512]
                        logger.error(
                            'ingress_materialization_failed',
                            object_id=str(object_id),
                            object_key=metadata.key,
                            error_type=type(exc).__name__,
                        )
                        continue
            with self._sessions.begin() as session:
                self._lock_key(session, metadata)
                row = session.get(
                    IngressObjectRow, object_id, with_for_update=True
                )
                if row is None or row.status is not IngressStatus.DISCOVERED:
                    continue
                if row.sha256 is None:
                    raise RuntimeError('prepared ingress has no SHA-256')
                published = False
                try:
                    if staged.exists():
                        if (
                            staged.stat().st_size != metadata.size
                            or file_sha256(staged) != row.sha256
                        ):
                            raise IOError('prepared ingress file changed')
                        os.replace(staged, destination)
                        published = True
                    else:
                        if destination.exists() and (
                            destination.stat().st_size != metadata.size
                            or file_sha256(destination) != row.sha256
                        ):
                            raise IOError('published ingress file changed')
                        logger.info(
                            'ingress_publication_recovered',
                            object_id=str(object_id),
                            object_key=metadata.key,
                        )
                    row.status = IngressStatus.MATERIALIZED
                    row.materialized_at = datetime.now(timezone.utc)
                    row.last_error = None
                except Exception as exc:
                    row.last_error = str(exc)[:512]
                    logger.error(
                        'ingress_publication_failed',
                        object_id=str(object_id),
                        object_key=metadata.key,
                        error_type=type(exc).__name__,
                    )
                    continue
            if published:
                logger.info(
                    'ingress_object_materialized',
                    object_id=str(object_id),
                    object_key=metadata.key,
                    sha256=row.sha256,
                )
                completed += 1
        return completed

    @staticmethod
    def _lock_key(session: Session, metadata: ObjectMetadata) -> None:
        """Serialize one bucket/key across PostgreSQL scan transactions."""
        if session.bind is None or session.bind.dialect.name != 'postgresql':
            return
        lock_key = int.from_bytes(
            hashlib.sha256(
                f'{metadata.bucket}\0{metadata.key}'.encode()
            ).digest()[:8],
            'big',
            signed=True,
        )
        session.execute(select(func.pg_advisory_xact_lock(lock_key)))
