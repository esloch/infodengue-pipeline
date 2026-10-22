"""MinIO implementation of transient object ingress."""

import os
import tempfile

from pathlib import Path

from minio import Minio
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from infodengue_pipeline.config import Settings
from infodengue_pipeline.models import ObjectMetadata


def _normalize_version_id(version_id: str | None) -> str | None:
    """Treat MinIO's unversioned sentinel as the absence of a version."""
    return None if version_id == 'null' else version_id


class MinioObjectStore:
    """Access one configured development ingress bucket."""

    def __init__(
        self,
        client: Minio,
        bucket: str,
        download_root: Path,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._download_root = download_root.resolve()

    def bucket_exists(self) -> bool:
        """Report whether the configured bucket exists."""
        return self._client.bucket_exists(self._bucket)

    def stat(self, key: str, version_id: str | None = None) -> ObjectMetadata:
        """Fetch SDK independent object metadata."""
        version_id = _normalize_version_id(version_id)
        info = (
            self._client.stat_object(self._bucket, key)
            if version_id is None
            else self._client.stat_object(
                self._bucket, key, version_id=version_id
            )
        )
        if info.size is None or info.etag is None:
            raise ValueError(f'Incomplete object metadata for {key}')
        return ObjectMetadata(
            self._bucket,
            key,
            info.size,
            info.etag,
            _normalize_version_id(info.version_id),
        )

    def list(self, prefix: str = '') -> list[ObjectMetadata]:
        """List object metadata under a prefix."""
        return [
            ObjectMetadata(
                self._bucket,
                item.object_name,
                item.size,
                item.etag,
                _normalize_version_id(item.version_id),
            )
            for item in self._client.list_objects(
                self._bucket,
                prefix=prefix,
                recursive=True,
                include_version=True,
            )
        ]

    def materialize(
        self,
        key: str,
        destination: Path,
        version_id: str | None = None,
    ) -> ObjectMetadata:
        """Atomically expose a complete download inside incoming workspace."""
        resolved = destination.resolve()
        if not resolved.is_relative_to(self._download_root):
            raise ValueError('download destination escapes incoming workspace')
        resolved.parent.mkdir(parents=True, exist_ok=True)
        info = self.stat(key, version_id)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f'.{resolved.name}.part-', dir=resolved.parent
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            if info.version_id is None:
                self._client.fget_object(self._bucket, key, str(temporary))
            else:
                self._client.fget_object(
                    self._bucket,
                    key,
                    str(temporary),
                    version_id=info.version_id,
                )
            if temporary.stat().st_size != info.size:
                raise IOError(f'incomplete download for {key}')
            current = self.stat(key, info.version_id)
            if (
                current.etag != info.etag
                or current.size != info.size
                or current.version_id != info.version_id
            ):
                raise IOError(f'object changed during download for {key}')
            os.replace(temporary, resolved)
            return info
        finally:
            temporary.unlink(missing_ok=True)


def build_object_store(settings: Settings) -> MinioObjectStore:
    """Build the ingress adapter from central settings."""
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        secure=settings.minio_secure,
    )
    return MinioObjectStore(
        client, settings.minio_bucket, settings.incoming_root
    )


def configure_development_lifecycle(
    client: Minio,
    *,
    bucket: str,
    environment: str,
) -> None:
    """Set a seven day expiry only on the known development bucket."""
    if environment != 'dev' or bucket != 'sinan-infodengue-dev':
        raise ValueError('lifecycle bootstrap is restricted to dev ingress')
    config = LifecycleConfig(
        [
            Rule(
                ENABLED,
                rule_id='expire-transient-ingress-after-7-days',
                expiration=Expiration(days=7),
            )
        ]
    )
    client.set_bucket_lifecycle(bucket, config)
