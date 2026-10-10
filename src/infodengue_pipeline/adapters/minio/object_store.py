"""MinIO implementation of transient object ingress."""

from pathlib import Path

from infodengue_pipeline.ports.contracts import ObjectMetadata
from minio import Minio


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

    def stat(self, key: str) -> ObjectMetadata:
        """Fetch SDK independent object metadata."""
        info = self._client.stat_object(self._bucket, key)
        if info.size is None or info.etag is None:
            raise ValueError(f'Incomplete object metadata for {key}')
        return ObjectMetadata(key, info.size, info.etag)

    def list(self, prefix: str = '') -> list[ObjectMetadata]:
        """List object metadata under a prefix."""
        return [
            ObjectMetadata(item.object_name, item.size, item.etag)
            for item in self._client.list_objects(
                self._bucket, prefix=prefix, recursive=True
            )
        ]

    def materialize(self, key: str, destination: Path) -> ObjectMetadata:
        """Download an object only into the configured incoming workspace."""
        resolved = destination.resolve()
        if not resolved.is_relative_to(self._download_root):
            raise ValueError('download destination escapes incoming workspace')
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self._client.fget_object(self._bucket, key, str(resolved))
        return self.stat(key)
