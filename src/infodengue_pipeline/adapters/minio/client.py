"""Construct an isolated MinIO client from central settings."""

from infodengue_pipeline.adapters.minio.object_store import MinioObjectStore
from infodengue_pipeline.config import Settings
from minio import Minio


def build_object_store(settings: Settings) -> MinioObjectStore:
    """Build the ingress adapter without exposing SDK objects to callers."""
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        secure=settings.minio_secure,
    )
    return MinioObjectStore(
        client,
        settings.minio_bucket,
        settings.incoming_root,
    )
