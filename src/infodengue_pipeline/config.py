"""Central validated application configuration."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment sourced settings for one pipeline process."""

    model_config = SettingsConfigDict(
        env_prefix='INFODENGUE_PIPELINE_',
        extra='forbid',
    )

    environment: Literal['dev', 'staging', 'prod'] = 'dev'
    log_level: str = 'INFO'
    database_url: SecretStr = SecretStr(
        'postgresql+psycopg://pipeline_dev:pipeline_dev@localhost:5434/'
        'pipeline_dev'
    )
    rabbitmq_url: SecretStr = SecretStr(
        'amqp://pipeline_dev:pipeline_dev@localhost:5673//'
    )
    minio_endpoint: str = 'localhost:9002'
    minio_access_key: SecretStr = SecretStr('pipeline_dev')
    minio_secret_key: SecretStr = SecretStr('pipeline_dev_only')
    minio_secure: bool = False
    minio_bucket: str = 'sinan-infodengue-dev'
    incoming_root: Path = Path('.local/incoming')
    canonical_root: Path = Path('.local/canonical')
    analysis_image: str | None = None
    analysis_cores: int = Field(default=1, ge=1, le=64)

    @field_validator('minio_bucket')
    @classmethod
    def validate_bucket(cls, value: str) -> str:
        """Reject empty or path-shaped bucket identifiers."""
        if not value or '/' in value or '..' in value:
            raise ValueError('minio_bucket must be a simple bucket name')
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load one validated settings object per process."""
    return Settings()
