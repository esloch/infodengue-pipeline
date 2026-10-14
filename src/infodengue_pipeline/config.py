"""Central validated application configuration."""

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from infodengue_pipeline.models import (
    DEFAULT_BR_REQUIRED_DISEASES,
    DEFAULT_UF_REQUIRED_DISEASES,
    Disease,
    disease_from_cid10,
)


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
    br_required_diseases: Annotated[frozenset[Disease], NoDecode] = (
        DEFAULT_BR_REQUIRED_DISEASES
    )
    uf_required_diseases: Annotated[frozenset[Disease], NoDecode] = (
        DEFAULT_UF_REQUIRED_DISEASES
    )

    @field_validator(
        'br_required_diseases', 'uf_required_diseases', mode='before'
    )
    @classmethod
    def parse_required_diseases(_cls, value: object) -> frozenset[Disease]:
        """Parse a comma-separated list of canonical CID-10 codes."""
        if isinstance(value, str) and not value.strip():
            raise ValueError('required_diseases must not be empty')
        values = value.split(',') if isinstance(value, str) else value
        if not isinstance(values, (list, tuple, set, frozenset)):
            raise ValueError(
                'required_diseases must be a comma-separated list of codes'
            )
        if not values:
            raise ValueError('required_diseases must not be empty')
        diseases: list[Disease] = []
        for item in values:
            if not isinstance(item, (str, Disease)):
                raise ValueError(f'invalid required disease code: {item!r}')
            code = item.strip().upper()
            if not code:
                raise ValueError(
                    'required_diseases must not contain empty codes'
                )
            diseases.append(disease_from_cid10(code))
        if len(set(diseases)) != len(diseases):
            raise ValueError('required_diseases contains duplicate codes')
        return frozenset(diseases)

    @field_validator('minio_bucket')
    @classmethod
    def validate_bucket(_cls, value: str) -> str:
        """Reject empty or path-shaped bucket identifiers."""
        if not value or '/' in value or '..' in value:
            raise ValueError('minio_bucket must be a simple bucket name')
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load one validated settings object per process."""
    return Settings()
