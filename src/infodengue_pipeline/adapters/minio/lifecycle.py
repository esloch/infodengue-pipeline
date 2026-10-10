"""Development-only transient ingress lifecycle configuration."""

from minio import Minio
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule


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
