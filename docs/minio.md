# MinIO

MinIO is the transient ingress store. `ObjectStore` isolates SDK operations;
objects are materialized before validation and canonical placement. Imported
files remain in StorageBox, while processed/queryable records live in
PostgreSQL.

Local development uses bucket `sinan-infodengue-dev`, fake local credentials,
and a seven-day lifecycle policy. Bootstrap checks the environment and bucket
name before changing bucket configuration. Never point this command at a
production endpoint. No production MinIO bucket or ownership has changed.

The development Compose service uses the same MinIO release family as the
read-only reference deployment and keeps data in a named local volume.
