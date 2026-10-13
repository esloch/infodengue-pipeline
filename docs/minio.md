# MinIO

MinIO is the transient ingress store. `ObjectStore` isolates SDK operations;
objects are materialized before validation and canonical placement. Imported
files remain in StorageBox, while processed/queryable records live in
PostgreSQL.

Local development uses bucket `sinan-infodengue-dev`, fake local credentials,
and a seven-day lifecycle policy. Bootstrap checks the environment and bucket
name before changing bucket configuration. Never point this command at a
production endpoint. No production MinIO bucket or ownership has changed.

MinIO Community stopped publishing maintained prebuilt Community images. The
development Compose service therefore builds the official Community source at
`RELEASE.2025-10-15T17-29-55Z` (commit
`9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`) into the local image
`infodengue-pipeline-minio:RELEASE.2025-10-15`. AIStor is not used because it
requires a license. The local service retains `server /data --console-address
":9001"`, development-only credentials, its healthcheck, and the named
`pipeline_minio` volume.

This pinned Community build supports local development and parity only. The
Community repository is no longer maintained, so review the object-store
strategy again before a production cutover. This release is not a long-term
production solution.
