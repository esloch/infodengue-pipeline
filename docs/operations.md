# Operations

## Phase 0

Phase 0 provides development infrastructure and a guarded staging filesystem
validation only. It does not provide deployable staging E2E, ingestion
watching, content inspection, stage/merge, production cutover, alerting, or
analysis execution.

MinIO is transient ingress. The existing AlertaDengue operational chain uses
`mc mirror --watch` to materialize complete local files in an incoming
directory before its watcher invokes ingestion. In this foundation, MinIO
materialization writes a temporary file and exposes it with an atomic rename
only after download and object identity checks succeed. The adapter does not
interpret object keys as disease, coverage, or epiweek.

## Local staging filesystem validation

The validation writes tiny temporary CSV fixtures only under the explicit
`staging-data/sinan/pipeline-validation` root and removes only files it
created. It does not test the StorageBox canonical destination or establish
StorageBox parity.

```sh
INFODENGUE_PIPELINE_ENVIRONMENT=staging \
INFODENGUE_PIPELINE_CANONICAL_ROOT=/opt/services/infodengue-pipeline/staging-data/sinan/pipeline-validation \
uv run makim staging.validate-storage
```

Do not point this command at production, StorageBox, or an operational SINAN
directory. The code has explicit path guards and refuses other roots.

## Storage distinction

The production historical/current AlertaDengue canonical storage contract is
the imported SINAN tree on StorageBox. Phase 0 validation uses a local
filesystem path solely to exercise deterministic path construction,
containment, and write/readback behavior. StorageBox parity has not been
validated by this repository.
