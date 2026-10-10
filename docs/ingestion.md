# Ingestion

The intended ingestion flow is object discovery, validation, materialization,
canonical file placement, chunked staging, and database merge. Large work is
performed by workers; request-facing code does not process full datasets.

Canonical layout follows
`{root}/{country}/{file_type}/{disease}/{year}/{YYYYWW}/{filename}`. Current
disease mappings are A90 → `dengue` / `DenInfodengue` and A92 → `chik` /
`ChikInfodengue`. Production StorageBox is not mounted or used here.

The migration inventory below records source responsibilities from the
read-only AlertaDengue review:

| Existing responsibility | Current source | Django dependency | Target | Phase |
| --- | --- | --- | --- | --- |
| Ingestion run, file, and audit state | `ingestion/models.py` | Yes | SQLAlchemy repositories | 2–3 |
| Chunked staging and merge tasks | `ingestion/tasks.py` | Yes | ingestion services and workers | 3 |
| File move and canonical naming | `mover_sinan_data.py` | No/limited | filesystem canonical storage | 1 |
| Direct file enqueue | `ingestion_enqueue_sinan.py` | Yes | ingress adapter and application service | 1, 5 |
| Manifest enqueue | `ingestion_enqueue_manifest.py` | Yes | manifest adapter and application service | 1, 5 |
| Watch incoming files | `ingestion_watcher.py` | Yes | MinIO watcher worker | 5 |
| Rollback imported batches | `rollback.py` | Yes | explicit ingestion rollback service | 3 |
| Prune old ingestion state | `retention.py` | Yes | scheduled retention task | 3 |
| Emit completion and coordinate work | not currently transactional | Yes | outbox and workflow coordinator | 6–7 |

The exact schema and behavior must be inventoried and compared before moving
production traffic. Phase 0 only establishes seams and workflow state.
