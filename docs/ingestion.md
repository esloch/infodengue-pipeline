# Ingestion

The intended ingestion flow is object discovery, validation, materialization,
canonical file placement, chunked staging, and database merge. Large work is
performed by workers; request-facing code does not process full datasets.

Canonical layout follows
`{root}/{country}/{file_type}/{disease}/{year}/{YYYYWW}/{filename}`. The
supported mappings are A90 → `dengue` / `DenInfodengue`, A92.0 → `chik` /
`ChikInfodengue`, and A928 → `zika` / `Infodengue`. Zika identity is resolved
from the full canonical CID-10 code before path routing; the generic A92
prefix is never used to classify it as chikungunya. The legacy mover uses
`Infodengue` as its filename prefix for disease directories other than
dengue and chik. Production StorageBox is not mounted or used here.

Dengue, chikungunya, and Zika are supported. Zika is optional for current
readiness, controlled by `INFODENGUE_PIPELINE_REQUIRED_DISEASES` rather than
fixed disease-specific columns. It can be enabled as a required input without
redesigning the schema. AlertaDengue's historical/runtime ecosystem already
contains Zika data structures, which the pipeline must preserve.

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
