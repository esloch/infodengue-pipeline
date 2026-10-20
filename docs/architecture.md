# Architecture

## Ownership boundaries

```text
AlertaDengue
  validated SINAN inspection, canonical mover, manifest, stage, merge,
  rollback, Municipio.Notificacao, public application

infodengue-pipeline
  MinIO ingress state and materialization, completion bridge, events/outbox,
  workflow readiness, analysis job identity, scheduling, retries, direct runner

AlertaDengueAnalise
  R / INLA computation, RData, SQL generation, optional SQL application, maps
```

The pipeline does not parse SINAN content or import Django/R code. The incoming
filename is provenance only. The existing AlertaDengue watcher and ingestion
commands decide the file's disease, coverage, and epiweek.

## End-to-end flow

```text
MinIO
  ↓
infodengue-pipeline ingress (stable metadata identity, atomic local write)
  ↓
shared AlertaDengue incoming directory and existing watcher
  ↓
content inspection → mover → manifest → stage → merge
  ↓
completed ingestion.run identified by local SHA-256
  ↓
read-only completion bridge → deterministic event → PostgreSQL outbox
  ↓
RabbitMQ / Celery → workflow readiness by (coverage, epiweek)
  ↓
one persisted analysis_job per current input fingerprint
  ↓
worker-analysis container → AlertaDengueAnalise runtime in the same container
  ↓
validated current-week RData and SQL → SUCCEEDED or FAILED
```

Ingress rows are keyed by a SHA-256 of bucket, key, ETag, version ID, and
size. Supported `.csv` and `.dbf` objects receive a deterministic UUID-based
destination below `incoming_root`; key contents never become a filesystem
path. The materializer computes the downloaded file SHA-256 used by
`ingestion.run.sha256`.

The completion adapter reads only `ingestion.run` fields and does not import
the AlertaDengue application. Only `status=completed` creates an
`IngestionCompleted` event. The deterministic event UUID and outbox primary
key make repeated polling idempotent.

## Workflow identity and readiness

Workflow identity is `(coverage, epiweek)`, so `BR/202639`, `MG/202639`, and
`SP/202639` remain separate. National input requires A90 and A92.0; A928 is
optional. State input requires A90; A92.0 and A928 are optional. The newest
per-disease input wins by `(completed_at, run_id)`. Every available supported
input participates in the fingerprint.

AlertaDengue now preserves canonical CID-10 values in completed run metadata.
The completion bridge rejects ambiguous legacy `A92` values.

PostgreSQL row locking protects the READY-to-QUEUED transition and unique
`(workflow_id, input_fingerprint)` constraint allows one logical analysis job.
The analysis consumer atomically claims QUEUED; duplicate delivery cannot run
RUNNING or terminal jobs. Failed jobs require an explicit retry. Completion
changes workflow status only if that job still matches the current fingerprint.

## Analysis contract

The analysis worker runs the scientific process directly through
`makim pipeline.refresh-alertas-job`, passing `--week`, `--states`, `--cores`,
and `--load`. BR maps to `ALL`; a state maps to that UF. Credentials are passed
through subprocess environment variables and never included in argv or logs.
The default is `--load false`. A zero exit status is necessary but not
sufficient: current-week per-state RData, consolidated BR RData, and both
non-empty dengue and chikungunya SQL files are validated. Maps are required only with
`load=true`.

The `pipeline.analysis` worker runs inside the dedicated Compose service.
The image extends AlertaDengueAnalise with the installed pipeline package.
No Docker socket or CLI is used at runtime. Celery concurrency is one;
`--cores` is the separate scientific state-parallelism setting.

## Validation status

Automated orchestration tests use isolated local SQL state and fake storage,
source, and runner boundaries. They do not execute the scientific workload.
The guarded staging validator requires `environment=staging`, an explicit
safe fixture object key, real shared staging services, and `load=false`. It
has not been run against staging unless separately reported as such.
