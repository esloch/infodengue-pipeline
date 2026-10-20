# Migration

This repository connects the existing systems through narrow operational
boundaries. AlertaDengue remains authoritative for SINAN content inspection,
canonical storage and collision behavior, manifest creation, stage, merge,
and rollback. This pipeline does not copy those responsibilities.

## Current integration sequence

```text
MinIO object
  → pipeline ingress record and atomic incoming materialization
  → existing AlertaDengue watcher and ingestion command
  → completed ingestion.run correlated by SHA-256
  → deterministic completion event and durable outbox
  → RabbitMQ and coverage + epiweek workflow state
  → persisted analysis_job and worker-analysis scientific execution
  → validated result
```

Alembic revision `0002_ingress_analysis_jobs` follows
`0001_pipeline_foundation` and adds only pipeline-owned ingress and analysis
job tables. Apply it with the normal development migration command; validate
both a clean database and an upgrade from `0001_pipeline_foundation` before
staging rollout.

## Compatibility and ownership

The AlertaDengue ingestion boundary decides disease, coverage, and epiweek
from file contents. Filenames and MinIO keys are not semantic metadata. Its
canonical mover retains checksum deduplication, collision suffixes, existing
file recovery, and canonical path decisions.

AlertaDengueAnalise remains a finite scientific job contract. The pipeline
supplies the week, states, cores, and safe `load=false` default; it validates
the current job's required output files. It does not import R or require a
sibling checkout at runtime.

The completion bridge rejects ambiguous legacy `A92` metadata. Current
AlertaDengue ingestion retains canonical CID-10 values for new runs.

There is no production cutover or StorageBox ownership migration in this
change. `BoletimQuinzenal` remains outside the scope until a concrete finite
runner contract is implemented.
