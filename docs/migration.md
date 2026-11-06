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

## Pipeline schema migration workflow

The migration configuration is `alembic.ini`; `alembic/env.py` loads the
validated `INFODENGUE_PIPELINE_DATABASE_URL` and uses only the SQLAlchemy
metadata in `infodengue_pipeline.db.models`. Revisions are in
`alembic/versions/` and own the `pipeline` schema. Do not add scientific
analysis SQL, Django models, or AlertaDengue schema changes here. Those systems
own their own migrations and scientific SQL.

For development, start the local stack and apply revisions with:

```sh
uv run makim containers.up
uv run makim db.upgrade
```

For inspection and revision authoring, use Alembic against a private
development database URL:

```sh
INFODENGUE_PIPELINE_DATABASE_URL='postgresql+psycopg://…' \
  uv run alembic current
INFODENGUE_PIPELINE_DATABASE_URL='postgresql+psycopg://…' \
  uv run alembic history
INFODENGUE_PIPELINE_DATABASE_URL='postgresql+psycopg://…' \
  uv run alembic revision --autogenerate -m 'describe pipeline change'
```

Review generated DDL and the revision's `upgrade()` and `downgrade()` before
keeping it. Autogeneration is a draft and may not capture constraints,
backfills, data safety, or server-specific behavior. Test a fresh database
upgrade and an upgrade from the current supported revision. Apply reviewed
revisions with `uv run alembic upgrade head` only in the intended environment;
verify with `uv run alembic current` and the expected pipeline tables. The
`db.upgrade` Makim task hardcodes the local development URL and is not an
environment-neutral deployment command.

The staging and production Compose files define a one-shot `migrate` service
that waits for the pipeline PostgreSQL health check and runs
`alembic upgrade head`. Their dependent workers wait for the service to
complete successfully during Compose startup. Operators can also run
`docker compose ... up migrate` explicitly for maintenance; validate private
configuration first. Building an image or validating Compose configuration
does not apply a migration.

Adding another finite workload may require pipeline-owned durable state, such
as a job table, uniqueness/idempotency constraints, lifecycle indexes, and
references to existing workflow inputs. Model that state in SQLAlchemy and
ship it in an Alembic revision. Keep workload result tables and scientific
data outside the pipeline schema. See the [bulletin workload contract](future-workloads.md)
for the planned example and its readiness dependencies.

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
