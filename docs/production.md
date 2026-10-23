# Production profile

## Scope and safety boundary

This profile defines a one-host Docker Compose deployment contract. The first
rehearsal runs on the **staging server** with
`INFODENGUE_PIPELINE_ENVIRONMENT=prod`, while **all external resources remain
staging resources**. Scientific writes remain disabled with
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`. This validates the deployment shape,
not production data or production readiness. Do not use real production
credentials, point either source or analysis database URL at the real production
database, or point MinIO at real production ingress during the rehearsal.

AlertaDengue continues to own SINAN inspection, canonical movement, manifest,
stage, merge, and rollback. The pipeline owns ingress coordination, events,
outbox, readiness, and scheduling. AlertaDengueAnalise owns the scientific
R/INLA, RData, SQL, and maps workflow.

## Configuration files

Copy the tracked placeholders, replace every placeholder with privately
provisioned rehearsal values, and restrict access:

```sh
cp .env.prod.example .env.prod
cp .env.prod.analysis.example .env.prod.analysis
chmod 600 .env.prod .env.prod.analysis
```

The copied files are untracked. The core file supplies the read-only
AlertaDengue `ingestion.run` source role and a runtime MinIO service account
with read/list access limited to the configured bucket and prefix. Only
`worker-analysis` receives `.env.prod.analysis` and its distinct, read-only
analysis database role. The analysis role stays read-only while `load=false`.
Keep the pipeline PostgreSQL, RabbitMQ, source DB, analysis DB, and MinIO
credentials separate.

## Production topology

The project `infodengue-pipeline-prod` has PostgreSQL, RabbitMQ, a finite
`migrate` job, four dedicated queue workers, and Celery Beat. PostgreSQL and
RabbitMQ have production-profile-specific persistent volumes and loopback-only
ports. MinIO is external. The ingestion worker and analysis worker each have
Celery concurrency one; keep ingestion at one replica. Ingestion, events,
orchestration, and analysis join the configured integration network. Beat
remains on the default network. No worker mounts a sibling checkout or Docker
socket.

## Staging-host rehearsal

Use these non-secret values in the private rehearsal configuration, alongside
staging-only credentials and staging-only database and MinIO URLs:

```text
PROD_INTEGRATION_NETWORK=infodengue-staging_infodengue
PROD_SINAN_INCOMING_HOST_PATH=/opt/data/staging/sinan/incoming
PROD_ANALYSIS_OUTPUT_HOST_PATH=/opt/data/staging/prod-analysis-output
PIPELINE_POSTGRES_HOST_PORT=5436
PIPELINE_RABBITMQ_HOST_PORT=5675
INFODENGUE_PIPELINE_ENVIRONMENT=prod
INFODENGUE_PIPELINE_ANALYSIS_LOAD=false
```

Prepare `/opt/data/staging/prod-analysis-output` as a dedicated directory
writable by the analysis container's runtime user. Assign permissions for that
user according to the host's deployment policy; do not assume root ownership.
Do not reuse `/opt/data/staging/analysis-output`, which belongs to the staging
profile. The incoming bind points to the staging AlertaDengue incoming path;
never use the real production incoming path for this rehearsal.

## Image identity

Set `PIPELINE_IMAGE` and `PIPELINE_ANALYSIS_IMAGE` to reviewed, immutable image
digests. Set `INFODENGUE_PIPELINE_ANALYSIS_IMAGE` to exactly the same value as
`PIPELINE_ANALYSIS_IMAGE` in both private env files, so persisted job metadata
records the executing image. Image publishing and release automation are outside
this profile.

## Rendering the configuration

The static check uses tracked placeholders and requires no real secrets or
external services:

```sh
uv run makim containers.prod-config
```

Before starting a privately configured rehearsal, render that configuration:

```sh
docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  config --quiet
```

## Starting the production profile

Run these commands only after the private rehearsal configuration and external
staging resources have been reviewed:

```sh
docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  up -d postgres rabbitmq

docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  run --rm migrate

docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  up -d \
  worker-ingestion \
  worker-events \
  worker-orchestration \
  worker-analysis \
  worker-beat

docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  ps
```

## Migrations

`migrate` runs `alembic upgrade head` against the pipeline-owned PostgreSQL
database. It does not run Django migrations, migrate the AlertaDengue database,
or apply generated scientific SQL. Migration is separate from analysis
`load=true`, which remains disabled for this issue.

## Stopping services

```sh
docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  down
```

Do not use `docker compose down -v` during normal operation: the PostgreSQL and
RabbitMQ volumes hold persistent state.

## What is not validated yet

Rendering this Compose file does not prove watcher supervision, production
runtime connectivity, `load=true`, scientific SQL application, map generation,
production backup/restore, retention, shadow execution, or production cutover.
The staging E2E result described in [operations](operations.md) has a different
scope and used the staging profile.

## Next production gates

- #8: watcher supervision.
- #9: production-profile deployment validation.
- #10: `load=true` staging validation.
- #11: operations, backup/restore, and retention.
- #12: shadow validation and cutover.
