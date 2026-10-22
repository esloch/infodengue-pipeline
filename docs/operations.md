# Operations

## Local services and workers

Local PostgreSQL, RabbitMQ, and MinIO use development-only credentials from
`.env.example`:

```sh
uv run makim containers.up
uv run makim db.upgrade
```

Celery Beat runs configured polling/recovery tasks for MinIO discovery,
completed-run bridging, outbox publication, and READY workflow dispatch. The
four queues remain separate: `pipeline.ingestion`, `pipeline.events`,
`pipeline.orchestration`, and `pipeline.analysis`.

Compose starts `worker-analysis` on `pipeline.analysis` with Celery
concurrency one. Its image extends the AlertaDengueAnalise image with the
installed pipeline package and runs Makim as a child process. The scientific
`--cores` option controls state-level work independently of Celery concurrency.
Keep `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` unless a mutating analysis is
explicitly approved and serialized for its database target.

## Guarded real staging E2E

`containers/compose-staging.yaml` defines pipeline PostgreSQL and RabbitMQ,
queue workers, Beat, the external staging integration network, the shared
incoming bind mount, and shared analysis artifacts. It connects to the
existing AlertaDengue MinIO service on that network; it does not mount sibling
source trees or a Docker socket. Populate private `.env.staging` and
`.env.staging.analysis` files from the tracked templates. Provision a private
`.env.staging.e2e` from its template for one-shot fixture submissions. Only
`worker-analysis` receives `PIPELINE_ANALYSIS_ENV_FILE` and the analysis DB
credential `INFODENGUE_PIPELINE_ANALYSIS_DATABASE_URL`. The core file contains
the read-only `INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL`.
Provision the runtime MinIO account with read/list access limited to the
configured bucket and prefix. Provision a separate E2E writer account with
only object upload and exact-object stat permissions under that prefix.
The endpoint, bucket, secure mode, and prefix come from private staging
configuration. Credential and policy provisioning are deployment concerns.
Configure the AlertaDengue legacy mirror to exclude the configured
pipeline-owned prefix.

`STAGING_ANALYSIS_OUTPUT_HOST_PATH` is the host-side bind source. On staging
its value is `/opt/data/staging/analysis-output`. `worker-analysis` and
`validator` mount it at `/var/lib/pipeline/analysis-output`.
`worker-orchestration` does not mount the directory; it only receives the
fixed logical `INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT` value. Compose
sets that value to `/var/lib/pipeline/analysis-output` in all three services,
overriding any private env file drift. Keep `worker-ingestion` at one replica
and `--concurrency=1`; horizontal materialization requires durable claim leases.

Build the analysis worker image from the reviewed AlertaDengueAnalise
base image and set `PIPELINE_ANALYSIS_IMAGE` to that image. Keep
`INFODENGUE_PIPELINE_ANALYSIS_IMAGE` equal to the same tag or digest so
analysis job rows record the executing image.
The analysis database URL points to the scientific target through the
integration network. The completion source URL is only used to read
`ingestion.run`.
Pipeline PostgreSQL and RabbitMQ use the unique `pipeline-postgres` and
`pipeline-rabbitmq` aliases. Set these hostnames in the private worker env
file so services on both networks reach the pipeline's own broker and database.

The preferred clean acceptance command is:

```sh
uv run makim staging.e2e \
  --fixture '/path/to/fresh-fixture.dbf'
```

The fixture must be CSV or DBF and its SHA256 must not appear in any
`ingestion.run` row. The command streams its hash, generates an object key under
the configured prefix, uploads with dedicated writer credentials, then follows
the existing validator. `analysis_load=false` is mandatory. A clean PASS
requires one analysis attempt. Failures preserve the uploaded object and all
pipeline state for investigation; the runner does not repair or retry them.

For explicit diagnostic validation, the lower-level validator refuses to run
unless `INFODENGUE_PIPELINE_ENVIRONMENT=staging`, the read-only AlertaDengue source
database is configured, and analysis loading is disabled. Supply a safe
fixture object key explicitly:

```sh
uv run makim staging.validate-e2e \
  --object-key 'approved-fixtures/staging-fixture.csv' \
  --timeout-seconds 7200
```

This runs the finite `validator` Compose service on the staging networks, so
Docker-only hostnames resolve. It mounts the shared analysis output path to
validate artifacts from the exact job.

Use a test object explicitly approved for staging. The path must include the
real staging MinIO, shared incoming directory, AlertaDengue watcher and
stage/merge, pipeline PostgreSQL and RabbitMQ, and pinned
AlertaDengueAnalise OCI image. The validator waits with bounded, named stages
for materialization, completed run correlation, readiness, job dispatch and
success, then checks current-run artifacts. It never deletes unrelated data
or outputs. Do not point staging settings at production.

The pipeline rejects ambiguous legacy disease code `A92`. Current
AlertaDengue ingestion preserves canonical CID-10 values for new runs.

A clean staging E2E validation reached SUCCESS on 2026-10-22 for coverage
MG, epiweek 202412, with `analysis_load=false`, one analysis attempt, and no
manual recovery after submission. The validator returned SUCCESS for run
`f4d52f3a-35f0-4717-a6ef-da35fd0bf1a4` and job
`0b9c429c-278d-49c6-add7-c9131e42f484`. It validated MinIO ingress, atomic
materialization,
AlertaDengue SINAN ingestion, the completed-run bridge, RabbitMQ/outbox
coordination, readiness, analysis dispatch, containerized
AlertaDengueAnalise execution, and RData and SQL artifact validation. SQL
application and map generation were not exercised because `load=false`.
This result does not establish production readiness or cutover.

## Failure and recovery

Outbox rows remain pending when RabbitMQ confirmation fails and are safely
retried by periodic publication. Event consumers are idempotent and keep
newer per-disease inputs when delayed events arrive.

Analysis jobs with status `FAILED` can be explicitly retried using the
`pipeline.analysis.retry` Celery task with the stable job UUID. The same
logical job is reused. Do not reset jobs that remain `RUNNING` after worker
loss automatically; inspect the output directory and database effects first,
then make an operator decision. This is particularly important for any
explicit `load=true` execution. Mutating analyses hold a PostgreSQL advisory
lock scoped to the target database for the full scientific process, waiting
at most `INFODENGUE_PIPELINE_ANALYSIS_LOAD_LOCK_TIMEOUT_SECONDS` (default 900
seconds). Read-only `load=false` jobs do not acquire this lock.

Inspect structured logs using `object_id`, `sha256`, `run_id`, `event_id`,
`coverage`, `epiweek`, `input_fingerprint`, and `job_id`. Secrets and database
passwords must not be logged.
