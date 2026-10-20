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
`.env.staging.analysis` files from the tracked templates. Only
`worker-analysis` receives `PIPELINE_ANALYSIS_ENV_FILE` and the analysis DB
credential. The core file contains the read-only source database URL.
Provision dedicated MinIO service credentials with read/list access limited
to the selected bucket/prefix; credential provisioning remains a deployment
concern.

Staging MinIO uses `minio:9000` over HTTP, bucket `sinan-infodengue`, and
pipeline prefix `approved-fixtures/`. Set the AlertaDengue deployment value
`MINIO_MATERIALIZER_EXCLUDE=approved-fixtures/*` so its legacy full-bucket
mirror excludes this pipeline-owned prefix. If unset or empty, the existing
mirror command remains unchanged.

`STAGING_ANALYSIS_OUTPUT_HOST_PATH` maps on the VPS to
`/var/lib/pipeline/analysis-output` in `worker-analysis` and `validator`.
`worker-orchestration` stores deterministic output path strings and does not
mount the artifacts. Keep `worker-ingestion` at one replica and
`--concurrency=1`; horizontal materialization requires durable claim leases.

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

The validator refuses to run unless
`INFODENGUE_PIPELINE_ENVIRONMENT=staging`, the read-only AlertaDengue source
database is configured, and analysis loading is disabled. Supply a safe
fixture object key explicitly:

```sh
uv run makim staging.validate-e2e \
  --object-key 'approved-fixtures/staging-fixture.csv' \
  --timeout-seconds 1800
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

This command being implemented does not mean staging was run or passed. Report
staging E2E as `NOT EXECUTED` until it has been run against those real
staging services.

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
