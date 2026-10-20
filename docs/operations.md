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

`containers/compose-staging.yaml` defines a separate pipeline PostgreSQL and
RabbitMQ, four queue workers, Beat, the external staging integration
network, the shared incoming bind mount, and the shared analysis output path.
It connects to staging MinIO and AlertaDengue through that existing network;
it does not mount sibling source trees or a Docker socket. Populate a private
`.env.staging` from the `../.env.staging.example` template, replace all
placeholders, and set `PIPELINE_WORKER_ENV_FILE=../.env.staging`. The
pipeline must be the only materializer for the selected staging test prefix;
disable the existing mirror for that prefix before enabling this ingress
worker. Build the analysis worker image from the reviewed AlertaDengueAnalise
base image and set `PIPELINE_ANALYSIS_IMAGE` to that image. Keep
`INFODENGUE_PIPELINE_ANALYSIS_IMAGE` equal to the same tag or digest so
analysis job rows record the executing image.
The worker joins the pipeline and staging integration networks, and mounts
the exact output directory used by the validator. Its AlertaDengue database
URL must point to the staging database through the integration network.
Pipeline PostgreSQL and RabbitMQ use the unique `pipeline-postgres` and
`pipeline-rabbitmq` aliases. Set these hostnames in the private worker env
file so services on both networks reach the pipeline's own broker and database.

The validator refuses to run unless
`INFODENGUE_PIPELINE_ENVIRONMENT=staging`, the dedicated AlertaDengue source
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
explicit `load=true` execution.

Inspect structured logs using `object_id`, `sha256`, `run_id`, `event_id`,
`coverage`, `epiweek`, `input_fingerprint`, and `job_id`. Secrets and database
passwords must not be logged.
