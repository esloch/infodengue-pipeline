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

The analysis worker needs host access to the OCI runtime and must run outside
the normal pipeline containers:

```sh
uv run makim analysis.worker
uv run makim analysis.health
```

Run one worker process by default. The scientific `--cores` option controls
state-level work inside the analysis image and is independent of Celery
concurrency. Do not mount the Docker socket into a worker container. Keep
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` unless a mutating analysis is
explicitly approved and serialized for its database target.

## Guarded real staging E2E

`containers/compose-staging.yaml` defines a separate pipeline PostgreSQL and
RabbitMQ, three queue workers, Beat, the external staging integration
network, the shared incoming bind mount, and the shared analysis output path.
It connects to staging MinIO and AlertaDengue through that existing network;
it does not mount sibling source trees or a Docker socket. Populate a private
`.env.staging` from the `../.env.staging.example` template, replace all
placeholders, and set `PIPELINE_WORKER_ENV_FILE=../.env.staging`. The
pipeline must be the only materializer for the selected staging test prefix;
disable the existing mirror for that prefix before enabling this ingress
worker. Point the analysis OCI `--network` setting at the existing staging
network.

The host-side analysis worker connects to the published pipeline ports and
uses the same absolute analysis output path configured in the Compose
orchestration worker. Run it with staging-specific environment overrides for
the host PostgreSQL and RabbitMQ endpoints. The analysis container itself is
started by that host process on the configured staging Docker network.

The validator refuses to run unless
`INFODENGUE_PIPELINE_ENVIRONMENT=staging`, the dedicated AlertaDengue source
database is configured, and analysis loading is disabled. Supply a safe
fixture object key explicitly:

```sh
uv run makim staging.validate-e2e \
  --object-key 'approved/staging-fixture.csv' \
  --timeout-seconds 1800
```

Use a test object explicitly approved for staging. The path must include the
real staging MinIO, shared incoming directory, AlertaDengue watcher and
stage/merge, pipeline PostgreSQL and RabbitMQ, and pinned
AlertaDengueAnalise OCI image. The validator waits with bounded, named stages
for materialization, completed run correlation, readiness, job dispatch and
success, then checks current-run artifacts. It never deletes unrelated data
or outputs. Do not point staging settings at production.

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
