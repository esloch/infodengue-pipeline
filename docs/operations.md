# Operations

## Local services and workers

Local PostgreSQL, RabbitMQ, and MinIO use development-only credentials from
`.envs/.env.example`:

```sh
uv run makim containers.up
uv run makim db.upgrade
```

Celery Beat runs configured polling/recovery tasks for MinIO discovery,
completed-run bridging, outbox publication, and READY workflow dispatch. The
four queues remain separate: `pipeline.ingestion`, `pipeline.events`,
`pipeline.orchestration`, and `pipeline.analysis`.

The production-profile foreground SINAN watcher is supervised by the systemd
user service described in [production](production.md#sinan-watcher-supervision).

## Inspect workers, queues, and persisted work

### Production-profile status

On the staging host, with the already running production-profile stack and its
private `.envs/.env.prod` file, run:

```sh
uv run makim prod.status
```

This executes the report inside the existing `worker-events` container. It
does not start services, dispatch Celery work, retry jobs, or change rows. The
`prod.status` task and Python module must be included in the deployed
`PIPELINE_IMAGE`; build and deploy the updated worker image, then recreate the
production-profile workers through the normal deployment process before this
command is available. Running containers keep their existing image and cannot
gain the command from a host checkout alone. The
database transaction defaults to read-only and has a five-second statement
timeout. The report shows counts and up to 20 recent rows per category; it
omits event payloads, connection settings, and recorded exception text. A
query or connection error exits nonzero with diagnostics suppressed. Counts
are current at query time; recent rows are bounded snapshots, not a live
worker or RabbitMQ health check.

Read a failed ingress `object_id` with its bucket/object identity and SHA-256
to correlate logs and retained source data. `MATERIALIZED` means the file was
placed in the local incoming tree; it does not establish that AlertaDengue
finished the run. Outbox pending events have not been confirmed published.
Workflow and job states live in PostgreSQL and remain authoritative when
broker messages are missing.

### Explicit analysis retry

Find a failed candidate with `uv run makim prod.status`, then inspect its
recorded row and output directory through the approved production-profile
access path. The row records `last_error`, `exit_code`, `attempts`, `load`, and
`output_dir`; `pipeline-runner.log` and validated scientific outputs are in
that directory. Treat logs and error text as potentially sensitive. Check
whether the failed runner applied any scientific database writes before
retrying. In particular, never retry an interrupted `load=true` execution
until an operator has reconciled its database side effects.

Only an operator may invoke the existing task, using the UUID from the row:

```sh
docker compose --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml exec -T worker-events celery -A \
  infodengue_pipeline.messaging.celery:celery_app call \
  pipeline.analysis.retry --args='["<job-uuid>"]'
```

Then inspect `prod.status` again and verify the same `job_id` is queued, its
attempt count changes only when execution is claimed, and its new output path
uses `attempt-N`. The task returns without dispatching if the job is no longer
`FAILED`, its workflow inputs are stale, or it is no longer the next eligible
week. It does not reset `RUNNING` work. A returned false result requires
operator investigation; do not edit rows to force eligibility.

Run Compose commands from the repository root and select the deployment's
Compose file and private `--env-file`. For development, inspect running
services and recent worker logs with:

```sh
docker compose --env-file .envs/.env.example -f containers/compose.yaml ps
docker compose --env-file .envs/.env.example -f containers/compose.yaml \
  logs --since 15m worker-ingestion worker-events worker-orchestration \
  worker-analysis worker-beat
docker compose --env-file .envs/.env.example -f containers/compose.yaml \
  exec worker-orchestration celery -A \
  infodengue_pipeline.messaging.celery:celery_app inspect active_queues
```

The worker health checks use Celery `inspect ping`; `active_queues` confirms
which queues a worker consumes. RabbitMQ management is exposed on loopback
port 15673 in development, or its CLI can show queue depth:

```sh
docker compose --env-file .envs/.env.example -f containers/compose.yaml \
  exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

Use read-only SQL to inspect durable state. Connect with the private pipeline
database role and avoid selecting credential-bearing configuration or dumping
event payloads into shared logs. The following query shape shows recent ingress
state and persisted jobs:

```sql
SELECT object_id, object_key, status, sha256, run_id, last_error, updated_at
FROM pipeline.ingress_object
ORDER BY updated_at DESC
LIMIT 50;

SELECT workflow_id, coverage, epiweek, status, queued_fingerprint, updated_at
FROM pipeline.workflow
ORDER BY updated_at DESC
LIMIT 50;

SELECT job_id, workflow_id, input_fingerprint, status, attempts, load,
       exit_code, last_error, output_dir, updated_at
FROM pipeline.analysis_job
ORDER BY updated_at DESC
LIMIT 50;

SELECT status, count(*)
FROM pipeline.outbox_event
GROUP BY status
ORDER BY status;
```

Use `object_id`/`sha256`, `run_id`, `event_id`, `workflow_id` and
`input_fingerprint`, then `job_id` to follow one delivery through the system.
An ingress row's `run_id` may be empty until correlation completes. Retained
outbox payloads are replayed to reconstruct later candidate-week inputs; do
not delete published rows as routine cleanup.

For an existing staging host, create `.envs/` and copy the private root files
without overwriting anything:

```sh
mkdir -p .envs
cp -pn .env.staging .envs/.env.staging
cp -pn .env.staging.analysis .envs/.env.staging.analysis
cp -pn .env.staging.e2e .envs/.env.staging.e2e
chmod 600 .envs/.env.staging .envs/.env.staging.analysis \
  .envs/.env.staging.e2e
```

Update the three `PIPELINE_*_ENV_FILE` values in `.envs/.env.staging` to
`../.envs/.env.staging`, `../.envs/.env.staging.analysis`, and
`../.envs/.env.staging.e2e`. Validate from the repository root with:

```sh
docker compose --env-file .envs/.env.staging \
  -f containers/compose-staging.yaml config --quiet
```

Keep the existing root files until the operator has confirmed the
configuration and their backup policy permits removal. This is a file-path
migration only; it does not require restarting services.

Compose starts `worker-analysis` on `pipeline.analysis` with Celery
concurrency one. Its image extends the AlertaDengueAnalise image with the
installed pipeline package and runs Makim as a child process. The scientific
`--cores` option controls state-level work independently of Celery concurrency.
Keep `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` unless a mutating analysis is
explicitly approved and serialized for its database target.

Set `INFODENGUE_PIPELINE_ANALYSIS_INITIAL_EPIWEEK` explicitly to the first
eligible scientific week. Choose a week operators have verified is pending;
the pipeline does not infer or backfill historical weeks. Set the same value
in the core worker environment file used by `worker-events` and
`worker-orchestration`, and in the analysis worker environment file used by
`worker-analysis`. A mismatch causes the analysis worker to leave dispatched
jobs queued because its candidate does not match the coordinator's week.
After a successful `load=true` job, the next candidate is its following
epidemiological week. A successful `load=false` job generates artifacts but
does not advance this persisted-data cursor. Delivery completions can satisfy
the candidate only when their delivery epiweek is at least the candidate. The
periodic analysis dispatcher reevaluates retained completion events after each
successful job, allowing it to proceed through multiple pending weeks from
one delivery.

Do not purge published `pipeline.outbox_event` rows. Reconciliation replays
published completion payloads to select inputs for later analysis weeks. The
repository has no outbox cleanup task; external deletion would make those
deliveries unavailable for future candidate readiness and input fingerprints.

## Guarded real staging E2E

This section covers the staging E2E flow. The separate
[production-profile deployment](production.md) defines a staging-host rehearsal
with `environment=prod` and `analysis_load=false`; it has no staging E2E
runner or validator.

`containers/compose-staging.yaml` defines pipeline PostgreSQL and RabbitMQ,
queue workers, Beat, the external staging integration network, the shared
incoming bind mount, and shared analysis artifacts. It connects to the
existing AlertaDengue MinIO service on that network; it does not mount sibling
source trees or a Docker socket. Populate private `.envs/.env.staging` and
`.envs/.env.staging.analysis` files from the tracked templates. Provision a
private `.envs/.env.staging.e2e` from its template for one-shot fixture
submissions. Only
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
logical job is reused, and the retry uses the currently configured
`analysis_cores` value. Each retry writes to a new attempt output directory;
earlier attempt artifacts remain available. A retry does not resume an
interrupted `RUNNING` job or automatically recover a lost worker. Inspect the
output directory and database effects first, then make an operator decision
before any explicit retry. This is particularly important for any explicit
`load=true` execution. Mutating analyses hold a PostgreSQL advisory
lock scoped to the target database for the full scientific process, waiting
at most `INFODENGUE_PIPELINE_ANALYSIS_LOAD_LOCK_TIMEOUT_SECONDS` (default 900
seconds). Read-only `load=false` jobs do not acquire this lock.

After reviewing the persisted row, output directory, structured logs, and
scientific database effects, an operator may explicitly invoke the retry task
from a worker container with the job UUID:

```sh
docker compose --env-file .envs/.env.example -f containers/compose.yaml \
  exec worker-analysis celery -A \
  infodengue_pipeline.messaging.celery:celery_app call \
  pipeline.analysis.retry --args='["<job-uuid>"]'
```

Replace `<job-uuid>` with the reviewed stable UUID. This command changes the
job from FAILED to QUEUED when it is still the current candidate and then
dispatches it. It does not repair RUNNING jobs or make an interrupted
scientific execution safe to repeat.

The contiguous cursor remains at a failed candidate, so an explicit retry
reuses that candidate's week and current input fingerprint. Retrying requires
the same configured initial week in both worker environment files.

Inspect structured logs using `object_id`, `sha256`, `run_id`, `event_id`,
`coverage`, `epiweek`, `input_fingerprint`, and `job_id`. Secrets and database
passwords must not be logged.

## Staging replacement and recovery validation

Use only staging credentials, a dedicated staging MinIO prefix, the staging
pipeline database, and the staging AlertaDengue watcher. Before enabling the
pipeline scanner, stop or disable the legacy MinIO synchronization for that
prefix. Also ensure no other pipeline deployment scans it. Keep the existing
watcher running; do not start another watcher or point this test at production.

1. Record the selected key, its initial ETag/version/size, and its SHA-256.
   Upload one supported dengue, chikungunya, or Zika CSV/DBF to the selected
   prefix. Wait for one `pipeline.ingress_object` row with `materialized` status
   and the matching SHA-256, then for one completed AlertaDengue
   `ingestion.run` with that SHA-256. Check the incoming file is moved into
   canonical storage by the watcher.
2. Run another scanner cycle without changing the object. Confirm the row
   count and completed run count do not increase and the file does not reappear
   in incoming.
3. Replace the *same key* with different valid content. Verify its current
   ETag or version and SHA-256 changed. Wait for a second ingress row with the
   new SHA-256 and one new completed ingestion run. Confirm the new canonical
   result and that the old ingress row and run remain in history.
4. Upload the same bytes again under that key with a changed ETag or version.
   Verify the new ingress row is `skipped`, has the existing SHA-256, and does
   not create an incoming file or another ingestion run.
5. To test recovery, interrupt only the staging scanner during a large test
   download, then restart it. Verify no `.part` file or incomplete CSV/DBF
   appeared in incoming. A later scan should materialize the complete object
   once; inspect `last_error` and structured ingress logs if it fails.

The staging and incoming directories must be siblings on the same filesystem.
The staging/prod Compose ingress mount exposes the parent of incoming to the
worker for this atomic rename. A cross-filesystem configuration fails the scan
before downloading; an incoming directory mounted separately is also rejected.
The parent must allow the worker's runtime UID to create
the private staging directory. The coordinator keeps that directory at mode
`0700` and publishes files at mode `0640`; the host watcher must run as the
same UID or have the worker's GID and directory traversal access to read them.
Do not clean the staging directory while an ingress record is in `discovered`
status with a SHA-256: that staged file is the recovery marker.

If validation fails, stop the pipeline scanner and Beat before re-enabling
legacy synchronization for the prefix. Let the existing watcher finish any
file already published to incoming; preserve ingress rows, staged files, and
ingestion history for diagnosis. Restore legacy synchronization only after
confirming the new materializer is inactive for that prefix. Reverse this
sequence for another staging attempt: disable legacy synchronization first,
then enable the pipeline scanner. Never run both publishers for one prefix.

## PostgreSQL backup and isolated restore

Pipeline PostgreSQL is authoritative for ingress materialization identities,
outbox delivery, workflow state, and analysis jobs. A custom-format `pg_dump`
creates a consistent snapshot while writes continue. Run from the deployment
host with the private profile configured for the intended staging deployment:

```sh
set -euo pipefail
umask 077
backup_dir=/var/backups/infodengue-pipeline
install -d -m 0700 "$backup_dir"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
docker compose --env-file .envs/.env.prod -f containers/compose-prod.yaml \
  exec -T postgres pg_dump -U pipeline_prod -d pipeline_prod -Fc \
  > "$backup_dir/pipeline-$stamp.dump"
chmod 0600 "$backup_dir/pipeline-$stamp.dump"
sha256sum "$backup_dir/pipeline-$stamp.dump" \
  > "$backup_dir/pipeline-$stamp.dump.sha256"
```

This procedure was reviewed but not run against a deployment in this change.
A nonzero dump exit, empty output, or checksum mismatch means the backup is
unusable; resolve the cause and take another dump. Copy verified dumps and
checksums to access-controlled storage on a
separate failure domain. Keep mode `0600` and directory mode `0700`. Suggested
initial policy: retain daily backups for 30 days and monthly backups for 12
months, subject to the organization's approved retention and recovery-point
objectives. Prune only verified backups under that policy; cleanup is manual.

Restore acceptance must use a separately provisioned, disposable PostgreSQL
target. Do not reuse a Compose project, database volume, database name,
endpoint, broker, worker, or scientific credential from a running deployment.
The following example creates a unique internal Docker network and a fresh
anonymous database volume. It has no broker or worker and uses only a loopback
host port. Substitute a verified backup path and unique suffix; first check
that the selected port and names are unused.

```sh
set -euo pipefail
backup_file=/var/backups/infodengue-pipeline/pipeline-REPLACE_WITH_VERIFIED_TIMESTAMP.dump
backup_dir=$(dirname "$backup_file")
backup_name=$(basename "$backup_file")
restore_name="pipeline-restore-$(date -u +%Y%m%dT%H%M%SZ)"
restore_network="$restore_name-net"
restore_container="$restore_name-postgres"
restore_port=55439
(
  cd "$backup_dir"
  sha256sum --check "$backup_name.sha256"
)
docker network create --internal "$restore_network"
cleanup_restore() {
  docker rm -fv "$restore_container" >/dev/null 2>&1 || true
  docker network rm "$restore_network" >/dev/null 2>&1 || true
}
trap cleanup_restore EXIT
docker run -d --name "$restore_container" --network "$restore_network" \
  -p "127.0.0.1:$restore_port:5432" \
  -e POSTGRES_DB=pipeline_restore -e POSTGRES_USER=restore \
  -e POSTGRES_PASSWORD=restore-only-local \
  postgres:16-alpine
docker cp "$backup_file" "$restore_container:/tmp/pipeline.dump"
ready=0
for attempt in $(seq 1 30); do
  if docker exec "$restore_container" pg_isready -U restore \
    -d pipeline_restore; then ready=1; break; fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo 'Isolated PostgreSQL did not become ready' >&2
  exit 1
fi
docker exec "$restore_container" pg_restore --exit-on-error --no-owner --no-acl \
  -U restore -d pipeline_restore /tmp/pipeline.dump
docker exec "$restore_container" psql -U restore -d pipeline_restore -c \
  "SELECT version_num FROM alembic_version"
docker exec "$restore_container" psql -U restore -d pipeline_restore -c \
  "SELECT table_name FROM information_schema.tables \
   WHERE table_schema = 'pipeline' ORDER BY table_name"
docker exec "$restore_container" psql -U restore -d pipeline_restore -c \
  "SELECT 'ingress', status, count(*) FROM pipeline.ingress_object \
   GROUP BY status UNION ALL SELECT 'workflow', status, count(*) \
   FROM pipeline.workflow GROUP BY status UNION ALL \
   SELECT 'analysis_job', status, count(*) FROM pipeline.analysis_job \
   GROUP BY status UNION ALL SELECT 'outbox_event', status, count(*) \
   FROM pipeline.outbox_event GROUP BY status"
INFODENGUE_PIPELINE_DATABASE_URL="postgresql+psycopg://restore:restore-only-local@127.0.0.1:$restore_port/pipeline_restore" \
  uv run python -m infodengue_pipeline.production.status
```

The internal network prevents the restore target from initiating external
connections. Docker publishes the PostgreSQL port on host loopback so the
validation client can connect from the deployment host. No PostgreSQL client
binaries are required on the host: `pg_restore`, `psql`, and `pg_isready` run
inside the disposable PostgreSQL image; the host status command uses the
project's Python driver. Confirm host connectivity using the status command
before accepting the restore.

Compare `alembic_version` with `uv run alembic heads`, confirm the expected
pipeline tables, and compare status counts and representative
`object_id`/`sha256`, `run_id`, `workflow_id`, `job_id` and `event_id` values
with the backup-time inventory. Investigate any mismatch before accepting the
restore. Keep the isolated target available until the comparison is complete;
then remove only the uniquely named disposable container and its internal
network. The `EXIT` trap removes only the newly named disposable container and
network after success or failure; keep the trap in place. Never restore over
an existing database. A successful dump alone is not restore acceptance.

No isolated PostgreSQL restore was available or
executed here; restore validation remains pending.

## RabbitMQ recovery

RabbitMQ is the Celery transport; the production Compose profile stores broker
data in named volume `pipeline_prod_rabbitmq`, mounted at `/var/lib/rabbitmq`.
Keep the volume when restarting the broker. A restart preserves persistent
broker data, but unacknowledged or not durably accepted messages can be
redelivered or lost according to broker and task acknowledgement timing. Do
not purge queues or remove the volume as recovery.

PostgreSQL remains the authoritative work ledger. Pending outbox rows can be
published by the existing outbox poller. Queued analysis jobs are redispatched
by `pipeline.analysis.dispatch_ready` when the configured beat/dispatch path
runs; the dispatcher republishes stale queued deliveries. A job left
`RUNNING` after worker loss is not automatically reset and needs operator
review of artifacts and scientific database effects. A `FAILED` job requires
the explicit retry procedure above. Do not assume every lost message is
recovered until PostgreSQL state and worker logs are checked.

## Ingress and scientific artifact retention

Original SINAN objects in MinIO are source-owned inputs. Retain them according
to the upstream data owner's policy and do not delete them as pipeline
cleanup. For operational planning, require at least 90 days of source-object
availability where the upstream owner permits it; a longer upstream policy
takes precedence. This is a retention expectation, not a MinIO lifecycle
change. Pipeline PostgreSQL owns each object identity and materialization
state. Downloaded files under the configured incoming root are staging copies
handed to AlertaDengue ingestion; they are not canonical SINAN storage. The
AlertaDengue watcher may move a staged file into its canonical storage tree;
that moved file is no longer a local incoming copy. This pipeline does not
move it back or apply retention to the watcher's canonical copy; follow
AlertaDengue and StorageBox retention rules for that data. Keep
successful local staged downloads for 30 days after `ingested`; retain
`discovered`, `materialized`, and `failed` files until an operator resolves
the state and for 30 days after the resulting terminal state.
Canonical storage movement and deduplication remain owned by AlertaDengue and
StorageBox. Keep MinIO lifecycle configuration unchanged. For investigation
and supported replay, retain source objects and local staged files until the
corresponding ingestion outcome and recovery window have been reviewed; this
pipeline does not delete either automatically.

Analysis outputs live below `INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT`, in a
job-ID directory. Explicit retries use `attempt-N` subdirectories and keep
prior attempts. Retain `pipeline-runner.log`, required generated `alertas`
artifacts, `sql/output_dengue.sql`, `sql/output_chik.sql`, and map images when
present. Retain failed and successful outputs for at least the 30-day
investigation window suggested for daily database backups, or longer while an
incident or scientific review is open. A directory is only a cleanup
candidate after matching it to a terminal-success job and its retention
deadline. Queued, running, failed, or unresolved jobs and all their attempts
are protected. There is no automated artifact cleanup; future automation
needs separate review and validation.

## Operational acceptance checklist

- `prod.status` runs against the intended profile and reports plausible state.
- Failed jobs were reviewed for error context, artifacts, and possible
  scientific database side effects before any explicit retry.
- A verified PostgreSQL backup exists in protected separate storage, and an
  isolated restore has been rehearsed and accepted.
- Broker restart/recovery preserved named volumes; queued, running, failed,
  and pending outbox states were reconciled against PostgreSQL.
- MinIO source retention, staged download handling, and canonical ownership
  boundaries are understood; no lifecycle changes were made by this pipeline.
- Analysis outputs for unresolved work remain retained and accessible.
