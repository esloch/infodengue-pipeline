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
mkdir -p .envs
cp .envs/.env.prod.example .envs/.env.prod
cp .envs/.env.prod.analysis.example .envs/.env.prod.analysis
chmod 600 .envs/.env.prod .envs/.env.prod.analysis
```

The copied files are untracked. The core file supplies the read-only
AlertaDengue `ingestion.run` source role and a runtime MinIO service account
with read/list access limited to the configured bucket and prefix. Only
`worker-analysis` receives `.envs/.env.prod.analysis` and its distinct, read-only
analysis database role. The analysis role stays read-only while `load=false`.
Keep the pipeline PostgreSQL, RabbitMQ, source DB, analysis DB, and MinIO
credentials separate.

## Production host storage

On Mosqserver, keep host persistence under `/Storage/infodengue_data`:

```text
INFODENGUE_SINAN_INCOMING_HOST_PATH=/Storage/infodengue_data/sinan/incoming
INFODENGUE_ANALYSIS_OUTPUT_HOST_PATH=/Storage/infodengue_data/pipeline/analysis-output
INFODENGUE_BULLETIN_OUTPUT_HOST_PATH=/Storage/infodengue_data/pipeline/bulletins
```

These are **host bind mount paths**, configured in the private
`.envs/.env.prod` file used for Compose interpolation. Keep the worker runtime
settings `INFODENGUE_PIPELINE_INCOMING_ROOT=/var/lib/pipeline/incoming`
and `INFODENGUE_PIPELINE_ANALYSIS_OUTPUT_ROOT=/var/lib/pipeline/analysis-output`
unchanged: they refer to the directories **inside containers**. The
`worker-ingestion` bind mounts the parent of the host incoming path at
`/var/lib/pipeline`; analysis and orchestration workers share the analysis
output bind mount, and the optional bulletin worker mounts it read-only.

When migrating an existing deployment, do not change the bind mount before
copying and verifying all existing job directories, including incomplete runs.
Ensure permissions match the analysis worker's runtime UID/GID. Check the
rendered Compose configuration with `docker compose --env-file
.envs/.env.prod -f containers/compose-prod.yaml config --quiet`. Recreate
only explicitly authorized services during a maintenance window; changing a
host `.env` file does not change an existing container's mounts. Retain the
old `/opt/data/prod/pipeline-analysis-output` until the new mounts and job
artifacts have been verified. Do not start paused analysis, orchestration, or
Beat workers as part of a storage migration. Keep the production analysis
maximum epiweek authorization gate in effect.

## Production topology

The project `infodengue-pipeline-prod` has PostgreSQL, RabbitMQ, a finite
`migrate` job, four dedicated queue workers, and Celery Beat. PostgreSQL and
RabbitMQ have production-profile-specific persistent volumes and loopback-only
ports. MinIO is external. The ingestion worker and analysis worker each have
Celery concurrency one; keep ingestion at one replica. Ingestion, events,
orchestration, and analysis join the configured integration network. Beat
remains on the default network. No worker mounts a sibling checkout or Docker
socket.

## Retained SINAN objects at first startup

Production MinIO retains `DENGUE (3).csv` and `CHIKUNGUNYA (4).csv` in the
`sinan-infodengue` bucket root. Before the pipeline scanner runs, verify the
read-only AlertaDengue source connection and the completed `ingestion.run`
records for their known SHA-256 values:

| Object | SHA-256 | Canonical CID-10 | Delivery week |
| --- | --- | --- | --- |
| `DENGUE (3).csv` | `848de6b612bf18b51a6f8e773075cde5b67ccf50634941cb960035370075e7fd` | `A90` | `202644` |
| `CHIKUNGUNYA (4).csv` | `bb385388ea60ae0be7781ae22bb1c293aa0e72c153ff64359d60b56684081b58` | `A92.0` | `202644` |

The scanner verifies downloaded bytes by SHA-256 before the publication
step. A matching completed run is recorded as `skipped` with its upstream
`run_id`; no file is placed in incoming. Missing source access or invalid
completed-run metadata fails closed. Keep the legacy root-level MinIO
materializer stopped before enabling this scanner so only one materializer
owns the bucket root. Keep Beat stopped for the first controlled scan. With
`worker-ingestion` running, enqueue one scan and wait for it to finish:

```sh
docker compose --env-file .envs/.env.prod -f containers/compose-prod.yaml \
  exec worker-ingestion celery -A \
  infodengue_pipeline.messaging.celery:celery_app call pipeline.ingestion.scan
```

Query `pipeline.ingress_object` for both hashes and verify `skipped` status,
matching run IDs, and no new files in the watched incoming directory. Only
then enable periodic scanning. Skipped rows create no historical completion
events or scientific jobs.

## Staging-host rehearsal

### Migrating the active staging deployment's private files

The active staging deployment uses `compose-prod.yaml` with private `.env.prod`
and `.env.prod.analysis` files. From the repository root, copy them without
overwriting any existing destination:

```sh
mkdir -p .envs
cp -pn .env.prod .envs/.env.prod
cp -pn .env.prod.analysis .envs/.env.prod.analysis
```

Edit only the Compose path values in `.envs/.env.prod`:
`PIPELINE_WORKER_ENV_FILE=../.envs/.env.prod` and
`PIPELINE_ANALYSIS_ENV_FILE=../.envs/.env.prod.analysis`. Keep both files at
mode 0600. Preserve the deployed PR #22 image references exactly:
`PIPELINE_IMAGE=ghcr.io/esloch/infodengue-pipeline@sha256:ac797591770128a1e946177ffd51d9304ce853516c65ff455849f0e05f9db5e3` and
`PIPELINE_ANALYSIS_IMAGE=ghcr.io/esloch/infodengue-pipeline-analysis@sha256:1bc523ef1b42eb02828a431a8739a03a52fffcc5d5231e21fb3bb6c754124648`.
Keep `INFODENGUE_PIPELINE_ANALYSIS_IMAGE` equal to the analysis image in both
files, `INFODENGUE_PIPELINE_ANALYSIS_INITIAL_EPIWEEK=202642` in both files,
and `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` in both files. Celery Beat must
remain stopped.

Validate without recreating containers:

```sh
stat -c '%a %n' .envs/.env.prod .envs/.env.prod.analysis
docker compose --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml config --quiet
docker compose --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml ps --all worker-beat
```

The first command should show mode 600 for both files, the config command
checks the new paths without printing configuration, and `ps` only observes
Beat's current state. Do not start or recreate any services for this migration.
Keep the original root files until the new configuration is verified and the
backup policy permits their removal.

For the issue #9 manual acceptance sequence, use the
[staging-host runbook](issue9-staging-rehearsal.md). Its image-digest, consumer
isolation, prerequisite, and teardown gates must be completed before running
the full profile on staging.

Use these non-secret values in the private rehearsal configuration, alongside
staging-only credentials and staging-only database and MinIO URLs:

```text
INFODENGUE_INTEGRATION_NETWORK=infodengue-staging_infodengue
INFODENGUE_SINAN_INCOMING_HOST_PATH=/opt/data/staging/sinan/incoming
INFODENGUE_ANALYSIS_OUTPUT_HOST_PATH=/opt/data/staging/prod-analysis-output
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

The production profile has separate pipeline state, so it cannot recognize
objects already processed by another pipeline database. The rehearsal must not
consume the same MinIO prefix concurrently with another active pipeline
deployment. Before enabling `worker-ingestion` and `worker-beat`, either stop
the regular staging pipeline consuming that prefix or select a dedicated
rehearsal prefix that the regular staging pipeline does not consume. Prefer a
dedicated prefix for a later controlled rehearsal object flow. Only one
pipeline materialization flow should feed the selected rehearsal MinIO prefix
into the shared AlertaDengue incoming directory at a time.

## SINAN watcher supervision

The `worker-ingestion` container atomically materializes files into the shared
incoming directory. One host-level systemd user service supervises the existing
AlertaDengue foreground watcher, which invokes `makim ingestion.run` and owns
SINAN interpretation and ingestion behavior. The watcher stays outside Compose.
Use one shared incoming directory and one `infodengue-sinan-watcher.service`
per user manager. Repeated starts of this non-template unit do not create
another instance. Do not copy or rename the unit to watch the same path.

From this repository, install the unit and a private host configuration for
the service account:

```sh
mkdir -p ~/.config/systemd/user
mkdir -p ~/.config/infodengue
cp deploy/systemd/infodengue-sinan-watcher.service \
  ~/.config/systemd/user/infodengue-sinan-watcher.service
cp deploy/systemd/sinan-watcher.env.example \
  ~/.config/infodengue/sinan-watcher.env
chmod 600 ~/.config/infodengue/sinan-watcher.env
```

Edit only the private `~/.config/infodengue/sinan-watcher.env`. For the current
staging-host rehearsal, set:

```text
SINAN_WATCH_PATH=/opt/data/staging/sinan/incoming
SINAN_WATCH_SETTLE_TIME=5.0
```

The pipeline profile still uses `INFODENGUE_PIPELINE_ENVIRONMENT=prod`; this
watch path remains staging-only. The required env file and `ExecStartPre` check
make startup fail if the configuration or incoming directory is missing; the
service does not create that directory. Do not put database or MinIO credentials
in this file. AlertaDengue uses its existing deployment configuration for its
runtime environment.

User-level systemd does not load an interactive shell profile. In the reviewed
AlertaDengue runtime, find Makim with `command -v makim`. If it returns, for
example, `/some/path/bin/makim`, set the private file's PATH to
`PATH=/some/path/bin:/usr/local/bin:/usr/bin:/bin`. Use the existing Makim
installation. The standard unit runs foreground `makim ingestion.watch` with
the path and settle time only; recovery flags require a separate operator
decision.

Before enabling the unit, stop any watcher previously launched with
`makim ingestion.watch-start` for the same incoming directory. Once systemd
owns the watcher, do not use `ingestion.watch-start` or
`ingestion.watch-stop` for normal lifecycle operations. Systemd is the sole
process supervisor.

After reviewing the private settings, activate and inspect the service:

```sh
systemctl --user daemon-reload
systemctl --user enable --now infodengue-sinan-watcher.service
systemctl --user status infodengue-sinan-watcher.service
systemctl --user is-active infodengue-sinan-watcher.service
```

Journald owns stdout and stderr. Read historical or live logs with:

```sh
journalctl --user -u infodengue-sinan-watcher.service
journalctl --user -u infodengue-sinan-watcher.service -f
```

Use systemd for routine lifecycle operations:

```sh
systemctl --user stop infodengue-sinan-watcher.service
systemctl --user start infodengue-sinan-watcher.service
systemctl --user restart infodengue-sinan-watcher.service
systemctl --user disable --now infodengue-sinan-watcher.service
```

The service uses `Restart=on-failure` with a five-second delay; a deliberate
stop stays stopped. Systemd sends SIGTERM to the watcher main process and allows
up to one hour for shutdown. The watcher stops its observer, cancels pending
settle timers, and lets an active ingestion action finish before exiting. Pending
files remain in the incoming directory for recovery on the next startup.

The watcher holds a kernel advisory lock for its selected incoming directory, so
a second foreground watcher for that path exits instead of running concurrently.
Systemd also limits the named user service to one instance. Stop any existing
`makim ingestion.watch-start` watcher before migrating supervision to systemd;
systemd remains the normal lifecycle owner after migration.

Startup reconciliation scans supported files already present in the incoming
directory and schedules them through the same settle and action path. This closes
the restart gap for files materialized while the watcher was down and does not
enable `--include-existing`, `--requeue`, or `--allow-outdated`.
Review journald after stopping it.

Only in a controlled rehearsal while the watcher is idle and no SINAN file is
being processed, inspect its main process and validate failure recovery:

```sh
systemctl --user show infodengue-sinan-watcher.service \
  --property=MainPID --property=ActiveState --property=SubState
systemctl --user kill --signal=SIGKILL infodengue-sinan-watcher.service
sleep 6
systemctl --user is-active infodengue-sinan-watcher.service
```

The final result should be `active`. Do not run this against active ingestion.
In the same idle, controlled setting, verify the normal SIGTERM stop and start:

```sh
systemctl --user stop infodengue-sinan-watcher.service
systemctl --user is-active infodengue-sinan-watcher.service
systemctl --user start infodengue-sinan-watcher.service
journalctl --user -u infodengue-sinan-watcher.service
```

The state check after stop should report `inactive`. If the watcher must start
before the service user logs in, have the host administrator enable lingering
for that service account, for example `loginctl enable-linger <service-user>`.

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
  --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml \
  config --quiet
```

## Read-only deployment validation

On the staging host, point the private production-profile files at **staging
resources only**, keep `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` in both files,
and start the reviewed profile. Then run:

```sh
uv run makim prod.validate
```

The command inspects the rendered Compose configuration and the running worker
states, then executes finite probes inside the ingestion and analysis workers.
It checks pipeline PostgreSQL and migration revision, RabbitMQ, the runtime
MinIO bucket, the read-only AlertaDengue source connection, the analysis
database connection, the incoming bind, and analysis output permissions under
the analysis worker's user. It requires both ingestion and analysis workers to
be configured with concurrency one, reports the analysis image digest and
`analysis_load=false`, and checks all five workers are healthy. It neither
starts services nor applies migrations. The database probes execute only
`SELECT`; the MinIO probe checks bucket accessibility without uploading an
object. Filesystem checks do not create files. The analysis image must have a
complete, lowercase 64-character SHA-256 digest. Compose commands and network
probes have bounded timeouts; a timeout exits nonzero with the affected
boundary's name.

Expected successful output includes named `...: ok` checks and ends with
`production validation passed`. A failure exits nonzero and names the failed
boundary; database and Compose diagnostics are suppressed to avoid printing
credentials. The check does not prove that the source or analysis role lacks
write grants; provision and audit those roles separately.

For a bounded view of persisted operational state, use
[`prod.status`](operations.md#production-profile-status). It requires the
existing `worker-events` container and reads only pipeline PostgreSQL. Backup,
isolated restore, broker recovery, and retention procedures are documented in
the [operations runbook](operations.md#postgresql-backup-and-isolated-restore).
They have not been operationally rehearsed against deployment resources by
this change; isolated restore acceptance remains a pending operator action.

## Read-only deployment validation

On the staging host, point the private production-profile files at staging
resources only, keep `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` in both files,
and start the reviewed profile. Then run:

```sh
uv run makim prod.validate
```

The command inspects the rendered Compose configuration and running worker
states, then runs finite probes inside the ingestion and analysis workers. It
checks pipeline PostgreSQL and migration revision, RabbitMQ, MinIO, the
AlertaDengue source connection, the analysis database, and worker filesystem
access. It does not start services or apply migrations. Database probes issue
only `SELECT`; the MinIO probe does not upload an object. The validator reports
failed boundaries without printing connection diagnostics or credentials.

## Starting the production profile

These commands describe the production-profile lifecycle. Do not start the
full ingestion and orchestration stack on the staging host until the private
rehearsal resources and MinIO prefix have been reviewed. Issue #9 provides the
explicit production-profile validation gate.

```sh
docker compose \
  --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml \
  up -d

docker compose \
  --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml \
  ps --all
```

Compose waits for healthy PostgreSQL and RabbitMQ, runs `migrate` once, and
starts the workers after it exits successfully. Expected state: PostgreSQL,
RabbitMQ, and all five workers are running and healthy; `migrate` is exited
successfully. The migration container does not remain running.

## Migrations

`migrate` runs `alembic upgrade head` against the pipeline-owned PostgreSQL
database. It does not run Django migrations, migrate the AlertaDengue database,
or apply generated scientific SQL. Migration is separate from analysis
`load=true`, which remains disabled for this issue.

For deliberate migration maintenance, an operator can run the Compose service:

```sh
docker compose \
  --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml \
  up migrate
```

This is an operator maintenance command, not an additional step before normal
`docker compose up -d` startup.

## Stopping services

```sh
docker compose \
  --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml \
  down
```

Do not use `docker compose down -v` during normal operation: the PostgreSQL and
RabbitMQ volumes hold persistent state.

## What is not validated yet

The read-only validation does not prove watcher runtime supervision,
`load=true`, scientific SQL application, map generation, backup/restore,
retention execution, shadow execution, or production cutover. Issue #11 adds
the status command and documents these procedures, but actual backup and
isolated restore acceptance remain pending.
The staging E2E result described in [operations](operations.md) has a different
scope and used the staging profile.

## Recife bulletin production activation

Bulletin scheduling remains off unless both
`INFODENGUE_PIPELINE_BULLETIN_ENABLED=true` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=true` are set and the read-only approval
manifest exactly matches the runtime resources. Keep the source and history
URLs read-only in Beat and the bulletin worker. The manifest profile is
`recife-live-production`; it binds the pipeline, source and history database
identities, approved reference and production bulletin config paths, renderer
SHA-256, revision, immutable bulletin image digest, and dedicated output
directory.
Create it only after the data owner approves those identities and paths. The
config baked into both reviewed images is
`configs/bulletin/recife-production.json`; it enables Recife nowcasting while
the normal bulletin config keeps it disabled.

The complete manifest uses this schema; values must match the container
settings exactly. `reference_sha256` must be the reviewed file's lowercase
SHA-256 and is checked again by readiness before a job can run:

```json
{
  "profile": "recife-live-production",
  "pipeline_database": "pipeline-postgres:5432/pipeline_prod",
  "source_database": "<approved-host>:<port>/<database>",
  "history_database": "<approved-host>:<port>/<database>",
  "reference": "/reference/recife_bairros.rds",
  "reference_sha256": "<64 lowercase hex>",
  "config": "/etc/infodengue/recife-production.json",
  "renderer_revision": "<reviewed-renderer-revision>",
  "bulletin_image": "<repository>@sha256:<64 lowercase hex>",
  "output_root": "/var/lib/pipeline/bulletins"
}
```

Before activation, verify the production PostgreSQL state and migrations, the
read-only source/history roles, approved reference checksum and readability,
and permissions and free space on the persistent bulletin output directory.
Record the bulletin image's `repository@sha256:<64 lowercase hex>` digest and
confirm it contains the reviewed intramunicipal package, Quarto version,
application revision, reference/config paths, and nowcasting config. Never use
an image tag alone. Prepare the exact authorization JSON on the host and mount
it read-only through `BULLETIN_AUTH_HOST_PATH`. Render Compose with the private
env file and inspect `config --images`; confirm Beat and the dedicated worker
resolve the reviewed resources and the worker receives no writable source or
history credentials.

With Beat stopped, run the existing read-only bulletin acceptance/readiness
check inside `worker-bulletin` for the selected epiweek:

```sh
docker compose --env-file .envs/.env.prod \
  -f containers/compose-prod.yaml --profile bulletin \
  exec -T worker-bulletin python -m infodengue_pipeline.bulletin.readiness \
  --week <approved-epiweek>
```

Require `RECIFE BULLETIN READINESS: PASS`. The command uses the same
authorization and readiness checks as the scheduled worker and performs no
rendering or database writes. Confirm the current PE analysis workflow and job
succeeded, the persisted artifacts validate, Recife source/history queries
succeed, and the approved reference/config validate.
For controlled activation, set the explicit switches and exact manifest,
start or recreate only the existing `worker-beat` and `worker-bulletin` in a
reviewed maintenance window, and confirm the registered
`dispatch-ready-bulletins` schedule. The existing queue, persisted job table,
dispatch lease, readiness gate, idempotency key, and explicit retry flow remain
the only scheduling and recovery path.

Monitor Beat and `worker-bulletin` logs, queue depth, and bulletin job states
(`queued`, `running`, `succeeded`, `failed`) in pipeline PostgreSQL. Confirm
the rendered PDF and manifest are under the dedicated persistent output path;
investigate a failed job before using the existing explicit retry task. To
roll back safely, set `INFODENGUE_PIPELINE_BULLETIN_ENABLED=false` (or remove
the authorization manifest), stop Beat first, then stop the bulletin worker.
Do not delete persisted jobs or output artifacts. Keep the existing database,
RabbitMQ, image, and output volume intact for diagnosis or a later reviewed
reactivation. This runbook does not authorize a production restart or deploy.

## Next production gates

Watcher supervision is defined in this production profile. The remaining
production gates are:

- #10: `load=true` staging validation.
- #11: operational status is implemented; backup/restore and retention
  procedures are documented, with isolated restore acceptance pending.
- #12: shadow validation and cutover.
