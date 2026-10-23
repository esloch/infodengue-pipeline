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
stop stays stopped. Systemd sends SIGTERM to the service control group and
allows up to 30 seconds to stop. The current AlertaDengue watcher catches
`KeyboardInterrupt`, but has no explicit SIGTERM cleanup handler; this contract
does not establish application-level graceful shutdown for an active ingestion.
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
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  config --quiet
```

## Starting the production profile

These commands describe the production-profile lifecycle. Do not start the
full ingestion and orchestration stack on the staging host until the private
rehearsal resources and MinIO prefix have been reviewed. Issue #9 provides the
explicit production-profile validation gate.

```sh
docker compose \
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  up -d

docker compose \
  --env-file .env.prod \
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
  --env-file .env.prod \
  -f containers/compose-prod.yaml \
  up migrate
```

This is an operator maintenance command, not an additional step before normal
`docker compose up -d` startup.

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

Rendering this Compose file does not prove watcher runtime supervision, production
runtime connectivity, `load=true`, scientific SQL application, map generation,
production backup/restore, retention, shadow execution, or production cutover.
The staging E2E result described in [operations](operations.md) has a different
scope and used the staging profile.

## Next production gates

Watcher supervision is defined in this production profile. The remaining
production gates are:

- #9: production-profile deployment validation.
- #10: `load=true` staging validation.
- #11: operations, backup/restore, and retention.
- #12: shadow validation and cutover.
