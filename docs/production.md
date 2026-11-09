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

## Mosqserver ownership and initial ingress gate

Production MinIO is `minio:9000`, bucket `sinan-infodengue`, on
`infodengue-prod_infodengue`. Its current SINAN namespace is the bucket root.
The current `mc mirror --watch` materializer mirrors that whole root, so a
dedicated prefix is not the present ownership contract. The selected cutover
replaces the root materializer under operator control: only one active
materializer may publish this namespace into
`/Storage/infodengue_data/sinan/incoming`. Do not run the pipeline scanner
alongside the legacy root mirror. `mc mirror --exclude` can filter object
paths, but an exclusion is safe only after validating the exact installed
`mc` version and pattern behavior against root keys; no production MinIO
configuration is changed by this repository.

Before starting workers or Beat, run the explicit historical reconciliation
command for the two approved keys. It downloads each object into private
staging, verifies SHA-256, validates the read-only `ingestion.run` record,
and records a `reconciled` ingress row. It does not create the watched
incoming directory, publish a file, create an outbox event, or dispatch
analysis. Source unavailability, an incomplete or ambiguous source record,
or invalid metadata prevents reconciliation. Running this command is the
pre-scanner acceptance step; the scanner itself retains the same fail-closed
SHA check as a defense in depth.

The verified production hashes are `848de6b612bf18b51a6f8e773075cde5b67ccf50634941cb960035370075e7fd`
for Dengue and `bb385388ea60ae0be7781ae22bb1c293aa0e72c153ff64359d60b56684081b58`
for Chikungunya. Before scanner authorization, compare downloaded bytes,
source rows and reconciled rows against those values. These read-only queries
prove source completion and local reconciliation; run them through the
approved read-only source and pipeline connections respectively:

```sql
SELECT id, status, disease, canonical_cid10, delivery_se, sha256,
       finished_at
FROM ingestion.run
WHERE sha256 IN (
  '848de6b612bf18b51a6f8e773075cde5b67ccf50634941cb960035370075e7fd',
  'bb385388ea60ae0be7781ae22bb1c293aa0e72c153ff64359d60b56684081b58'
);

SELECT object_key, status, sha256, run_id, canonical_cid10, delivery_se,
       run_finished_at, reconciled_at
FROM pipeline.ingress_object
WHERE sha256 IN (
  '848de6b612bf18b51a6f8e773075cde5b67ccf50634941cb960035370075e7fd',
  'bb385388ea60ae0be7781ae22bb1c293aa0e72c153ff64359d60b56684081b58'
);
```

The source query must return exactly one `completed` row per hash with the
expected canonical disease and run UUID. After explicit reconciliation the
pipeline query must show both hashes as `reconciled`, with matching run UUIDs,
canonical CID-10, and delivery week 202644. Confirm neither retained file is
present in `/Storage/infodengue_data/sinan/incoming`. These checks happen
before scanner or Beat startup, removing the previous circular gate.

### SE 202644 acceptance criteria

Accept reconciliation only when all of these checks pass:

- `DENGUE (3).csv` hashes to
  `848de6b612bf18b51a6f8e773075cde5b67ccf50634941cb960035370075e7fd` and
  has exactly one completed source run with canonical CID-10 `A90`, delivery
  week `202644`, a valid UUID and a non-null completion timestamp.
- `CHIKUNGUNYA (4).csv` hashes to
  `bb385388ea60ae0be7781ae22bb1c293aa0e72c153ff64359d60b56684081b58` and
  has exactly one completed source run with canonical CID-10 `A92.0`, delivery
  week `202644`, a valid UUID and a non-null completion timestamp.
- Each pipeline row is `reconciled`, with the exact object SHA, source run UUID,
  canonical disease, delivery week, completion timestamp and reconciliation
  timestamp. Re-running the command returns zero newly reconciled rows and
  preserves the same metadata.
- A before/after manifest of `/Storage/infodengue_data/sinan/incoming` is
  unchanged. Outbox-event and analysis-job counts are unchanged by the
  reconciliation command. Scanner and Beat remain stopped during these checks.
- After the scanner is separately enabled, repeated scans leave both rows
  `reconciled`; neither key is materialized into incoming and no duplicate
  AlertaDengue run is created.

Historical reconciliation and scientific event bootstrap are separate
operations. The command records auditable metadata only and never creates
events. This repository does not provide an event-bootstrap command. If
historical inputs are later approved for workflow readiness, that must be a
separate reviewed change with an explicit week allowlist, audited event IDs,
outbox idempotency and proof that dispatch remains disabled. Preserve the
`reconciled` row and source metadata. Fresh hashes continue through normal
materialization, watcher, completion bridge, outbox, readiness, and analysis.

Atomic publication requires `/var/lib/pipeline/.incoming-staging` and
`/var/lib/pipeline/incoming` to be on the same filesystem. The production
Compose bind maps the parent of the host incoming path to `/var/lib/pipeline`;
it intentionally does not mount canonical StorageBox. The image runs as the
`pipeline` user, default UID/GID 1000 unless the image is built with different
`HOST_UID`/`HOST_GID`; provision the host directory for that identity with
directory traversal/write access. The scanner sets staging mode 0700 and file
mode 0640. Keep analysis output on its own persistent writable host directory.
Compose operational ports bind only to loopback; workers depend on healthy
pipeline PostgreSQL, successful migration, and healthy RabbitMQ. Beat is a
single service and must remain stopped until the shadow gate below passes.

## Database least privilege and verification

Use separate roles and URLs for pipeline operational PostgreSQL,
AlertaDengue's `ingestion.run` source, and the scientific analysis database.
The source role needs schema usage and `SELECT` only on the run fields queried
by the adapter (`id`, `status`, `uf`, `disease`, `canonical_cid10`,
`delivery_se`, `sha256`, and `finished_at`). The analysis role needs only
read access while `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`; provision any
write grants only as a separate approved change when load is authorized.
Role names do not prove effective permissions. Verify using the actual
connection roles and inspect `has_table_privilege` / `has_column_privilege`
and effective grants, including inherited role memberships. Do not print
connection URLs or grant output containing secrets. Provision grants through
the database owner using deployment-managed SQL; never change AlertaDengue
Django schemas from this repository.

Run these checks while connected as the configured source role and then as
the analysis role (the table names are the source ingestion table and the
scientific tables required by the reviewed analysis image):

```sql
SELECT current_user,
       has_schema_privilege(current_user, 'ingestion', 'USAGE')
         AS source_schema_usage,
       has_table_privilege(current_user, 'ingestion.run', 'SELECT')
         AS source_select,
       has_table_privilege(current_user, 'ingestion.run', 'INSERT')
         AS source_insert,
       has_table_privilege(current_user, 'ingestion.run', 'UPDATE')
         AS source_update,
       has_table_privilege(current_user, 'ingestion.run', 'DELETE')
         AS source_delete,
       has_column_privilege(
         current_user, 'ingestion.run', 'canonical_cid10', 'SELECT'
       ) AS canonical_cid10_select;
```

Source results must show schema usage and required column reads, with no
source write privileges. Repeat the DML privilege checks for every scientific
table used by the analysis image; all must be false while load is disabled.
Then inspect inherited grants as the database owner (`\du+` and
`information_schema.role_table_grants`) because effective privileges can be
inherited. The owning DBA should provision only the source schema usage and
column-scoped SELECT required by the adapter, and analysis SELECT grants for
shadow mode; use a separate reviewed DBA change for scientific write grants.
Do not put passwords or role credentials in SQL files or command history.

## Mosqserver deployment gates

Run each command below only after setting the shell variables from the
reviewed private environment files. Inspection commands are labeled
**inspect**; commands that change service state are labeled **change**.
Do not use `compose up` for the initial deployment: it would start Beat before
reconciliation and shadow authorization.

1. **Inspect** reviewed source and analysis image provenance. Both image
   references must use immutable reviewed SHA-256 digests, and the core and
   analysis image settings must agree. Confirm private files are mode 0600;
   required pipeline PostgreSQL, RabbitMQ, MinIO, source DB and analysis DB
   secrets are provisioned separately. Rotate no existing credentials as
   part of this rollout.
2. **Inspect** MinIO endpoint, bucket, root namespace, retained objects and
   ownership. Verify that no other materializer will publish the same root
   namespace when the new scanner is enabled. Confirm both retained files by
   fetching them read-only and comparing against SHA-256 values in the
   incident record. Never include object contents or credentials in logs.
3. **Inspect** AlertaDengue source records by SHA-256. Confirm a single
   `completed` run, non-null completion timestamp, run UUID and canonical
   disease for each retained object. The explicit reconciliation command's
   durable rows are the cutover proof; do not manually insert ingress records.
4. **Inspect** effective DB grants as described above. Run the production
   validator from the appropriate isolated worker role, and check source DB
   availability before scanner startup. The source connection is read-only.
5. **Inspect** the scientific database's latest persisted week and any active
   legacy analysis process. Production currently has historical alert tables
   through SE 202643 and completed deliveries through 202644; these facts do
   not determine the first eligible scientific week. Choose and document that
   week from database state and active-run inspection, then put the same
   `INFODENGUE_PIPELINE_ANALYSIS_INITIAL_EPIWEEK` in core and analysis files.
   Keep `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` in both.
6. **Inspect** capacity, incoming and output ownership, and existing
   PostgreSQL/RabbitMQ recovery plans. Take a PostgreSQL backup and prove it
   restores into an isolated disposable database. Verify RabbitMQ persistent
   data and recovery instructions. Confirm StorageBox free space and incoming
   filesystem permissions. Never test restore against an active shared DB.
7. **Change** watcher ownership only in a coordinated quiet window: verify the
   existing watcher is healthy, stop duplicate watcher processes, then have
   the existing AlertaDengue watcher continue to own canonical validation,
   stage/merge, movement and rollback. Do not replace watcher behavior.
8. **Change** legacy materializer ownership only after verifying it is idle
   and its pending transfers are complete. Retire the legacy root mirror
   before enabling the pipeline scanner. Do not alter MinIO credentials or
   configuration during this code deployment.
9. **Change** apply the reviewed pipeline migrations. Then run the explicit
   historical reconciliation command while the scanner and Beat remain
   stopped. It records database state only and cannot dispatch analysis:

   ```sh
   docker compose --env-file .envs/.env.prod \
     -f containers/compose-prod.yaml run --rm migrate
   docker compose --env-file .envs/.env.prod \
     -f containers/compose-prod.yaml run --rm --no-deps worker-ingestion \
     python -m infodengue_pipeline reconcile-historical \
     --object-key 'DENGUE (3).csv' \
     --object-key 'CHIKUNGUNYA (4).csv' \
     --confirm historical-only
   ```

   Verify the two `reconciled` rows and run metadata with the SQL above; verify
   both files remain absent from incoming and that no outbox event or analysis
   job was created by reconciliation. Only then **change** start non-scheduling
   workers with Beat stopped. This proves the retained SE 202644 objects
   cannot be republished at scanner startup.
10. **Inspect** shadow mode with load disabled. Confirm fresh objects follow
    the normal watcher and outbox path, no duplicate legacy consumer is active,
    and candidate workflow weeks agree with the approved initial week. Do not
    bootstrap historical events or start Beat during this review.
11. **Change** start the single Beat service only after separate shadow-mode
    authorization. Keep scientific load disabled. Enabling `load=true` needs
    explicit scientific-owner authorization, a reviewed analysis role with
    required grants, an approved week, a fresh backup/restore check, and
    confirmation that no legacy analysis is running.
12. **Inspect** post-cutover rows, watcher logs, object/run hashes, event IDs,
    job IDs, output artifacts, backup status and error logs. Confirm secrets
    never appeared in expanded container commands or logs.

### MinIO runtime credential remediation

Production inspection found administrative MinIO credentials expanded in the
legacy materializer's Docker entrypoint. Treat this as a security finding.
Provision a dedicated read-only runtime account scoped to bucket listing and
object reads for the owned namespace, then update the materializer through a
coordinated credential handoff. Keep credentials in a protected environment
file or secret facility rather than command arguments; suppress rendered
Compose configuration and entrypoint output from logs. Rotate the old
administrative credentials only in a separately scheduled change after the
new account is verified and every consumer has moved. This repository does not
rotate credentials or modify production MinIO.

### Rollback

Stop Beat first, then stop scanner and pipeline workers. Leave pipeline
PostgreSQL, RabbitMQ, ingress rows, outbox rows, logs and analysis artifacts
intact for diagnosis. Re-enable the legacy materializer only after proving the
pipeline scanner is stopped and the legacy watcher remains the sole canonical
ingestion owner. Do not remove or overwrite canonical files. For any rollback
that affects source or scientific DB state, use the reviewed isolated restore
and recovery procedure; do not replay a potentially mutating analysis without
operator review.

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
`load=true`, scientific SQL application, map generation, production
backup/restore, retention, shadow execution, or production cutover.
The staging E2E result described in [operations](operations.md) has a different
scope and used the staging profile.

## Next production gates

Watcher supervision is defined in this production profile. The remaining
production gates are:

- #10: `load=true` staging validation.
- #11: operations, backup/restore, and retention.
- #12: shadow validation and cutover.
