# Issue #10: controlled scientific load on staging

Run these phases on the **staging server**, never the development VPS. Stop at
any failed gate. This is a manual acceptance procedure; CI never invokes the
mutating command. Keep credentials, fixture contents, SQL, and database dumps
out of tickets. Use an isolated production-profile Compose project, dedicated
MinIO prefix, and a new pipeline PostgreSQL/RabbitMQ state. Never use `down -v`.

`AlertaDengueAnalise` generates SQL with `AlertTools::write_alerta` and applies
the dengue and chikungunya files via `psql -f` inside
`pipeline.refresh-alertas-job --load true`; Zika SQL may be generated but the
pinned task does **not** apply it. Upserts can cover the default 100 week
scientific window. A selected UF/week does not bound those writes. The analysis
image therefore installs a `psql` wrapper in `/usr/local/bin` that checks the
generated statements and live target **before** forwarding to the pinned
Conda environment's PostgreSQL client.
Only the fixed, restored `analysis-e2e-db/analysis_e2e` clone can pass. That
database is isolated in its own Compose volume, has no published port, and
must use a restricted `analysis_e2e_writer` role. It is never the ordinary
staging scientific database. The full historical window remains available to
the real scientific code; no artificial one-week policy is imposed.

**Acceptance blocker:** the pinned image retains the real `psql` binary at
`/opt/conda/envs/alertadengueanalise/bin/psql`. The task's normal PATH call
uses the review wrapper, but direct invocation of that binary or another
PostgreSQL client can bypass the wrapper. The wrapper strips libpq connection
override variables and accepts only literal values in reviewed upserts, but
these measures do not establish a mandatory SQL boundary. The restricted
writer and isolated clone limit the damage. Do not claim pre-application review
is unbypassable or accept the scientific load until a mandatory boundary is
implemented and verified against the pinned image.

## 1. Pin checkout

```sh
git status --short --branch
git branch --show-current
git rev-parse HEAD
```

Require `feat/prod-analysis-load-validation-10`, a reviewed commit and a clean
tree. Record the SHA in the private evidence log.

## 2. Inventory staging services

```sh
docker ps --format '{{.Names}} {{.Status}}'
docker volume ls --filter name=infodengue-pipeline-prod
ps -eo pid,user,lstart,args | grep '[i]ngestion_watcher.py'
pgrep -af '[i]ngestion_watcher.py'
for pid in $(pgrep -f '[i]ngestion_watcher.py'); do
  printf 'PID=%s cwd=' "$pid"
  readlink "/proc/$pid/cwd"
  tr '\0' ' ' <"/proc/$pid/cmdline"
  printf '\n'
done
test -d /opt/data/staging/sinan/incoming
```

Record ordinary staging service health. Confirm the rehearsal project, volumes,
watcher, and incoming directory ownership. Inspect each watcher's `--path`,
working directory and command line in the read-only listing.
The inactive `infodengue-sinan-watcher.service` does not establish that no
watcher is running. The October 22 host process watches
`/opt/data/staging/sinan/incoming` with `--requeue --include-existing`; five
June processes used the literal incorrect `$` path. Confirm current facts
before proceeding. Do not start, stop, or clean up a watcher in this rehearsal.

## 3. Verify both image digests

Build the core and analysis worker from this commit using the issue #9 image
procedure, publish them to a staging-accessible OCI registry, and record their
pullable `repository@sha256:<manifest digest>` references. Verify each digest
pulls to the reviewed image ID. Check the analysis image's pinned
`AlertaDengueAnalise` revision in the CI workflow; check
`R --version`, `python --version`, `makim deps.check`, and
`makim pipeline.refresh-alertas-job --help` in that image. Require
`command -v psql` to resolve to `/usr/local/bin/psql` and
`test -x /opt/conda/envs/alertadengueanalise/bin/psql` before continuing.
Never use a mutable
tag or Docker image configuration ID as a pullable digest.

## 4. Verify the integration network

```sh
docker network inspect infodengue-staging_infodengue --format '{{.Name}}'
docker inspect infodengue-pipeline-staging-minio \
  --format '{{json .NetworkSettings.Networks}}'
docker inspect infodengue-staging-postgres-1 \
  --format '{{json .NetworkSettings.Networks}}'
```

Require `pipeline-minio` on the pipeline MinIO container and `postgres` on
the staging AlertaDengue PostgreSQL container. The separate
`infodengue-staging-minio-1` is not attached to this integration network.
Confirm the network reaches only approved staging services.

## 5. Provision isolated pipeline state

Use the `infodengue-pipeline-prod` project in
`containers/compose-prod.yaml`, with its own named PostgreSQL and RabbitMQ
volumes and fresh passwords. The separate `analysis-e2e-db` profile has its
own scientific clone volume and **no host port**. Verify ports 5436 and 5675
are free. Never point the pipeline URL at the ordinary staging pipeline
database or broker.

## 6. Verify dedicated MinIO scope

Provision a new staging-only bucket or empty prefix outside every existing
consumer's namespace. Verify emptiness with the staging MinIO administrator.
For this entry point, use the `infodengue-staging_infodengue` integration
network, `pipeline-minio:9000`, a dedicated bucket beginning `issue10-`, and a prefix
beginning `issue10/`; the command refuses any other routing. Confirm the
staging network actually owns that DNS alias before enabling workers. This
endpoint names the pipeline MinIO instance; do not change the ordinary staging
MinIO instance. Use a fresh `issue10/` subprefix with no other active consumer.
The worker role needs bucket existence, list and object read in that scope;
the separate `.env.prod.e2e` writer needs `PutObject` and `StatObject` for that
scope. Neither needs deletes. Check host watchers and non-Docker consumers as
well as containers. Reusing issue #9's read-only MinIO policy will not permit
the fixture upload.

## 7. Configure private files

```sh
cp -n .env.prod.example .env.prod
cp -n .env.prod.analysis.example .env.prod.analysis
cp -n .env.prod.e2e.example .env.prod.e2e
chmod 600 .env.prod .env.prod.analysis .env.prod.e2e
```

Set only staging hosts, verified image digests, the unique bucket/prefix,
isolated pipeline URLs, a clone admin password, and dedicated output bind.
Set `PIPELINE_E2E_ENV_FILE=../.env.prod.e2e` in `.env.prod`. Begin with
`INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false` in **both** worker files for the
read-only prerequisite. The analysis DB URL in `.env.prod.analysis` and
`.env.prod.e2e` must point to
`analysis_e2e_writer@analysis-e2e-db:5432/analysis_e2e`; set a private writer
password. The core file needs the staging source DB read role and the same
readiness policy as the intended test. Avoid BR because default BR readiness
requires both dengue and chikungunya ingestions. Keep all files untracked.
Use `pipeline_readonly@postgres:5432/dengue` for the source URL and
`pipeline_analysis_readonly@postgres:5432/dengue` for the initial scientific
read-only URL. The source and writer credentials must remain separate. Verify
the effective database and role through a read-only connection; URL text alone
is insufficient.

## 8. Verify analysis DB identity and grants

The staging DBA must confirm the **source** dump comes from staging and the
**destination** is only the isolated clone. On that clone, create
`analysis_e2e_writer` with `NOSUPERUSER NOCREATEDB NOCREATEROLE
NOREPLICATION NOBYPASSRLS`. Grant the scientific read permissions required by
the pinned revision and `INSERT, UPDATE` on the three
`"Municipio"."Historico_alerta*"` tables only. Do not grant DELETE, TRUNCATE,
DDL, ownership or broad database roles. Verify actual column names, unique
constraints, schema grants and effective privileges before doing so. Grant
read-only access to `rehearsal.restore_evidence` (phase 9). Require
`SELECT current_database(), current_user, inet_server_addr();` to report the
clone and writer role. The clone supplies database-enforced isolation from
ordinary staging and production while allowing the real 100 week analysis
window.

After phase 9's restore, connect to the clone as its admin and apply reviewed
grants. Set the writer password interactively with `\password`; never put it
in a shell command or evidence log:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  exec analysis-e2e-db psql -U analysis_e2e_admin -d analysis_e2e
```

```sql
CREATE ROLE analysis_e2e_writer LOGIN NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS;
\password analysis_e2e_writer
GRANT CONNECT ON DATABASE analysis_e2e TO analysis_e2e_writer;
GRANT USAGE ON SCHEMA "Municipio" TO analysis_e2e_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA "Municipio" TO analysis_e2e_writer;
GRANT INSERT, UPDATE ON
  "Municipio"."Historico_alerta",
  "Municipio"."Historico_alerta_chik",
  "Municipio"."Historico_alerta_zika"
TO analysis_e2e_writer;
```

The DBA adds only the read grants the scientific revision needs on other
schemas, verifies the writer inherits no broader grants, and tests effective
privileges. The writer must read the historical tables and
`rehearsal.restore_evidence` and write only those historical tables within
the clone.

## 9. Back up and prove restoration

The staging DBA creates a custom-format dump of the **source staging scientific
database** and records its source cluster identity, database, timestamp,
command, owner and recovery procedure in private evidence. Confirm the dump
was taken with staging credentials and verify its archive and checksum:

```sh
pg_restore --list /secure/staging/analysis-before.dump >/dev/null
sha256sum /secure/staging/analysis-before.dump
```

Start only the isolated empty clone and copy the dump into it:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never analysis-e2e-db
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  ps analysis-e2e-db
CLONE_CONTAINER=$(docker compose --env-file .env.prod \
  -f containers/compose-prod.yaml ps -q analysis-e2e-db)
docker cp /secure/staging/analysis-before.dump \
  "$CLONE_CONTAINER:/tmp/analysis-before.dump"
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  exec -T analysis-e2e-db sh -ec '
    pg_restore --list /tmp/analysis-before.dump >/dev/null
    PGPASSWORD="$POSTGRES_PASSWORD" pg_restore --exit-on-error \
      --single-transaction --no-owner --no-acl \
      -U analysis_e2e_admin -d analysis_e2e \
      /tmp/analysis-before.dump
  '
```

Require `pg_restore` exit zero. If an extension or schema cannot be restored,
stop; never mark it successful. Query the clone's three historical tables and
other source control totals to prove restored content. Keep the restore log.
The DBA then creates the restricted writer and grants from phase 8, plus this
marker **only after** the successful restore and content checks:

```sql
CREATE SCHEMA rehearsal;
CREATE TABLE rehearsal.restore_evidence (
  backup_sha256 text NOT NULL,
  source_environment text NOT NULL,
  restored_database text NOT NULL,
  restored_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO rehearsal.restore_evidence
  (backup_sha256, source_environment, restored_database)
VALUES ('<verified 64-character SHA-256>', 'staging', 'analysis_e2e');
GRANT USAGE ON SCHEMA rehearsal TO analysis_e2e_writer;
GRANT SELECT ON rehearsal.restore_evidence TO analysis_e2e_writer;
```

Record a private JSON proof file with `backup_sha256`,
`source_environment` (`staging`), actual `source_host` (`postgres`),
`source_database` (`dengue`),
`source_role`, `restored_database` (`analysis_e2e`) and
`restore_test_succeeded` (`true`). The runner checks the dump bytes and proof;
it also queries the independent marker in the **restored clone**. JSON alone
is insufficient. Preserve the actual `pg_restore` exit status, log, content
checks, recovery command and responsible DBA. Never put dumps under the
checkout. If the restore was made with a superuser role, confirm the writer
is not that role and owns no restored tables.

## 10. Choose one fresh fixture

Use a small, authorized synthetic or sanitized **dengue UF** SINAN CSV/DBF
accepted by the real AlertaDengue watcher. No suitable fixture is committed
here: fixture preparation and scientific validity are an operator prerequisite.
The existing incoming directory already contains staging CSV/DBF files. Never
move, overwrite, delete, or reingest them. File absence from `incoming` is no
proof of freshness: the watcher can process a file and the source database
keeps the run SHA. The runner checks `ingestion.run` by SHA before upload and
uses an unpredictable object key in a dedicated bucket/prefix. The current
watcher uses `--requeue --include-existing`, so verify its effective consumer
scope and existing work before executing.
Record provenance, UF, epidemiological year/week, disease, size, SHA-256, and
the intended new bucket/prefix. The runner probes `ingestion.run` by SHA and
creates a nonce-bearing key; an existing SHA is refused. Check no other
consumer can ingest it. Do not alter real patient records to force freshness.

## 11. Capture bounded baseline

Confirm actual columns and identifiers with `information_schema.columns` for
`Municipio.Historico_alerta`, `_chik`, and `_zika`. For the selected week and
reviewed municipality geocodes, capture keys, count, and update indicators
from the **restored clone** using the same bounded query for each applicable
table, for example:

```sql
SELECT "SE", municipio_geocodigo, count(*) AS rows,
       min(id) AS min_id, max(id) AS max_id
FROM "Municipio"."Historico_alerta"
WHERE "SE" = :selected_week
  AND municipio_geocodigo = ANY(:reviewed_geocodes)
GROUP BY "SE", municipio_geocodigo;
```

Also capture read-only source staging control totals to prove ordinary staging
remains unchanged. Dengue and chik SQL are required and applied by the pinned
`load=true` contract; Zika SQL is conditional and **not applied** by that
command. Capture expected changes before writing. Keep geocodes bounded.

## 12. Validate Compose statically

```sh
uv run makim containers.prod-config
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --images
```

Inspect mounts, network, digest images, both initial `load=false` settings,
and the fixed clone target privately. Confirm `analysis-e2e-db` has no host
port and a separate volume. Never print full rendered Compose output to a log.

## 13. Start only the rehearsal project

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never
```

The `analysis-e2e-runner` and clone are profile-gated. The clone was started
explicitly in phase 9; the runner will not start with `up`. No scientific
load can run in this initial `load=false` configuration.

## 14. Wait for migrations and worker health

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  exec -T worker-ingestion python -c \
  'from infodengue_pipeline.config import get_settings; from infodengue_pipeline.production.analysis_e2e import verify_source_staging; verify_source_staging(get_settings()); print("source database, role and read-only grants: ok")'
```

Require `migrate` exit 0 and all five workers healthy. Verify the actual
host-level watcher processes and paths again. Require one effective watcher
for `/opt/data/staging/sinan/incoming` and establish that other consumers
cannot process the dedicated object or its materialized file. The watcher's
per-directory lock does not coordinate different watch paths.

## 15. Run the read-only prerequisite

`prod.validate` from issue #9 deliberately requires `load=false`. With the
initial private files still set to false, run:

```sh
uv run makim prod.validate
```

Record the read-only result. Only after every earlier gate passes, stop the
rehearsal workers and Beat (keep their isolated volumes and the clone), change
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=true` in both private worker files, and
set the analysis file's fixed `ANALYSIS_STAGING_TARGET_HOST=analysis-e2e-db`,
`_DATABASE=analysis_e2e`, `_ROLE=analysis_e2e_writer`,
`ANALYSIS_STAGING_BACKUP_SHA256=<verified dump SHA>`,
`ANALYSIS_STAGING_BACKUP_VERIFIED=true`, and selected
`ANALYSIS_E2E_COVERAGE`/`_EPIWEEK`. Confirm the MinIO prefix is still empty,
then recreate only the isolated production-profile workers and Beat. Verify
their health and effective non-secret settings. Do not restart ordinary
staging services. The worker independently checks the clone's restore marker
before starting any mutating job.

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  stop worker-beat worker-ingestion worker-events worker-orchestration worker-analysis
${EDITOR:-vi} .env.prod .env.prod.analysis
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never --force-recreate \
  worker-ingestion worker-events worker-orchestration worker-analysis worker-beat
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
```

## 16. Execute one controlled ingestion

Only after the DBA scope, backup, fixture, and consumer isolation gates pass:

```sh
uv run makim prod.analysis-e2e \
  --fixture /secure/staging/fresh-sinan.csv \
  --backup /secure/staging/analysis-before.dump \
  --restore-proof /secure/staging/restore-proof.json \
  --backup-sha256 '<64-character-sha256>' \
  --expected-host analysis-e2e-db \
  --expected-database analysis_e2e \
  --expected-role analysis_e2e_writer
```

The wrapper mounts the fixture and evidence read-only into a one-shot runner.
It checks the fixed clone identity, linked backup bytes, live restore marker,
and upstream SHA freshness before upload. The scientific image then reviews
each generated dengue/chik SQL file **before** its `psql` application. It
rejects other SQL shapes, paths and destinations. The database role and
isolated clone provide an independent boundary if SQL review fails. The
command uses the existing MinIO ingress and workflow validator.
Do not retry this command after a failure without an investigation and a new
fixture. Never manually retry the analysis task during acceptance.

## 17. Verify lifecycle

Record object key, SHA, `ingestion.run` ID, workflow ID, job ID, status,
`attempts=1`, `load=true`, image digest and timestamps. Query the isolated
`pipeline.ingress_object`, `workflow_input`, `workflow`, and `analysis_job`
rows by those exact IDs. Require one dispatch and no pending or running job for
the selected fingerprint. Celery analysis queue routing is
`pipeline.analysis`; check its queue depth and workers.

## 18. Inspect scientific artifacts

Inspect the job's dedicated output directory. Require loadable current-week
UF and BR `.RData` using `Rscript --vanilla -e 'e<-new.env();
stopifnot(length(load(commandArgs(TRUE)[1], envir=e))>0)' <path>` in the
analysis image. Review nonempty dengue and chik SQL statements, their target
tables, covered weeks and geocodes, plus the pre-application review result for
each file. Review Zika SQL if produced and record that it was not applied. Validate
actual maps with `file` or PNG decoding, not just existence. Preserve the
analysis logs and confirm SQL application completed before map generation.

## 19. Compare bounded scientific rows

Repeat phase 11's same filtered queries on the clone. Compare counts, keys
and relevant values, including expected updates to preexisting rows. A
missing expected change fails acceptance. Explain Zika exclusion or generated
but unapplied Zika SQL. Compare source staging control totals again to prove
the ordinary staging scientific database was untouched. Review the full
analysis window's affected weeks on the clone with DBA-approved bounded audit
evidence; the SQL is not limited to the selected week.

## 20. Check lock and duplicates

The analysis runner uses a database-scoped PostgreSQL advisory lock and
releases it on exit. Query `pg_locks` for the analysis role after completion
and require no advisory lock or orphaned scientific task. Confirm one job for
the workflow fingerprint and `attempts=1` in pipeline PostgreSQL. A failed
job is evidence: do not clean or retry it automatically.

## 21. Retain sanitized evidence

Store commit and image digests, non-secret DB identity, fixture SHA/object
identity, bounded before/after counts, job IDs/status/attempts, artifact
checks, lock result and restore proof reference. Redact URLs, credentials,
patient data, and SQL row values.

## 22. Stop only the rehearsal project

```sh
docker compose --profile analysis-e2e-db --env-file .env.prod \
  -f containers/compose-prod.yaml down
```

Keep volumes, output files, failed-job evidence, backup and logs for review.

## 23. Recheck ordinary staging

```sh
docker ps --format '{{.Names}} {{.Status}}'
ps -eo pid,user,lstart,args | grep '[i]ngestion_watcher.py'
docker inspect infodengue-pipeline-staging-minio \
  --format '{{json .NetworkSettings.Networks}}'
docker inspect infodengue-staging-postgres-1 \
  --format '{{json .NetworkSettings.Networks}}'
```

Require ordinary staging services healthy and unchanged. The issue is accepted
only after the controlled load and every scientific check above succeeds.
