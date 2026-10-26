# Issue #10: controlled scientific load on staging

Run these phases on the **staging server**, never the development VPS. Stop at
any failed gate. This is a manual acceptance procedure; CI never invokes the
mutating command. Keep credentials, fixture contents, SQL, and database dumps
out of tickets. Use an isolated production-profile Compose project, dedicated
MinIO prefix, and a new pipeline PostgreSQL/RabbitMQ state. Never use `down -v`.

**Known upstream limit:** `AlertaDengueAnalise` applies its generated SQL inside
`pipeline.refresh-alertas-job --load true`; the pipeline cannot inspect those
statements before they execute. `AlertTools::write_alerta` produces per-row
upserts for dengue, chikungunya, and potentially Zika, with rows from the
analysis window. The production-profile runner checks the declared job week
and coverage, but that does not constrain individual SQL rows. **Do not run
phase 16 until the staging DBA has reviewed the exact scientific revision,
established database-enforced write restrictions for the selected coverage and
week, and tested them with the analysis role.** If that cannot be done without
changing the scientific contract, stop and add a pre-apply SQL review hook
upstream. A command-line declaration is not a substitute for that gate.

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
systemctl --user status infodengue-sinan-watcher.service
```

Record ordinary staging service health. Confirm the rehearsal project, volumes,
watcher, and incoming directory ownership. Stop if any rehearsal state has an
unknown owner. Do not start a second watcher.

## 3. Verify both image digests

Build the core and analysis worker from this commit using the issue #9 image
procedure, publish them to a staging-accessible OCI registry, and record their
pullable `repository@sha256:<manifest digest>` references. Verify each digest
pulls to the reviewed image ID. Check the analysis image's pinned
`AlertaDengueAnalise` revision in the CI workflow; check
`R --version`, `python --version`, `makim deps.check`, and
`makim pipeline.refresh-alertas-job --help` in that image. Never use a mutable
tag or Docker image configuration ID as a pullable digest.

## 4. Verify the integration network

```sh
docker network inspect infodengue-staging_infodengue --format '{{.Name}}'
```

Confirm this network reaches only approved staging services.

## 5. Provision isolated pipeline state

Use the `infodengue-pipeline-prod` project in
`containers/compose-prod.yaml`, with its own named PostgreSQL and RabbitMQ
volumes and fresh passwords. Verify ports 5436 and 5675 are free. Never point
the pipeline URL at the ordinary staging pipeline database or broker.

## 6. Verify dedicated MinIO scope

Provision a new staging-only bucket or empty prefix outside every existing
consumer's namespace. Verify emptiness with the staging MinIO administrator.
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

Set only staging hosts, the verified image digests, unique bucket/prefix,
isolated pipeline URLs, and dedicated output bind. Set `PIPELINE_E2E_ENV_FILE`
to `../.env.prod.e2e` in `.env.prod`. Set
`INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=true` in **both** worker files. The
analysis file also needs the actual staging analysis DB URL, exact
`ANALYSIS_STAGING_TARGET_HOST`, `_DATABASE`, `_ROLE`, selected UF coverage and
epiweek, and `ANALYSIS_STAGING_BACKUP_VERIFIED=true` only after phase 9. The
core file needs the staging source DB read role and the same readiness policy
as the intended test. Avoid BR because default BR readiness requires both
dengue and chikungunya ingestions. Keep all files untracked.

## 8. Verify analysis DB identity and grants

The DBA must verify host, database, effective role, `rolbypassrls=false`,
effective table grants, and enforced write scope for the chosen UF and week.
Use the same credentials as the analysis worker. Query the live connection with
`SELECT current_database(), current_user, inet_server_addr();` and inspect
`pg_roles`, `pg_class`, and `pg_policies` for the three historical tables.
The role must not own those tables or have broad update/delete privileges.
Test permitted and denied writes inside rolled-back transactions. Verify the
exact tables and keys before designing policies; the scope must cover SQL
generated for the full analysis window, or the job will fail and may leave
partial application. Do not silently broaden it.

## 9. Back up and prove restoration

Use a staging DBA-approved backup of the affected staging scientific database
immediately before the run. For a PostgreSQL custom-format dump, verify:

```sh
pg_restore --list /secure/staging/analysis-before.dump >/dev/null
sha256sum /secure/staging/analysis-before.dump
```

Restore that dump into a separate empty staging test database using the DBA's
documented recovery procedure; query its identity and the three historical
tables. Record a private JSON proof file with `backup_sha256`,
`source_environment` (`staging`), `source_host`, `source_database`,
`source_role`, `restored_database`, and `restore_test_succeeded` (`true`). The
runner checks the dump hash and proof linkage. The operator must retain the
actual restore logs; a JSON file alone does not establish restorability.
Record the recovery command and owner before proceeding. Do not put the dump
under this checkout.

## 10. Choose one fresh fixture

Use a small, authorized synthetic or sanitized **dengue UF** SINAN CSV/DBF
accepted by the real AlertaDengue watcher. No suitable fixture is committed
here: fixture preparation and scientific validity are an operator prerequisite.
Record provenance, UF, epidemiological year/week, disease, size, SHA-256, and
the intended new bucket/prefix. The runner probes `ingestion.run` by SHA and
creates a nonce-bearing key; an existing SHA is refused. Check no other
consumer can ingest it. Do not alter real patient records to force freshness.

## 11. Capture bounded baseline

Confirm actual columns and identifiers with `information_schema.columns` for
`Municipio.Historico_alerta`, `_chik`, and `_zika`. For the selected week and
reviewed municipality geocodes, capture keys, count, and update indicators
using the same bounded query for each applicable table, for example:

```sql
SELECT "SE", municipio_geocodigo, count(*) AS rows,
       min(id) AS min_id, max(id) AS max_id
FROM "Municipio"."Historico_alerta"
WHERE "SE" = :selected_week
  AND municipio_geocodigo = ANY(:reviewed_geocodes)
GROUP BY "SE", municipio_geocodigo;
```

Record which diseases are applicable. Dengue and chik SQL are required by the
current scientific `load=true` contract; Zika is conditional. Capture the
expected row changes before writing. Keep geocodes bounded and reviewed.

## 12. Validate Compose statically

```sh
uv run makim containers.prod-config
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --images
```

Inspect mounts, network, digest images, both `load=true` settings and the
selected scope privately. Never print full rendered Compose output to a log.

## 13. Start only the rehearsal project

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never
```

The `analysis-e2e-runner` is profile-gated and will not start with `up`.

## 14. Wait for migrations and worker health

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
```

Require `migrate` exit 0 and all five workers healthy. Verify the existing
single watcher is active and points to the staging incoming bind.

## 15. Run the read-only prerequisite

`prod.validate` from issue #9 deliberately requires `load=false`. For this
phase, use a separate private copy of the issue #9 configuration with
`load=false` and the same reviewed staging resources, run
`uv run makim prod.validate`, then restore the reviewed `load=true` files and
recreate only the rehearsal workers. Do not change the meaning of
`prod.validate`. If this cannot be performed without disrupting the isolated
project, complete the issue #9 read-only validation before the `load=true`
startup in phase 13. Record that output as the prerequisite.

## 16. Execute one controlled ingestion

Only after the DBA scope, backup, fixture, and consumer isolation gates pass:

```sh
uv run makim prod.analysis-e2e \
  --fixture /secure/staging/fresh-sinan.csv \
  --backup /secure/staging/analysis-before.dump \
  --restore-proof /secure/staging/restore-proof.json \
  --backup-sha256 '<64-character-sha256>' \
  --expected-host '<staging-db-host>' \
  --expected-database '<staging-db-name>' \
  --expected-role '<staging-scoped-role>'
```

The wrapper mounts the fixture and evidence read-only into a one-shot runner.
It checks target identity, linked backup proof and upstream SHA freshness
before upload. It then uses the existing MinIO ingress and workflow validator.
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
tables, covered weeks and geocodes. Review Zika SQL if produced. Validate
actual maps with `file` or PNG decoding, not just existence. Preserve the
analysis logs and confirm SQL application completed before map generation.

## 19. Compare bounded scientific rows

Repeat phase 11's same filtered queries. Compare counts, keys and relevant
values, including expected updates to preexisting rows. A missing expected
change fails acceptance. Explain Zika exclusion if no Zika input/output was
configured. Check for unexpected changes outside the selected scope using
DBA-approved audit evidence.

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
docker compose --env-file .env.prod -f containers/compose-prod.yaml down
```

Keep volumes, output files, failed-job evidence, backup and logs for review.

## 23. Recheck ordinary staging

```sh
docker ps --format '{{.Names}} {{.Status}}'
systemctl --user is-active infodengue-sinan-watcher.service
```

Require ordinary staging services healthy and unchanged. The issue is accepted
only after the controlled load and every scientific check above succeeds.
