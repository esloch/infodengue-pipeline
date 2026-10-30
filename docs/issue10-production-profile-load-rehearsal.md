# Issue #10: production-profile scientific load rehearsal

Status (2026-10-30): blocked before mutating execution. All 55 DBF files
checked under `/Storage/staging_data/sinan/incoming/` already have SHA-256
values in staging `ingestion.run`; no eligible fresh fixture is available.
That check was reported by the operator, not repeated from this development
host. No `load=true` run or scientific database mutation is claimed here.

## Read-only preparation findings

The checked-out production-profile Compose file passes
`uv run makim containers.prod-config` (exit 0). This validates its placeholder
topology, **not** live endpoint routing. The profile defines a separate
`pipeline_prod` database and broker, an external integration network, an
incoming bind, and a dedicated analysis output bind. Its ingestion and analysis
worker commands each specify `--concurrency=1`. The production-profile
validator requires an immutable SHA-256 analysis image reference, checks that
all worker image metadata matches it, and requires workers to be healthy.
It also requires `analysis_load=false`;
it cannot validate the later `load=true` run.

This checkout has no `.env.prod` or `.env.prod.analysis`, no staging incoming
mount, and no running `infodengue-pipeline-prod` containers. The local Docker
images include `infodengue-pipeline-analysis:issue10-check` with image ID
`sha256:a8fa67a7d51cf43797dd057cc6e2ce92e2645d8779e3f8bd3dd0d31fcb975b61`;
that local image ID is **not** a verified deployed OCI digest. The local
`systemctl --user is-active infodengue-sinan-watcher.service` probe could not
connect to the user bus. Thus staging-only database/MinIO endpoint resolution,
the deployed image digest, MinIO prefix exclusivity and mirror exclusion,
watcher state, and actual worker health remain unverified.

The AlertaDengue pgBackRest procedure uses a separate `staging` stanza after
restoring a selected `prod` backup. No staging pgBackRest repository, backup
label, successful restore receipt, or live staging scientific database was
available here. On the staging host, inspect the `staging` stanza with the
read-only `pgbackrest --stanza=staging info` command in
`AlertaDengue/docs/pgbackrest/hetzner_staging_restore_from_pgbackrest.md`.
Match its explicit full-backup label and timestamp to a completed restore
record; verify the recorded post-restore `pg_is_in_recovery()`, data directory,
database size, and `"Dengue_global"."Municipio"` checks. Repository `info`
alone does not prove a restore succeeded. Record the backup ID, restore-check
result, time, and operator before any scientific write.

```sh
cd /opt/services/infodengue/AlertaDengue
docker compose --env-file .envs/.env \
  --file containers/compose-base.yaml \
  --file containers/compose-staging.yaml \
  --file containers/compose-pgbackrest.yaml \
  --project-name infodengue-staging \
  exec -T pgbackrest pgbackrest --stanza=staging info
```

For workflow `(MG, <approved YYYYWW>)`, the default UF readiness contract
requires `A90` dengue; `A92.0` chikungunya and `A928` Zika are optional. The
national BR contract requires `A90` and `A92.0`, with Zika optional. Verify
the running worker's disease settings before submission. The intended MG
epiweek and fresh fixture disease have not been selected. A pre-existing
workflow/fingerprint or queued job must be checked before changing load mode:
`load` is stored when a new job is claimed, so changing the environment does
not turn an existing `load=false` job into `load=true`.

## Prepared read-only scientific snapshots

Run these through a staging **read-only** scientific role before and after
the controlled execution, using the same approved epiweek. First inspect the
live catalog; the column names below come from AlertaDengue's unmanaged
adapters and must be confirmed against staging. `"SE"` is the epidemiological
week, `id` is the stable row key, and `municipio_geocodigo` joins to
`"Dengue_global"."Municipio".uf` (full state name). Join
`"Dengue_global"."estado"` to select abbreviation `MG`. If the Zika table is
absent, record that and skip its snapshot.

```sql
SELECT to_regclass('"Municipio"."Historico_alerta"'),
       to_regclass('"Municipio"."Historico_alerta_chik"'),
       to_regclass('"Municipio"."Historico_alerta_zika"'),
       to_regclass('"Dengue_global"."Municipio"'),
       to_regclass('"Dengue_global"."estado"');
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE (table_schema, table_name) IN
  (('Municipio','Historico_alerta'),
   ('Municipio','Historico_alerta_chik'),
   ('Municipio','Historico_alerta_zika'),
   ('Dengue_global','Municipio'), ('Dengue_global','estado'))
  AND column_name IN ('id','SE','municipio_geocodigo','geocodigo','uf','nome')
ORDER BY table_schema, table_name, ordinal_position;
```

In `psql`, set `epiweek` to the approved MG `YYYYWW` and `alert_table` to each
existing table name in turn. Run the same two bounded queries for each table
before and after. Record the count, ID range, row digest, and at most 20
sample rows; retain the snapshots outside the repository.

```sql
\set epiweek 2026XX
\set alert_table Historico_alerta
WITH scoped AS (
  SELECT h.* FROM "Municipio".:"alert_table" AS h
  JOIN "Dengue_global"."Municipio" AS m
    ON m.geocodigo = h.municipio_geocodigo
  JOIN "Dengue_global"."estado" AS e ON e.nome = m.uf
  WHERE e.uf = 'MG' AND h."SE" = :epiweek
)
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id,
       md5(coalesce(string_agg(md5(row_to_json(scoped)::text), ''
                                ORDER BY id), '')) AS row_digest
FROM scoped;
SELECT h.id, h.municipio_geocodigo, h."SE", h.casos_est, h.nivel
FROM "Municipio".:"alert_table" AS h
JOIN "Dengue_global"."Municipio" AS m
  ON m.geocodigo = h.municipio_geocodigo
JOIN "Dengue_global"."estado" AS e ON e.nome = m.uf
WHERE e.uf = 'MG' AND h."SE" = :epiweek
ORDER BY h.id LIMIT 20;
```

Replace `2026XX` with the approved numeric week before execution. The digest
compares the complete scoped rows without dumping them; compare changed IDs
separately if a digest differs. Also capture the matching pipeline workflow,
inputs, and analysis job IDs/fingerprints before and after.

## Controlled commands after all gates pass

These are prepared commands for the staging operator, **not executed** in this
preparation. First finish the gates below, including independent staging-only
endpoint review, the deployed digest check, prefix exclusion from every other
consumer and legacy mirror, the active single watcher, verified backup restore,
and a fresh approved fixture. Use the existing issue #9 prefix-isolation
procedure; the production validator does not prove ownership. Confirm the
analysis database role has the needed write grants. Set `analysis_load=true`
in both private production-profile worker files, retaining
`environment=prod`, staging-only URLs, and matching immutable image digests.
Recreate the five existing workers so no service retains a stale environment;
do not recreate the database or broker. The core file setting matters to
`worker-orchestration`, which persists `load` on a **new** job claim.

```sh
cd /opt/services/infodengue/infodengue-pipeline
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --images
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-deps --no-build --pull never --force-recreate \
  worker-ingestion worker-events worker-orchestration worker-analysis worker-beat
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
systemctl --user is-active infodengue-sinan-watcher.service
```

Check the rendered **sanitized** routing and runtime health without printing
full `docker compose config` or private env files. `uv run makim prod.validate`
is intentionally unavailable in this mode because it requires
`analysis_load=false`. Submit the newly approved fixture exactly once through
the isolated staging MinIO prefix using the existing writer and watcher path;
record key, object identity, size, and SHA-256. Then observe, without manual
retry or resubmission:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  logs --since 30m worker-ingestion worker-events worker-orchestration worker-analysis worker-beat
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
```

Use read-only pipeline and scientific SQL to verify the exact SHA/run,
workflow/fingerprint, one `load=true` job with `status=succeeded` and
`attempts=1`, artifact checks, bounded before/after snapshots, and advisory
lock release as described below. Inspect the current pipeline schema before
using the following job queries. Run them before and after in a read-only
transaction; replace the placeholders with the approved numeric week and
fresh fixture SHA-256. Do not substitute a historical SHA or change a stored
`load=false` job. The specific object submission command and final week remain
unset until a fresh fixture and isolated writer path are approved.

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  exec -T postgres psql -U pipeline_prod -d pipeline_prod
```

```sql
\set epiweek 2026XX
\set sha REPLACE_WITH_APPROVED_FRESH_SHA256
BEGIN TRANSACTION READ ONLY;
SELECT object_id, bucket, object_key, object_identity, status, sha256, run_id
FROM pipeline.ingress_object WHERE sha256 = :'sha' ORDER BY created_at;
SELECT w.workflow_id, w.coverage, w.epiweek, w.status,
       w.queued_fingerprint, i.disease, i.run_id, i.fingerprint
FROM pipeline.workflow AS w
LEFT JOIN pipeline.workflow_input AS i ON i.workflow_id = w.workflow_id
WHERE w.coverage = 'MG' AND w.epiweek = :epiweek
ORDER BY i.disease;
SELECT j.job_id, j.workflow_id, j.input_fingerprint, j.status, j.load,
       j.attempts, j.image, j.output_dir, j.exit_code
FROM pipeline.analysis_job AS j
JOIN pipeline.workflow AS w ON w.workflow_id = j.workflow_id
WHERE w.coverage = 'MG' AND w.epiweek = :epiweek
ORDER BY j.created_at, j.job_id;
COMMIT;
```

The pipeline state may contain earlier fingerprints; identify the job for the
newly observed workflow fingerprint and require exactly one row for that
workflow/fingerprint. Query the staging AlertaDengue `ingestion.run` through
its read-only role for the same SHA, including `status`, `disease`,
`canonical_cid10`, `delivery_se`, `uf`, and `finished_at`.

## Existing execution path

The existing MinIO ingress materializes a file for the AlertaDengue watcher.
The completion bridge reads `ingestion.run`, emits the canonical disease input,
and workflow readiness dispatches one persistent analysis job per workflow and
input fingerprint. The analysis worker runs with concurrency one. A `load=true`
job holds a PostgreSQL advisory lock on the scientific target throughout the
Makim process and artifact checks. Failed jobs require an explicit retry; a
rehearsal failure must be preserved for investigation. The existing staging E2E
validator and production profile validator require `analysis_load=false`, so
their success does not establish SQL application or map generation.

## Gate before submission

Perform these checks on the staging host and record evidence outside the
repository. Stop on any unknown or failed check.

1. Confirm the approved fixture is a valid SINAN CSV or DBF, record its SHA-256,
   and use the read-only source role to verify that the SHA-256 is absent from
   **all** `ingestion.run` statuses. Record its expected UF, epiweek, and
   canonical disease code. The bridge accepts `disease=A90`, `A92.0`, or
   `A928` with a null `canonical_cid10`, and the coarse `disease=A92` only
   when `canonical_cid10` is `A92.0` or `A928`. It also accepts a matching
   canonical CID alongside an exact disease code. Bare `A92` is ambiguous,
   and conflicting disease and canonical CID values are rejected.
2. Verify a recent staging scientific database backup by checking its restore
   result and selected table/row checks. Record backup identity and verification
   time. Do not make the rehearsal the first test of the backup.
3. Inspect the rendered production-profile Compose configuration and both
   private worker environments without printing secrets. Resolve every database
   hostname, MinIO endpoint, bucket/prefix, incoming bind, and analysis output
   bind to staging resources. Confirm the analysis role has the intended staging
   write grants; the ingestion source role remains read-only. Confirm immutable
   analysis image identity, one analysis worker at concurrency one, and the
   existing watcher as the sole owner of the incoming path. Keep the legacy
   MinIO mirror excluded from the pipeline prefix.
4. Capture bounded, read-only before snapshots for the selected UF and epiweek
   in `Historico_alerta`, `Historico_alerta_chik`, and
   `Historico_alerta_zika`, where those tables exist. Inspect the scientific
   schema first and use its actual UF/week columns and disease keys. Record
   counts and stable row identifiers, not broad table dumps. Also record the
   current workflow and analysis job identities for that scope.

## Controlled execution and acceptance

Only after every gate passes, configure the production-profile workers for
`INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=true`, retaining staging-only endpoints.
Submit the fixture once through the existing MinIO/watcher path. Do not invoke
an ingestion or analysis retry during this run.

Correlate the exact object identity and SHA-256 to one completed `ingestion.run`
row, its workflow input, workflow fingerprint, and analysis job. Wait for that
job's terminal state. Require `status=succeeded`, `attempts=1`, `load=true`,
and one job for the workflow/fingerprint. On failure, preserve the job, logs,
artifacts, and database state; do not reset or retry it automatically.

Inspect the job's own output directory. Validate RData with the scientific R
loader, not just file size. Check generated dengue and chikungunya SQL is
non-empty and parses against the staging scientific schema. Check Zika SQL
when the pinned AlertaDengueAnalise image and selected disease inputs generate
it; record an explicit exclusion otherwise. Verify the scientific process
reported successful SQL application, then compare bounded after snapshots for
the same UF/week and expected disease tables. Review the actual row changes,
including any expected unchanged disease table. Validate expected map PNGs by
decoding them and checking they correspond to the selected scope.

Finally, from a separate staging scientific database session, verify the
analysis advisory lock is no longer held. Compare job counts and scientific
execution logs for the exact job/fingerprint to rule out duplicate execution.
Record the image digest, fixture SHA-256, coverage, epiweek, disease inclusion
and exclusions, backup identity, before/after results, artifact paths, job ID,
status, and attempts. This establishes staging acceptance only; it is not
production cutover.
