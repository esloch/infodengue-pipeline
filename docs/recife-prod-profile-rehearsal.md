# Recife bulletin production-profile rehearsal

Status: **BLOCKED for live execution** until the staging operator verifies the
gates below. The code path uses the existing production Compose topology with
`--profile bulletin`; the existing SINAN watcher remains the sole watcher.
No publication task or destination is configured.

## Authorization and configuration

Use a dedicated Compose project name and new, isolated pipeline PostgreSQL and
RabbitMQ volumes. Reuse only the approved staging integration network. Supply
reviewed immutable `repository@sha256:<64 lowercase hex>` references for all
three images, including `PIPELINE_BULLETIN_IMAGE`. Do not build on staging.
The bulletin image must contain the reviewed R package and Quarto revision.

Set these variables in the private production-profile env file, in addition
to the existing issue #17 settings:

```text
INFODENGUE_PIPELINE_ENVIRONMENT=prod
INFODENGUE_PIPELINE_ANALYSIS_LOAD=true
INFODENGUE_PIPELINE_BULLETIN_ENABLED=true
INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL=<read-only original staging source>
INFODENGUE_PIPELINE_BULLETIN_HISTORY_DATABASE_URL=<read-only isolated scientific target>
PIPELINE_BULLETIN_IMAGE=<reviewed immutable bulletin image>
PROD_ANALYSIS_OUTPUT_HOST_PATH=<dedicated staging analysis directory>
PROD_BULLETIN_OUTPUT_HOST_PATH=<dedicated staging bulletin directory>
RECIFE_REFERENCE_DIR=<approved Recife neighborhood reference directory>
RECIFE_REFERENCE_FILE=<approved neighborhood sf RDS filename>
BULLETIN_AUTH_HOST_PATH=<absolute path to reviewed authorization JSON>
```

The analysis worker's private env file must use `analysis_load=true` and a
write-capable URL for the isolated scientific target. The core/Beat and
bulletin worker env file carries read-only URLs for the original source and
the same isolated target. Ingestion correlation and notification extraction
use the original source; readiness and rendering read post-load history from
the target. Beat and the bulletin worker use the same core env file and
the same read-only authorization manifest mount. The normal production values
are `bulletin_enabled=false`, `analysis_load=false`, and a `/dev/null` manifest.

After independent staging resource review, create the authorization JSON
outside the repository. Its complete schema is:

```json
{
  "profile": "isolated-staging-rehearsal",
  "pipeline_database": "pipeline-postgres:5432/pipeline_prod",
  "source_database": "<verified-original-source>:5432/<source-db>",
  "analysis_database": "<verified-isolated-target>:5432/<target-db>",
  "minio_endpoint": "<verified-staging-minio-host>:9000",
  "minio_bucket": "<verified-exclusive-staging-bucket>",
  "minio_prefix": "<verified-exclusive-prefix>/",
  "renderer_revision": "<reviewed-renderer-revision>"
}
```

These are identity examples, not approved endpoints. The application requires
an exact JSON match, a nonempty prefix, `environment=prod`, both explicit
switches, and distinct approved staging source and target identities. Missing
or inconsistent settings fail closed. The manifest is an authorization record,
not proof of isolation by itself. Before creating it, probe the databases
through their separate runtime roles and verify the actual server identity
against the staging inventory. A copied development enable switch
alone cannot authorize production scheduling.

## Read-only preflight and stop gates

Start with the host, watcher, network, volume, MinIO prefix and image checks in
`docs/issue17-staging-acceptance.md`. Record checkout SHA and PR #30 revision;
confirm the host is staging. Inspect each resolved database and MinIO endpoint
without printing credentials or unredacted Compose output. Verify the selected
prefix is empty and exclusive, and no legacy mirror or concurrent consumer can
claim it. Keep the existing watcher running; do not start another.

Before `analysis_load=true`, run these read-only probes from the reviewed
images on the existing staging integration network. Use the core env file for
the source role and the analysis env file for the target role. The commands
print no URLs or credentials:

```sh
docker run --rm --network <verified-staging-network> \
  --env-file <private-core-env-file> <reviewed-core-image-digest> \
  python -m infodengue_pipeline.production.validate rehearsal-source
docker run --rm --network <verified-staging-network> \
  --env-file <private-analysis-env-file> <reviewed-analysis-image-digest> \
  python -m infodengue_pipeline.production.validate rehearsal-analysis \
  > /tmp/recife-writer-probe.json
docker run --rm --network <verified-staging-network> \
  --env-file <private-core-env-file> <reviewed-core-image-digest> \
  python -m infodengue_pipeline.production.validate rehearsal-history \
  > /tmp/recife-history-probe.json
uv run python -m infodengue_pipeline.production.validate rehearsal-target \
  --writer-probe /tmp/recife-writer-probe.json \
  --history-probe /tmp/recife-history-probe.json \
  --approved-database <DBA-approved-database> \
  --approved-address <DBA-approved-server-address> \
  --approved-port <DBA-approved-server-port>
```

The target comparison requires both server-reported identities to equal the
independently approved staging target. Compare the source probe separately
with its approved source identity and confirm it differs from the target.
The source role must have
`SELECT` but no write grants on notifications and historical tables. The
analysis role must have `SELECT`, `INSERT`, and `UPDATE` on both historical
tables and a writable session. The history role requires a read-only session,
`SELECT` on both histories, and no `INSERT`, `UPDATE`, or `DELETE` grants.
The probes issue only `SELECT` statements;
they do not prove whether unrelated staging processes can overwrite results.

The staging DBA must also verify that the analysis
target is a dedicated writable staging database, contains no production-only
state, has sufficient disk, has a tested backup/recovery procedure, and grants
the analysis role only the required writes. Inspect SQL produced by the pinned
AlertaDengueAnalise image: `pipeline.refresh-alertas-job --load true` applies
`output_dengue.sql` and `output_chik.sql` to its `ALERTA_DB_*` target. Confirm
that the bulletin history URL reads the same target, including
`Municipio.Historico_alerta` and `Municipio.Historico_alerta_chik`. The
analysis worker uses a PostgreSQL advisory lock, but other staging writers may
not honor it; exclude them for the test window. If any condition is unverified,
keep load disabled and stop. Do not treat a synthetic fixture as real data.

Verify a real approved PE SINAN object for the **selected** epidemiological
week, Recife geocode `2611606`, current notifications, dengue and required
chikungunya history, population, approved neighborhood geometry, and reviewed
thresholds/climate config. The municipal polygon is insufficient. Validate
readiness with the normal code path; preserve the selected week on failure.
Confirm file ownership, runtime UID/GID write access to the dedicated output
directory, read access to analysis output and reference, and Alembic head
compatibility before any migration. Confirm no publication service is enabled.

Use the existing read-only production validator for the core profile. Its
`analysis_load=false` assertion is intentional and must remain unchanged;
it does not authorize a write-enabled rehearsal. The scientific runner itself
retains its normal production behavior; rehearsal isolation is an operator
preflight and bulletin authorization gate. For the rehearsal, review
`docker compose --profile bulletin ... config --quiet`, `config --images`,
`config --services`, `ps --all`, image digests, service environments through
sanitized checks, migration heads, roles, and mounted paths. Do not emit bare
`config`. A missing verified immutable image or isolated scientific target is
a **BLOCKED** result.

## Controlled execution and evidence

Only after all gates pass, start the dedicated project with the reviewed env
file and `--profile bulletin`, `up -d --no-build --pull never`. Keep the normal
staging services, volumes and watcher untouched. Upload the exact approved
object under the exclusive prefix with the approved writer role. Record object
key, version/ETag, size and SHA-256. Never reuse a key or silently change week.

Run the read-only acceptance command inside the bulletin worker, with a bound
timeout longer than the scientific job:

```sh
docker compose -p <dedicated-project> --profile bulletin \
  --env-file <private-env-file> -f containers/compose-prod.yaml \
  exec -T worker-bulletin python scripts/run_prod_bulletin_acceptance.py \
  --object-key '<exact-key>' --sha256 '<64-character-sha256>' \
  --week <YYYYWW> --timeout-seconds 28800
```

The acceptance command correlates the exact object identity and hash,
watcher `ingestion.run`, PE workflow fingerprint, a successful `load=true`
scientific job, readiness, and one persisted bulletin job for the scientific
job and renderer revision. It requires a broker lease, one execution attempt,
success status, exit code, timestamps and a nonempty PDF header at the stored
artifact path. Preserve sanitized IDs, worker logs and the PDF manifest. A
dispatch lease alone is insufficient; success requires the worker's persisted
result. If a gate fails, report **BLOCKED** or **PARTIAL**, not PASS.

## Rollback

Stop only this project's `worker-beat`, `worker-bulletin`, and
`worker-ingestion` after confirming the existing watcher is not executing an
active ingestion. Preserve the pipeline and scientific databases, volumes,
broker state, MinIO objects, analysis output, bulletin artifacts and logs.
Restore legacy prefix routing only after the scanner and Beat have stopped.
Do not use `down -v`, prune, delete artifacts, or alter other staging services.
