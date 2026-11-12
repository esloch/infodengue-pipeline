# Recife intramunicipal bulletin pilot

This development-only workflow reads the existing `Municipio`
notification and alert-history tables through read-only transactions (one per
database when source and scientific target differ). It writes
CSVs, a QMD, logs, a PDF when rendering succeeds, and `manifest.json` in a new
UUID-named directory under `.local/bulletins`. It never writes to the source DB,
publishes the PDF. Celery scheduling is disabled by default.

## Prerequisites

The dedicated image extends the scientific analysis image and installs the
upstream package at commit `fe82d1d5f6caf0a91aca585f0385cf1abea2e953`,
Quarto 1.11.9 with its verified release checksum, and TeX. The base image has
R 4.5.3, AlertTools 1.1.0, INLA 26.1.26.1, and tinytex 0.60; it lacks Quarto,
the intramunicipal package, and nowcaster. Nowcasting is disabled. Build this
image separately from the Python workers; do not start the normal Compose stack
to run a bulletin.

Provide an approved Recife **neighborhood** `sf` reference as RDS/RData with
`nm_bairro`, `id_bairro`, `distrito`, `pop`, and `geometry`, or update the column
names in `configs/bulletin/recife.json` to the verified names. The existing
`static/shapefile/2611606.shp` contains one municipal polygon and is unsuitable.
The config's thresholds and climate parameter are transcribed from the Recife
A90 row of `boletins/Estaduais/dados/parameters.csv`. Population is taken from
the latest source alert-history row in the selected period.

Copy `.envs/.env.bulletin.example` to `.envs/.env.bulletin`, then set a
development read-only source URL there. Set
`PIPELINE_BULLETIN_ENV_FILE=../.envs/.env.bulletin` for the Compose invocation.
Then run:

```sh
export RECIFE_REFERENCE_DIR=/absolute/path/to/approved/reference
export RECIFE_REFERENCE_FILE=recife_bairros.rds
export BULLETIN_WEEK=202630
export PIPELINE_BULLETIN_ENV_FILE=../.envs/.env.bulletin
docker compose -f containers/compose-bulletin.yaml --profile bulletin build bulletin-runner
docker compose -f containers/compose-bulletin.yaml --profile bulletin run --rm bulletin-runner
```

For QMD-only diagnostics on a host with the R package installed:

```sh
INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL='postgresql+psycopg://...' \
  python -m infodengue_pipeline.bulletin.runner --week 202630 \
  --reference /absolute/path/recife_bairros.rds --qmd-only
```

The runner preserves every source notification row, including identifiers that
repeat with different dates, weeks, or neighborhoods. An identifier is not a
scientifically approved deduplication key. The export retains notification
week (`ano_notif`, `se_notif`) separately from symptom-onset week
(`se_sin_pri`); it does not recalculate either or change the scientific case
definition. The source query includes scientific chikungunya CID aliases
`A92`, `A920`, and `A92.0`. It includes digitizations from 2018-01-01 through the Saturday
ending the selected CDC epiweek, using an inclusive lower and exclusive
next-day SQL boundary. For SE 202631 the last included date is 2026-08-08.
Missing or invalid onset dates remain in the export; the manifest counts them
and reports neighborhood ID/name coverage. Source date and week values are
exported unchanged. Dengue history is always
required. Chikungunya history is required only when source chikungunya
notifications exist. The runner rejects missing data and duplicate history weeks
before calling R. The upstream package validates the spatial reference and
generates the QMD; Quarto renders the PDF. An attempt's manifest records status,
source hashes, package version after generation, and output paths. No existing
attempt directory is overwritten.

`AlertTools::getCases()` defaults to digitizations from 2018-01-01 through
`lastday` inclusive. With `dataini="sinpri"`, it drops missing onset dates and
onset dates outside the 0-to-365-day notification delay, then calculates onset
epiweeks. The bulletin preserves the source rows and additionally bounds by
**notification week**, preserving the existing renderer input contract. These
populations can therefore differ; compare aggregate counts before scientific
acceptance. The geographic reference sums to 1,540,874 residents, while the
municipal alert history has a different denominator. The runner passes the
municipal history population to the renderer and does not adjust either value.

The existing staging RDS was adapted from
`AlertaDengue/intramunicipal/recife/ref_RF.RData`. Keep that source unchanged.
The [PR30 hash record](../staging-data/pr30-recife-reference.json) identifies
the original RData (`5a4262f62d750007ea61eaa6425e4c06071fdee5ce57000003898e4a6b45c26e`)
and adapted RDS (`4c59f5c1f0071d652ed470a5472432e269c2bcceb0a18c584887e12c583b8b7e`).
The exact prior adaptation steps and scientific approval are not documented.
Check both known hashes without modifying either file:
`python3 scripts/validate_pr30_reference.py /path/to/recife_bairros.rds --source /path/to/ref_RF.RData`.

For a new adaptation, use the separate reviewable adapter workflow.
The adapter `scripts/prepare_recife_reference.R` requires a manifest with
SHA-256 hashes for the upstream geometry and exact name-to-population mapping.
For Recife, set `expected_neighborhoods` to 94, `expected_districts` to 6,
and `expected_population` to 1540874. It rejects duplicate IDs/names,
incomplete joins, missing CRS, and empty, invalid, or non-polygon geometry.
Any Bongi repair must be represented as a separate, hashed input
transformation with its own provenance; this adapter never repairs geometry
implicitly. Its new output cannot substitute for the known RDS hash without
separate review.

## Execution readiness

`CheckBulletinReadiness` is a read-only application check; it does not create
or dispatch a bulletin job. For a requested week, it requires a succeeded PE
workflow whose queued fingerprint matches its current ingestion inputs, and a
matching succeeded analysis job with exit code 0, `load=true`, and a finish
timestamp. It rechecks the analysis RData, SQL, and map artifacts using the
existing analysis output validator. This separates generated artifacts from
the successful `load=true` run needed to apply history to the analysis
database.

The check also validates the Recife configuration and nonempty neighborhood
reference using the runner's shared checks, then reads the configured alert
database through the runner's read-only snapshot and its existing notification
and history validators. That requires current-week Recife dengue notifications
and dengue history, valid population data, and chikungunya history when current
notifications include chikungunya. The pipeline job fingerprint is checked
against current workflow inputs; the history tables do not yet persist an
analysis-job fingerprint, so the check cannot independently prove which exact
input fingerprint produced a matching history row.

## Durable job record

`pipeline.bulletin_job` stores a Recife municipality job for one bulletin
kind, scientific analysis job, and renderer revision. The analysis job
identifies the PE workflow, epidemiological week, and scientific input
fingerprint, so those workflow fields are not copied into the bulletin row.
The unique constraint on kind, municipality geocode, analysis job ID, and
renderer revision prevents duplicate logical jobs. A new scientific
fingerprint or renderer revision permits a new row.

New rows start `queued` with zero attempts and no result. The dedicated
`pipeline.bulletin` worker rechecks readiness, claims a job atomically, then
renders outside a PostgreSQL transaction. It records the PDF path or a safe
failure label. Duplicate delivery does not start another render. Failed jobs
require an explicit `pipeline.bulletin.retry` task; interrupted jobs become
failed after the configured timeout. Expired dispatch leases allow queued
messages to be republished after a broker failure.

To enable the development schedule, export
`INFODENGUE_PIPELINE_BULLETIN_ENABLED=true` for Compose and start the `bulletin`
profile of `containers/compose.yaml`. This sets the same opt-in for Beat and the
bulletin worker. Supply the approved
reference directory as `RECIFE_REFERENCE_DIR`, with
`recife_bairros.rds` inside it, and set the read-only source URL in the bulletin
worker env file. The worker mounts analysis artifacts read-only and persists
PDFs in `pipeline_bulletin_output`. Keep the flag false until the source data,
reference, and image are verified. No PDF publication task is provided.

## Synthetic container integration test

The synthetic Feira Livre fixture can be tested without R, Quarto, or LaTeX on
the host. The test uses the existing `infodengue-pipeline-bulletin:dev` image,
generates the QMD and PDF in one temporary container, validates the PDF
structure, and removes the container and temporary artifacts afterward. It
does not connect to a database or use Recife data. The ordinary host test run
skips this test unless explicitly enabled. To require it, run:

```sh
RUN_BULLETIN_CONTAINER_INTEGRATION=1 \
  uv run pytest -q tests/integration/test_bulletin_package.py
```

With the opt-in set, missing Docker CLI/daemon or missing image causes a test
failure. The test does not build the image; build it separately with the
Compose command above when needed.

## PR30 isolated staging acceptance

The old pinned `alertadengueanalise:dev-container-worker` base lacks the
atomic loader. Rebuild the base from `AlertaDengueAnalise` source revision
`78114fbd497e3b447c942590361a860ea5fa241f`, which tracks
`scripts/load_alertas.py` and its `.makim.yaml` invocation. The analysis
Dockerfile checks those actual files and fails on the old base.
That loader validates both disease SQL files before a single PostgreSQL
transaction with `ON_ERROR_STOP=1`; failure rolls back both. Its upserts allow
retry only after target rows and SQL are reconciled. Never perform a live load
as part of PDF acceptance. Production use still requires explicit scientific,
data, reference, image, and deployment authorization.

After an operator verifies that the isolated target `recife_scientific_pr30`
contains the expected SE 202631 dengue and chikungunya histories, create a
private env file with **read-only**
`INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL` and
`INFODENGUE_PIPELINE_BULLETIN_HISTORY_DATABASE_URL` (the latter points to the
isolated target). Verify the database grants independently. The file must be
outside this repository and readable only by the operator. The staging RDS
must be `/opt/data/staging/pr30-rehearsal/geo/recife_bairros.rds`; artifacts
are written under `/opt/data/staging/pr30-rehearsal/bulletins`.
The existing RDS needs no invented sidecar. The script checks its known hash
and its sf contents in the disposable bulletin image.

```sh
source_dir="$(mktemp -d /tmp/pr30-scientific-source.XXXXXX)"
git -C ../AlertaDengueAnalise archive 78114fbd497e3b447c942590361a860ea5fa241f | tar -x -C "$source_dir"
docker build -t alertadengueanalise:pr30-atomic -f "$source_dir/containers/Dockerfile" "$source_dir"
docker build -t infodengue-pipeline-analysis:pr30 --build-arg ALERTA_ANALYSIS_BASE_IMAGE=alertadengueanalise:pr30-atomic -f containers/Dockerfile.analysis .
docker build -t infodengue-pipeline-bulletin:dev --build-arg PIPELINE_ANALYSIS_IMAGE=infodengue-pipeline-analysis:pr30 -f containers/Dockerfile.bulletin .
python3 scripts/validate_pr30_reference.py /opt/data/staging/pr30-rehearsal/geo/recife_bairros.rds
export BULLETIN_WEEK=202631
export BULLETIN_ENV_FILE=/private/path/pr30-bulletin-readonly.env
export EXPECTED_IMAGE_ID="$(docker image inspect --format '{{.Id}}' infodengue-pipeline-bulletin:dev)"
bash scripts/run_pr30_staging_bulletin.sh
attempt="$(find /opt/data/staging/pr30-rehearsal/bulletins -mindepth 1 -maxdepth 1 -type d -name 'recife-202631-*' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)"
jq '{status, reference_week, digitization_start, digitization_end_exclusive, notification_coverage, input_hashes, outputs}' "$attempt/manifest.json"
pdfinfo "$attempt/bulletin.pdf"
sed -n '1,120p' "$attempt/generate.log"
sed -n '1,120p' "$attempt/render.log"
```

The script checks the image ID, RDS hash, env keys, staging network, and
output readiness before running one disposable container. The runner writes
`notifications.csv`, both history CSVs when chikungunya is present,
`bulletin.qmd`, `bulletin.pdf`, `manifest.json`, `generate.log`, and
`render.log` in a new attempt directory. A failed attempt still has a failed
manifest and any available diagnostic log. No existing service is changed.
Record all three resulting image IDs in the acceptance record. The commands
never execute the scientific SQL loader.

Run host checks with `uv run ruff format --check .`, `uv run ruff check .`,
`uv run mypy src`, and `uv run pytest`. Set
`RUN_BULLETIN_CONTAINER_INTEGRATION=1` to require the optional container PDF
test when Docker and the built bulletin image are available.
