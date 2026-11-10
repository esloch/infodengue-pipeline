# Recife intramunicipal bulletin pilot

This is a manual development-only workflow. It reads the existing `Municipio`
notification and alert-history tables in one read-only transaction. It writes
CSVs, a QMD, logs, a PDF when rendering succeeds, and `manifest.json` in a new
UUID-named directory under `.local/bulletins`. It never writes to the source DB,
publishes the PDF, or schedules a Celery task.

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

The runner preserves source notification IDs as text, and exports source date
and epidemiological-week values without recalculation. Dengue history is always
required. Chikungunya history is required only when source chikungunya
notifications exist. The runner rejects missing data and duplicate IDs or weeks
before calling R. The upstream package validates the spatial reference and
generates the QMD; Quarto renders the PDF. An attempt's manifest records status,
source hashes, package version after generation, and output paths. No existing
attempt directory is overwritten.

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

New rows start `queued` with zero attempts and no result. A future executor
may transition `queued → running → succeeded/failed`; a failed job requires
an explicit retry back to `queued`. That transition and the attempt increment
must be atomic when execution is implemented. Creation and retrieval are
available through `SqlBulletinJobRepository` within the existing Unit of Work.
There is currently no automatic claim, dispatch, or rendering from this table.

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

## Current development data limitation

The development `Municipio.Notificacao` table has one row overall and none for
Recife. Both Recife history tables have 865 rows through SE 202630. A real
Recife QMD/PDF cannot be produced from this data. The runner intentionally fails
the completeness gate until approved notifications and a neighborhood reference
are available. No real SINAN data is stored in this repository.
