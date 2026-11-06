# Infodengue Pipeline

Infodengue Pipeline coordinates SINAN file ingress and finite
AlertaDengueAnalise jobs for Infodengue. It is a standalone Python 3.11 service
built with SQLAlchemy/Alembic, Celery, RabbitMQ, PostgreSQL, MinIO, Docker
Compose, and `uv`.

The pipeline owns MinIO materialization state, completion events and outbox,
workflow readiness, durable analysis job identity, dispatch, retry requests,
and execution status. AlertaDengue remains authoritative for SINAN content
inspection, canonical movement, manifests, stage, merge, rollback, and
ingestion. AlertaDengueAnalise owns scientific R/INLA computation and its
RData, SQL, optional SQL application, and map outputs. The pipeline invokes
those existing systems through their process and database contracts; it does
not import Django or R code or interpret SINAN contents.

```text
MinIO
  ↓
pipeline ingress and atomic materialization
  ↓
AlertaDengue incoming watcher and ingestion
  ↓
completed ingestion.run
  ↓
SHA-256 completion bridge and PostgreSQL outbox
  ↓
RabbitMQ / Celery and coverage + epiweek readiness
  ↓
one analysis_job per fingerprint
  ↓
worker-analysis with the AlertaDengueAnalise scientific runtime
  ↓
validated RData and SQL output
```

The pipeline does not parse SINAN content, import Django or R code, apply
generated SQL by default, publish bulletins, or provide a supported multi-host
deployment. The supported topology is one Docker host with persistent
PostgreSQL/RabbitMQ and local shared artifact storage. See
[architecture](docs/architecture.md), [development](docs/development.md),
[operations](docs/operations.md), [production](docs/production.md),
[migration](docs/migration.md), and the [future workload contract](docs/future-workloads.md).

## Quick start

From the repository root, first build the scientific base image using the
verified procedure in [development setup](docs/development.md#configuration-and-command-reference).
The analysis worker image depends on that image, so a clean checkout must
build it before starting Compose. Then install the locked development
dependencies, start the services, and apply the pipeline-owned migrations:

```sh
export ALERTA_ANALYSIS_SOURCE_DIR=/path/to/AlertaDengueAnalise
docker build -t alertadengueanalise:dev-container-worker \
  -f "$ALERTA_ANALYSIS_SOURCE_DIR/containers/Dockerfile" \
  "$ALERTA_ANALYSIS_SOURCE_DIR"
uv sync --extra dev --locked
uv run makim containers.up
uv run makim db.upgrade
```

The default development stack uses local-only credentials from
`.envs/.env.example`. The analysis worker image also needs the separate
AlertaDengueAnalise scientific runtime; see [development setup](docs/development.md)
before attempting scientific execution. `db.upgrade` targets the development
database at localhost port 5434. Do not use it as a staging or production
migration procedure.

For a given MinIO prefix, run only this pipeline's materializer after disabling
legacy MinIO synchronization. It stages and publishes complete files to the
existing AlertaDengue watcher; the watcher still owns canonical ingestion.
See [staging validation and rollback](docs/operations.md#staging-replacement-and-recovery-validation)
before changing synchronization ownership.

## Development

Python 3.11, `uv`, Docker Compose, PostgreSQL, RabbitMQ, and MinIO are used for
local development.

```sh
uv sync --extra dev --locked
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
uv run makim docs.build
uv run makim containers.config
```

`quality.all` runs Ruff formatting/lint checks, mypy, Bandit, and Vulture;
`tests.all` runs pytest with coverage; `docs.build` runs a strict MkDocs build.
The automated tests cover unit, contract, and integration behavior using
isolated SQL state and fakes. They do not execute a real R/INLA workload or
prove the behavior of shared staging or production services.

The real staging validator is guarded and requires a safe, explicitly supplied
staging object key. It is not a substitute for automated orchestration tests.
A clean staging E2E validation reached SUCCESS on 2026-10-22 for MG,
epiweek 202412, with `analysis_load=false`, one analysis attempt, and no
manual recovery after submission. It exercised ingress through RData and SQL
artifact checks. SQL application and map generation were not exercised. See
[operations](docs/operations.md) for the validated scope. No production
cutover is claimed.

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
