# Development

Use Python 3.11 and `uv`. Local PostgreSQL, RabbitMQ, and MinIO are provided
by the development Compose stack. Its credentials and bucket are local-only.

```sh
uv sync --extra dev --locked
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
uv run makim docs.build
uv run makim containers.config
uv run makim containers.staging-config
```

The repository uses a `src/` package layout. `models.py`, `events.py`,
`protocols.py`, and `services.py` contain framework-independent domain data,
contracts, and orchestration use cases. `db/` contains SQLAlchemy mappings,
repositories, Unit of Work, and durable analysis-job coordination;
`storage/`, `ingestion/`, `messaging/`, `analysis/`, and `workers/` adapt
MinIO, the AlertaDengue read-only completion source, RabbitMQ/Celery, the
scientific process, and Celery tasks. `staging/` and `production/` contain
guarded validation tooling. Keep business rules in services and persistence
contracts; task functions should remain adapters.

The test suite is organized as `tests/unit/`, `tests/contracts/`,
`tests/integration/`, plus CLI tests in `tests/test_cli.py`. The integration
tests exercise repository/job behavior using isolated local SQL state; fake
storage, completion sources, and analysis runners keep scientific work out of
the automated suite. `makim tests.unit` runs only unit tests and
`makim tests.all` runs the configured suite with a 90% coverage threshold.
`makim quality.all` runs Ruff format/lint, mypy, Bandit, and Vulture. The
strict docs build is `uv run makim docs.build`.

## Configuration and command reference

The `.envs/` directory contains tracked templates, not deployable secrets:

| Template | Purpose |
|---|---|
| `.env.example` | Development core services, pipeline DB/broker, MinIO ingress, read-only AlertaDengue source, analysis job defaults, and Compose path variables. |
| `.env.analysis.example` | Development analysis worker and scientific target DB settings; it intentionally omits MinIO and completion-source credentials. |
| `.env.staging.example` and `.env.staging.analysis.example` | Staging core and analysis worker settings. Replace every placeholder in private copies. |
| `.env.staging.e2e.example` | Dedicated staging fixture-writer credentials for one-shot E2E submission. |
| `.env.prod.example` and `.env.prod.analysis.example` | Production-profile placeholders; the documented first rehearsal is staging-only and uses `analysis_load=false`. |

Settings use the `INFODENGUE_PIPELINE_` prefix and reject unknown settings.
The canonical names and defaults are defined in `config.py`: database and
RabbitMQ URLs; MinIO endpoint, credentials, bucket, prefix, and polling
interval; `INCOMING_ROOT`; separate `ALERTA_SOURCE_DATABASE_URL` and
`ANALYSIS_DATABASE_URL`; analysis image, output root, cores, load mode,
initial epiweek, timeout, and load-lock timeout; disease readiness sets; and
bridge, outbox, analysis, and dispatch-retry intervals. Full variable names
map directly to these setting names, for example
`INFODENGUE_PIPELINE_ANALYSIS_INITIAL_EPIWEEK`. Set that initial week
explicitly and identically in the core worker and analysis worker files before
expecting analysis dispatch. Leave `ANALYSIS_LOAD=false` unless a controlled
write is intended. Keep secrets in private, permission-restricted files and
never put them in argv, logs, or committed templates.

Compose path resolution has two parts: `docker compose --env-file` paths are
interpreted from the shell's current directory; service `env_file` paths are
relative to the Compose file. The examples below assume commands run from the
repository root and therefore use `../.envs/...` for service-level paths.
Use the config checks before starting a stack; they render/validate settings
without starting services:

```sh
uv run makim containers.config
uv run makim containers.staging-config
uv run makim containers.prod-config
```

Makim task groups are declared in `.makim.yaml`: `quality.*`, `tests.*`,
`docs.build`, `db.upgrade`, `containers.*`, `dev.validate`, `staging.*`, and
`prod.validate`. Sugar's `.sugar.yaml` describes only the local development
profile and a contract placeholder for staging/production. It is not the
deployment definition; the corresponding `containers/compose-*.yaml` files
are authoritative.

The installed CLI entry point is `infodengue-pipeline`, also available as
`python -m infodengue_pipeline`. It currently provides `--version` and
`bootstrap-minio`. Bootstrap is restricted to the development environment and
the `sinan-infodengue-dev` bucket; it creates the local bucket and its
seven-day lifecycle. It is not a staging or production setup command.

Start the stack and apply Alembic revisions with:

```sh
# Build the scientific base from its separate repository first.
docker build -t alertadengueanalise:dev-container-worker \
  -f "$ALERTA_ANALYSIS_SOURCE_DIR/containers/Dockerfile" \
  "$ALERTA_ANALYSIS_SOURCE_DIR"
uv run makim containers.up
docker compose --env-file .envs/.env.example -f containers/compose.yaml \
  run --rm worker-orchestration alembic upgrade head
```

Set `ALERTA_ANALYSIS_SOURCE_DIR` to the AlertaDengueAnalise checkout. The
analysis worker Dockerfile installs the pipeline wheel and dependencies from
the pipeline lockfile, while its final container keeps the scientific Makim,
R, and INLA runtime. The scientific base currently resolves Python without
a version pin; the derivative image pins Python 3.11 until the separate
upstream runtime cleanup is merged. Core services use
`INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL` with a read-only role. Only
`worker-analysis` receives `PIPELINE_ANALYSIS_ENV_FILE` and
`INFODENGUE_PIPELINE_ANALYSIS_DATABASE_URL`; that file omits MinIO and
completion-source credentials. Provision dedicated MinIO service credentials
with least-privilege access to the selected bucket/prefix; do not use MinIO
root credentials for pipeline access.

Automated tests use isolated SQL state and fake MinIO, completion source, and
analysis runner boundaries. Advisory-lock behavior is covered with fake
PostgreSQL connections. No real `load=true` job or R workload is executed.

The real staging validator is a separate guarded operation. It requires a
safe supplied fixture key, staging-only configuration, and live staging
services. See [operations](operations.md). Compose starts `worker-analysis`
with the scientific runtime and its dedicated `pipeline.analysis` queue.

The AlertaDengueAnalise scientific base provides the Python 3.11 runtime.
The analysis worker image extends that runtime with the pipeline package and
its dependencies without replacing the scientific environment.
