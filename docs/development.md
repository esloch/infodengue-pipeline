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

Start the stack and apply Alembic revisions with:

```sh
# Build the scientific base from its separate repository first.
docker build -t alertadengueanalise:dev-container-worker \
  -f "$ALERTA_ANALYSIS_SOURCE_DIR/containers/Dockerfile" \
  "$ALERTA_ANALYSIS_SOURCE_DIR"
uv run makim containers.up
docker compose --env-file .env.example -f containers/compose.yaml \
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
