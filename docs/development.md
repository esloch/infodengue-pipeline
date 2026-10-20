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
a version pin; the derivative image pins Python 3.11 to satisfy the pipeline
package requirement. Before running a real scientific job, copy `.env.example`
to a private development worker env file, set
`INFODENGUE_PIPELINE_ALERTA_DATABASE_URL` to the development database, and
export `PIPELINE_WORKER_ENV_FILE=../<your-dev-env-file>` before Compose starts.

Automated tests use isolated SQL state and fake MinIO, completion source, and
analysis runner boundaries. PostgreSQL-specific locking still requires a
disposable PostgreSQL integration environment for validation. Tests do not
run the real R workload.

The real staging validator is a separate guarded operation. It requires a
safe supplied fixture key, staging-only configuration, and live staging
services. See [operations](operations.md). Compose starts `worker-analysis`
with the scientific runtime and its dedicated `pipeline.analysis` queue.
