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
uv run makim containers.up
uv run makim db.upgrade
```

Automated tests use isolated SQL state and fake MinIO, completion source, and
analysis runner boundaries. PostgreSQL-specific locking still requires a
disposable PostgreSQL integration environment for validation. Tests do not
run the real R workload.

The real staging validator is a separate guarded operation. It requires a
safe supplied fixture key, staging-only configuration, and live staging
services. See [operations](operations.md). The analysis worker runs on the
host with an OCI runtime: `uv run makim analysis.worker`; its independent
health check is `uv run makim analysis.health`.
