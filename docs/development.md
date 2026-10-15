# Development

Use Python 3.11 and `uv`. Conda and Pipenv are not required. `uv sync` creates
the project environment.

```sh
uv sync --extra dev --locked
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
uv run makim docs.build
uv run makim containers.config
```

Start local development services with `uv run makim containers.up`; apply the
pipeline schema with `uv run makim db.upgrade`. Stop services with
`uv run makim containers.down`. The Compose stack uses development-only
credentials, loopback published ports, and named volumes. It does not launch
SINAN analysis or perform staging deployment.

The guarded local service validation is `uv run makim dev.validate`. It
requires the explicitly configured development PostgreSQL, RabbitMQ, and
MinIO services. Staging filesystem validation is separate; see
[operations](operations.md).
