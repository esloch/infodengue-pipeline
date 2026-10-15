# Infodengue Pipeline

Standalone SINAN ingress coordination and finite AlertaDengueAnalise job
orchestration for InfoDengue. AlertaDengue remains the authority for content
inspection, canonical movement, manifest creation, stage, merge, and rollback.
This repository coordinates MinIO ingress, completion events, readiness,
durable analysis job state, and OCI execution.

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
AlertaDengueAnalise finite OCI job
  ↓
validated RData and SQL output
```

The pipeline does not parse SINAN content, import Django or R code, apply
generated SQL by default, or own production storage. See
[architecture](docs/architecture.md), [development](docs/development.md),
[operations](docs/operations.md), and [migration](docs/migration.md).

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

The real staging validator is guarded and requires a safe, explicitly supplied
staging object key. It is not a substitute for automated orchestration tests.
No production cutover or staging E2E success is claimed by this repository.

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
