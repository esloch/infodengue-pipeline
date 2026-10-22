# Infodengue Pipeline

Standalone SINAN ingress coordination and finite AlertaDengueAnalise job
orchestration for InfoDengue. AlertaDengue remains the authority for content
inspection, canonical movement, manifest creation, stage, merge, and rollback.
This repository coordinates MinIO ingress, completion events, readiness,
durable analysis job state, and containerized scientific execution.

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
A clean staging E2E validation reached SUCCESS on 2026-10-22 for MG,
epiweek 202412, with `analysis_load=false`, one analysis attempt, and no
manual recovery after submission. It exercised ingress through RData and SQL
artifact checks. SQL application and map generation were not exercised. See
[operations](docs/operations.md) for the validated scope. No production
cutover is claimed.

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
