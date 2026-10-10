# Development

Requirements: Python 3.11, uv, Docker Compose, and containers-sugar.

```bash
uv sync --extra dev
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
sugar --profile dev compose up -d
uv run makim db.upgrade
uv run makim dev.smoke
```

To validate Compose without starting services: `makim containers.config`.
Stop and remove local containers with `makim containers.down`; named volumes
are retained unless explicitly removed. The smoke test checks only local dev
services and must never use production configuration.
