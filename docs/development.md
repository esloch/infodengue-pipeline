# Development

Requirements: Python 3.11, uv, Docker Compose, and containers-sugar.

Use standalone `uv` from a host or base shell. `uv sync` creates the
repository-local `.venv`; do not activate a project-specific Conda environment
or use Pipenv. Run project tools through `uv run` so they use that environment.

```bash
uv sync --extra dev
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
uv run sugar --profile dev compose up -d
uv run makim db.upgrade
uv run makim dev.smoke
```

To validate Compose without starting services: `uv run makim containers.config`.
Stop and remove local containers with `uv run makim containers.down`; named
volumes are retained unless explicitly removed. The smoke test checks only
local dev services and must never use production configuration.
