# Infodengue Pipeline

Standalone SINAN ingestion and workflow coordination for InfoDengue. The
project is an experimental foundation for gradual migration; production
ingestion continues to run in AlertaDengue. Staging E2E deployment, SINAN
inspection, stage/merge, and analysis execution are not implemented here.

## Development

Python 3.11 and `uv` are required. Docker Compose is used for local
infrastructure.

```sh
uv sync --extra dev --locked
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
uv run makim docs.build
```

## Architecture

The pipeline owns orchestration state and event delivery. AlertaDengue owns
the validated ingestion mover, stage/merge, and public runtime. See
[architecture](docs/architecture.md), [development](docs/development.md),
[operations](docs/operations.md), and [migration](docs/migration.md).

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
