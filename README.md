# Infodengue Pipeline

Infodengue Pipeline is the standalone data ingestion and workflow
coordination service for InfoDengue.

**Status: Experimental / migration in progress.** Production SINAN ingestion
still runs in AlertaDengue. This repository is the foundation for an
incremental migration.

The intended flow is:

```text
MinIO ingress → SINAN ingestion → PostgreSQL → domain event
    → scientific analysis → artifact publication → runtime refresh
```

The project was bootstrapped with the [SciCookie OSL profile](https://github.com/osl-incubator/scicookie)
using a `src` layout, `uv`, Click, MkDocs Material, and GitHub Actions.

## Local development

Install Python 3.11, `uv`, Docker Compose, and containers-sugar. Then run:

```bash
uv sync --extra dev
uv run pre-commit run --all-files
uv run makim quality.all
uv run makim tests.all
sugar --profile dev compose up -d
uv run makim db.upgrade
uv run makim dev.smoke
```

The local stack binds only to loopback and uses development-only credentials
and named volumes. The smoke test never starts an analysis job. See
[development](docs/development.md) and [operations](docs/operations.md).

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
