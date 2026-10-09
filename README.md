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

Install Python 3.11 and `uv`, then run `uv sync`. The first implementation
branch will add the development services and quality tasks.

## License

GNU General Public License version 3. See [LICENSE](LICENSE).
