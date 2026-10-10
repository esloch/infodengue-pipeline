# Incremental migration plan

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Repository, bootstrap, and foundation | This PR |
| 1 | Port framework-neutral SINAN parser, mover, and manifest logic | Next |
| 2 | Inventory exact database schema and define Alembic baseline | Planned |
| 3 | Port stage/merge operations to SQLAlchemy/SQL | Planned |
| 4 | Compare staging ingestion against current behavior | Planned |
| 5 | Move watcher and MinIO ownership in staging | Planned |
| 6 | Transactional outbox and `sinan.ingestion.completed.v1` | Foundation present; connect to ingestion in a later phase |
| 7 | A90/A92 readiness and real analysis runner | Readiness foundation present |
| 8 | Validate/publish artifacts, invoke runtime refresh, verify public service | Planned |
| 9 | Production cutover | Planned |
| 10 | Remove ingestion execution code and dependencies from AlertaDengue | Planned |

No AlertaDengue changes happen in Phase 0. Production ingestion remains in its
current location until staging parity, operational readiness, and a separately
approved cutover are complete. Production MinIO ownership has not moved.
