# Operations

Phase 0 operational resources are limited to local development Compose
services: PostgreSQL, RabbitMQ, MinIO, and three queue-specific Celery workers.
No worker mounts a Docker socket. Credentials are development placeholders.

Production operation, alerting, backups, retries, and cutover procedures are
not yet implemented. When a scientific job and validated outputs are ready,
the later pipeline may invoke AlertaDengue's stable runtime refresh contract:

```text
uv run makim deployment.refresh-runtime --env prod --week YYYYWW
```

That operation is documented only and is not invoked by this repository in
Phase 0. It must run after scientific job success, output validation, and
artifact publication. Cache invalidation and public verification remain owned
by the external runtime contract.
