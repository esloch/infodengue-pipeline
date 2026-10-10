# Architecture

InfodenguePipeline is a standalone ingestion and workflow coordination service.
It owns ingestion state and future workflow coordination. It does not own the
public web application or scientific calculations.

```text
MinIO transient ingress → ingestion → StorageBox canonical files
                                  ↓
                         PostgreSQL processed state
                                  ↓ events
                finite AlertaDengueAnalise OCI job
                                  ↓ validated artifacts
                   AlertaDengue runtime refresh
```

MinIO is transient ingress. StorageBox is the canonical imported file source
of truth. PostgreSQL stores processed data and pipeline state. AlertaDengue
remains the web/API application. AlertaDengueAnalise remains a finite,
independently executable scientific job. This repository will own ingestion
and workflow coordination after a phased migration.

The code uses domain and application modules behind ports. SQLAlchemy,
RabbitMQ/Celery, MinIO, and filesystem adapters implement infrastructure
boundaries. Phase 0 provides state and contracts; it does not yet parse or
merge SINAN data or launch external jobs.

# Environment boundary

The Compose stack is development-only, binds published ports to loopback, and
uses local named volumes. Production endpoints and credentials are not
configured by this repository's development defaults.
