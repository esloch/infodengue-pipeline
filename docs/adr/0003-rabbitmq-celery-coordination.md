# ADR 0003: RabbitMQ and Celery coordination

Status: Accepted

Use RabbitMQ as the initial broker and Celery for asynchronous worker
execution. Keep tasks thin and route ingestion, event, and orchestration work
to explicit queues. Do not add Redis without a demonstrated requirement.
