"""Fail if the host-side analysis queue has no responding worker."""

import socket

from infodengue_pipeline.messaging.celery import celery_app


def main() -> int:
    """Ping the analysis worker independently of container workers."""
    destination = f'worker-analysis@{socket.gethostname()}'
    replies = celery_app.control.ping(timeout=5.0, destination=[destination])
    return 0 if replies else 1


if __name__ == '__main__':
    raise SystemExit(main())
