"""Structured JSON logging configuration."""

import logging

import structlog


def configure_logging(level: str) -> None:
    """Configure structured logs without exposing secret settings."""
    logging.basicConfig(level=level.upper(), format='%(message)s')
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt='iso', utc=True),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
