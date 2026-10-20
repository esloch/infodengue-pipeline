FROM ghcr.io/astral-sh/uv:0.11.14 AS uv

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-install-project

COPY alembic.ini ./
COPY alembic ./alembic
COPY scripts ./scripts
COPY src ./src
RUN uv sync --frozen

ARG HOST_UID=1000
ARG HOST_GID=1000

RUN groupadd --gid "${HOST_GID}" pipeline \
    && useradd --create-home --uid "${HOST_UID}" --gid "${HOST_GID}" pipeline \
    && mkdir -p /var/lib/pipeline/incoming \
    && chown -R pipeline:pipeline /app /var/lib/pipeline

USER pipeline
ENTRYPOINT []
