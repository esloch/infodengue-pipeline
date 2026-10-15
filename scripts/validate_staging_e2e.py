"""Guarded validator for one explicitly supplied safe staging SINAN object."""

from __future__ import annotations

import argparse
import time

from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from infodengue_pipeline.analysis.runner import validate_outputs
from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    AnalysisJobStatus,
    IngressObjectRow,
    IngressStatus,
    WorkflowRow,
)
from infodengue_pipeline.ingestion.completion import build_source
from infodengue_pipeline.models import AnalysisJobRequest, Coverage, Epiweek
from sqlalchemy import select

T = TypeVar('T')


def wait_for(stage: str, timeout: float, read: Callable[[], T | None]) -> T:
    """Poll one real staging state with a bounded, named timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = read()
        if result is not None:
            return result
        time.sleep(min(2.0, max(0.1, deadline - time.monotonic())))
    raise TimeoutError(f'timed out waiting for staging stage: {stage}')


def main() -> int:
    """Validate the staging path for one explicit MinIO object key."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--object-key', required=True)
    parser.add_argument('--timeout-seconds', type=float, default=1800)
    args = parser.parse_args()
    settings = get_settings()
    if settings.environment != 'staging':
        parser.error('staging E2E validation requires environment=staging')
    if settings.alerta_database_url is None:
        parser.error('AlertaDengue source database is not configured')
    if settings.analysis_load:
        parser.error('staging E2E requires analysis_load=false')
    if not settings.minio_prefix or not args.object_key.startswith(
        settings.minio_prefix
    ):
        parser.error(
            'object key must be under the configured staging fixture prefix'
        )
    sessions = session_factory()

    def ingress_read() -> IngressObjectRow | None:
        with sessions() as session:
            return session.scalar(
                select(IngressObjectRow)
                .where(
                    IngressObjectRow.object_key == args.object_key,
                    IngressObjectRow.bucket == settings.minio_bucket,
                )
                .order_by(IngressObjectRow.created_at.desc())
            )

    def materialized_read() -> IngressObjectRow | None:
        row = ingress_read()
        if row is not None and row.status in {
            IngressStatus.MATERIALIZED,
            IngressStatus.INGESTED,
            IngressStatus.FAILED,
        }:
            return row
        return None

    ingress = wait_for(
        'object materialized', args.timeout_seconds, materialized_read
    )
    if ingress.status is IngressStatus.FAILED:
        raise RuntimeError(
            f'ingress materialization failed: {ingress.last_error}'
        )
    if not ingress.sha256:
        raise RuntimeError('materialized ingress has no SHA-256')

    source = build_source(settings.alerta_database_url.get_secret_value())
    run = wait_for(
        'completed AlertaDengue run by sha256',
        args.timeout_seconds,
        lambda: source.by_sha256(ingress.sha256 or ''),
    )
    coverage = Coverage(run.uf)
    epiweek = Epiweek.parse(run.delivery_se)

    def workflow_read() -> WorkflowRow | None:
        with sessions() as session:
            row = session.scalar(
                select(WorkflowRow).where(
                    WorkflowRow.coverage == coverage,
                    WorkflowRow.epiweek == int(epiweek),
                )
            )
            if row is not None and row.status.value in {
                'ready',
                'queued',
                'running',
                'succeeded',
                'failed',
            }:
                return row
            return None

    workflow = wait_for('workflow ready', args.timeout_seconds, workflow_read)
    if workflow.status.value == 'failed':
        raise RuntimeError('workflow readiness failed')

    def job_read() -> AnalysisJobRow | None:
        with sessions() as session:
            return session.scalar(
                select(AnalysisJobRow)
                .where(
                    AnalysisJobRow.workflow_id == workflow.workflow_id,
                )
                .order_by(AnalysisJobRow.created_at.desc())
            )

    job = wait_for('analysis job queued', args.timeout_seconds, job_read)
    if job.status is AnalysisJobStatus.FAILED:
        raise RuntimeError(f'analysis job failed: {job.last_error}')
    if job.status is not AnalysisJobStatus.QUEUED:
        job = wait_for(
            'analysis job running',
            args.timeout_seconds,
            lambda: (
                candidate
                if (candidate := job_read()) is not None
                and candidate.status
                in {
                    AnalysisJobStatus.RUNNING,
                    AnalysisJobStatus.SUCCEEDED,
                    AnalysisJobStatus.FAILED,
                }
                else None
            ),
        )
    if job.status is not AnalysisJobStatus.SUCCEEDED:
        job = wait_for(
            'analysis job succeeded',
            args.timeout_seconds,
            lambda: (
                candidate
                if (candidate := job_read()) is not None
                and candidate.status
                in {
                    AnalysisJobStatus.SUCCEEDED,
                    AnalysisJobStatus.FAILED,
                }
                else None
            ),
        )
    if job.status is AnalysisJobStatus.FAILED:
        raise RuntimeError(f'analysis job failed: {job.last_error}')
    request = AnalysisJobRequest(
        job.job_id,
        coverage,
        epiweek,
        job.input_fingerprint,
        cores=job.cores,
        load=False,
    )
    validate_outputs(Path(job.output_dir), request)
    print(f'Staging E2E succeeded for object key {args.object_key}')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (TimeoutError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
