"""Validate one explicitly selected MinIO object through staging analysis."""

from __future__ import annotations

import argparse
import time

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import TypeVar
from uuid import UUID

from infodengue_pipeline.analysis.runner import validate_outputs
from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    IngressObjectRow,
    IngressStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.ingestion.completion import (
    build_source,
    resolve_run_disease,
)
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobStatus,
    Coverage,
    Disease,
    Epiweek,
    IngestionInput,
    Readiness,
    WorkflowStatus,
)
from infodengue_pipeline.storage.ingress import object_identity
from infodengue_pipeline.storage.minio import build_object_store
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

T = TypeVar('T')


@dataclass(frozen=True)
class CurrentWorkflow:
    """The resolved workflow identity and its current input fingerprint."""

    workflow_id: UUID
    coverage: Coverage
    epiweek: Epiweek
    fingerprint: str
    status: WorkflowStatus


def wait_for(stage: str, timeout: float, read: Callable[[], T | None]) -> T:
    """Poll one state with a bounded timeout that names the missing stage."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = read()
        if result is not None:
            return result
        time.sleep(min(2.0, max(0.1, deadline - time.monotonic())))
    raise TimeoutError(f'timed out waiting for {stage}')


def load_ingress(
    sessions: sessionmaker[Session],
    bucket: str,
    object_key: str,
    identity: str,
) -> IngressObjectRow | None:
    """Load the ingress record for the current MinIO object version."""
    with sessions() as session:
        return session.scalar(
            select(IngressObjectRow).where(
                IngressObjectRow.bucket == bucket,
                IngressObjectRow.object_key == object_key,
                IngressObjectRow.object_identity == identity,
                IngressObjectRow.status.in_(
                    (
                        IngressStatus.MATERIALIZED,
                        IngressStatus.INGESTED,
                        IngressStatus.FAILED,
                    )
                ),
            )
        )


def load_workflow_input(
    sessions: sessionmaker[Session], run_id: UUID
) -> WorkflowInputRow | None:
    """Find the pipeline input that contains this exact completed run ID."""
    with sessions() as session:
        return session.scalar(
            select(WorkflowInputRow).where(WorkflowInputRow.run_id == run_id)
        )


def load_current_workflow(
    sessions: sessionmaker[Session],
    workflow_input: WorkflowInputRow,
    settings: Settings,
) -> CurrentWorkflow | None:
    """Resolve a ready workflow and fingerprint from all current inputs."""
    with sessions() as session:
        workflow = session.get(WorkflowRow, workflow_input.workflow_id)
        if workflow is None:
            return None
        input_rows = session.scalars(
            select(WorkflowInputRow).where(
                WorkflowInputRow.workflow_id == workflow.workflow_id
            )
        ).all()
        inputs: dict[Disease, IngestionInput] = {}
        for row in input_rows:
            completed_at = row.completed_at
            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(tzinfo=timezone.utc)
            else:
                completed_at = completed_at.astimezone(timezone.utc)
            inputs[row.disease] = IngestionInput(
                row.run_id, row.fingerprint, completed_at
            )
        coverage = Coverage(workflow.coverage)
        epiweek = Epiweek.parse(workflow.epiweek)
        required = (
            settings.br_required_diseases
            if coverage is Coverage.BR
            else settings.uf_required_diseases
        )
        readiness = Readiness(coverage, epiweek, inputs, required)
        fingerprint = readiness.input_fingerprint
        if fingerprint is None or workflow.status is WorkflowStatus.WAITING:
            return None
        return CurrentWorkflow(
            workflow.workflow_id,
            coverage,
            epiweek,
            fingerprint,
            workflow.status,
        )


def load_analysis_job(
    sessions: sessionmaker[Session], workflow: CurrentWorkflow
) -> AnalysisJobRow | None:
    """Load only the job for this workflow's current input fingerprint."""
    with sessions() as session:
        return session.scalar(
            select(AnalysisJobRow).where(
                AnalysisJobRow.workflow_id == workflow.workflow_id,
                AnalysisJobRow.input_fingerprint == workflow.fingerprint,
            )
        )


def load_terminal_analysis_job(
    sessions: sessionmaker[Session], workflow: CurrentWorkflow
) -> AnalysisJobRow | None:
    """Return the exact analysis job after it reaches a terminal state."""
    job = load_analysis_job(sessions, workflow)
    if job is None or job.status not in {
        AnalysisJobStatus.SUCCEEDED,
        AnalysisJobStatus.FAILED,
    }:
        return None
    return job


def validate_configuration(
    settings: Settings, object_key: str, timeout: float
) -> None:
    """Reject unsafe staging validation settings before polling services."""
    if settings.environment != 'staging':
        raise ValueError('staging E2E requires environment=staging')
    if settings.alerta_source_database_url is None:
        raise ValueError('AlertaDengue source database is not configured')
    if settings.analysis_load:
        raise ValueError('staging E2E requires analysis_load=false')
    if not settings.minio_prefix or not object_key.startswith(
        settings.minio_prefix
    ):
        raise ValueError(
            'object key must be under the configured fixture prefix'
        )
    if timeout <= 0:
        raise ValueError('timeout-seconds must be positive')


def main() -> int:
    """Validate one explicit object and its exact staging analysis job."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--object-key', required=True)
    parser.add_argument('--timeout-seconds', type=float, default=1800)
    args = parser.parse_args()
    settings = get_settings()
    validate_configuration(settings, args.object_key, args.timeout_seconds)

    sessions = session_factory()
    metadata = build_object_store(settings).stat(args.object_key)
    identity = object_identity(metadata)
    ingress = wait_for(
        'ingress materialization',
        args.timeout_seconds,
        lambda: load_ingress(
            sessions, metadata.bucket, args.object_key, identity
        ),
    )
    if ingress.status is IngressStatus.FAILED:
        raise RuntimeError(
            f'ingress materialization failed: {ingress.last_error}'
        )
    if not ingress.sha256:
        raise RuntimeError('materialized ingress has no SHA-256')

    source = build_source(
        settings.alerta_source_database_url.get_secret_value()
    )
    run = wait_for(
        f'completed AlertaDengue ingestion.run for sha256={ingress.sha256}',
        args.timeout_seconds,
        lambda: source.by_sha256(ingress.sha256 or ''),
    )
    if run.sha256 != ingress.sha256:
        raise RuntimeError('completed run SHA-256 does not match ingress')
    run_disease = resolve_run_disease(run.disease, run.canonical_cid10)
    workflow_input = wait_for(
        f'workflow input for run_id={run.run_id}',
        args.timeout_seconds,
        lambda: load_workflow_input(sessions, run.run_id),
    )
    if workflow_input.run_id != run.run_id:
        raise RuntimeError(
            'workflow input run_id does not match completed run'
        )
    if workflow_input.disease is not run_disease:
        raise RuntimeError(
            'workflow input disease does not match completed AlertaDengue run'
        )
    if workflow_input.fingerprint != ingress.sha256:
        raise RuntimeError(
            'workflow input fingerprint does not match ingress SHA-256'
        )
    bridged_ingress = load_ingress(
        sessions, metadata.bucket, args.object_key, identity
    )
    if bridged_ingress is None or bridged_ingress.run_id != run.run_id:
        raise RuntimeError(
            'ingress run_id does not match completed AlertaDengue run'
        )

    workflow = wait_for(
        f'ready workflow for run_id={run.run_id}',
        args.timeout_seconds,
        lambda: load_current_workflow(sessions, workflow_input, settings),
    )
    job = wait_for(
        f'analysis job for fingerprint={workflow.fingerprint}',
        args.timeout_seconds,
        lambda: load_analysis_job(sessions, workflow),
    )
    if (
        job.workflow_id != workflow.workflow_id
        or job.input_fingerprint != workflow.fingerprint
    ):
        raise RuntimeError(
            'analysis job does not match current workflow inputs'
        )
    job = wait_for(
        f'analysis job completion for fingerprint={workflow.fingerprint}',
        args.timeout_seconds,
        lambda: load_terminal_analysis_job(sessions, workflow),
    )
    if job.status is AnalysisJobStatus.FAILED:
        raise RuntimeError(f'analysis job failed: {job.last_error}')
    current_workflow = load_current_workflow(
        sessions, workflow_input, settings
    )
    if (
        current_workflow is None
        or current_workflow.fingerprint != workflow.fingerprint
    ):
        raise RuntimeError(
            'workflow inputs changed during staging E2E validation'
        )

    request = AnalysisJobRequest(
        job.job_id,
        workflow.coverage,
        workflow.epiweek,
        workflow.fingerprint,
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
