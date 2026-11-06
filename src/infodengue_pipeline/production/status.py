"""Read-only operational summary for pipeline-owned PostgreSQL state."""

from datetime import datetime, timezone
from typing import Any, TypeVar
from uuid import UUID

import click

from sqlalchemy import Engine, create_engine, func, select
from sqlalchemy.orm import Session

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    Base,
    IngressObjectRow,
    IngressStatus,
    OutboxEventRow,
    OutboxStatus,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.models import AnalysisJobStatus, WorkflowStatus

RowT = TypeVar('RowT', bound=Base)


def _timestamp(value: datetime | None) -> str:
    """Format database timestamps predictably in UTC."""
    if value is None:
        return '-'
    aware = (
        value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    )
    return aware.astimezone(timezone.utc).isoformat()


def _recent(
    session: Session, model: type[RowT], order: Any, limit: int
) -> list[RowT]:
    """Fetch a bounded set of recent ORM records."""
    return list(
        session.scalars(select(model).order_by(order.desc()).limit(limit))
    )


def _count(session: Session, model: Any, condition: Any = None) -> int:
    """Count rows, optionally restricted by a status condition."""
    statement = select(func.count()).select_from(model)
    if condition is not None:
        statement = statement.where(condition)
    return int(session.scalar(statement) or 0)


def report(session: Session, limit: int = 20) -> str:
    """Return a credential-free operational report using bounded queries."""
    if limit < 1 or limit > 100:
        raise ValueError('limit must be between 1 and 100')
    lines = [f'Pipeline operational status (UTC; recent limit {limit})']

    ingress_groups = (
        ('Latest SINAN ingress objects', None),
        ('Failed ingress objects', IngressStatus.FAILED),
        (
            'Materialized ingress awaiting completion',
            IngressStatus.MATERIALIZED,
        ),
    )
    for title, status in ingress_groups:
        if status is None:
            ingress_rows = _recent(
                session,
                IngressObjectRow,
                IngressObjectRow.updated_at,
                limit,
            )
            condition = None
        else:
            condition = IngressObjectRow.status == status
            ingress_rows = list(
                session.scalars(
                    select(IngressObjectRow)
                    .where(IngressObjectRow.status == status)
                    .order_by(IngressObjectRow.updated_at.desc())
                    .limit(limit)
                )
            )
        lines.append(
            f'\n{title}: {_count(session, IngressObjectRow, condition)} total'
        )
        for ingress_row in ingress_rows:
            lines.append(
                f'  object_id={ingress_row.object_id} '
                f'bucket={ingress_row.bucket} '
                f'object_key={ingress_row.object_key} '
                f'sha256={ingress_row.sha256 or "-"} '
                f'run_id={ingress_row.run_id or "-"} '
                f'status={ingress_row.status.value} '
                f'updated_at={_timestamp(ingress_row.updated_at)}'
            )
        if not ingress_rows:
            lines.append('  (none)')

    pending = OutboxStatus.PENDING
    pending_rows = list(
        session.scalars(
            select(OutboxEventRow)
            .where(OutboxEventRow.status == pending)
            .order_by(OutboxEventRow.created_at.desc())
            .limit(limit)
        )
    )
    pending_count = _count(
        session, OutboxEventRow, OutboxEventRow.status == pending
    )
    lines.append(f'\nPending outbox events: {pending_count} total')
    for pending_row in pending_rows:
        lines.append(
            f'  event_id={pending_row.event_id} '
            f'event_type={pending_row.event_type} '
            f'status={pending_row.status.value} '
            f'created_at={_timestamp(pending_row.created_at)}'
        )
    if not pending_rows:
        lines.append('  (none)')

    lines.append('\nWorkflow states:')
    for workflow_status in sorted(WorkflowStatus, key=lambda item: item.value):
        workflow_count = _count(
            session,
            WorkflowRow,
            WorkflowRow.status == workflow_status,
        )
        lines.append(f'  {workflow_status.value}: {workflow_count}')
    workflow_rows = _recent(
        session, WorkflowRow, WorkflowRow.updated_at, limit
    )
    workflow_ids = [row.workflow_id for row in workflow_rows]
    workflow_inputs = (
        list(
            session.scalars(
                select(WorkflowInputRow)
                .where(WorkflowInputRow.workflow_id.in_(workflow_ids))
                .order_by(WorkflowInputRow.completed_at.desc())
                .limit(limit)
            )
        )
        if workflow_ids
        else []
    )
    run_ids_by_workflow: dict[UUID, list[str]] = {}
    for workflow_input in workflow_inputs:
        run_ids_by_workflow.setdefault(workflow_input.workflow_id, []).append(
            str(workflow_input.run_id)
        )
    for workflow_row in workflow_rows:
        run_ids = (
            ','.join(run_ids_by_workflow.get(workflow_row.workflow_id, []))
            or '-'
        )
        lines.append(
            f'  workflow_id={workflow_row.workflow_id} '
            f'coverage={workflow_row.coverage.value} '
            f'epiweek={workflow_row.epiweek} '
            f'status={workflow_row.status.value} '
            f'run_ids={run_ids} '
            f'updated_at={_timestamp(workflow_row.updated_at)}'
        )

    job_groups = (
        ('Queued analysis jobs', AnalysisJobStatus.QUEUED),
        ('Running analysis jobs', AnalysisJobStatus.RUNNING),
        ('Failed analysis jobs', AnalysisJobStatus.FAILED),
        ('Latest successful analysis jobs', AnalysisJobStatus.SUCCEEDED),
    )
    for title, job_status in job_groups:
        condition = AnalysisJobRow.status == job_status
        recency = (
            AnalysisJobRow.finished_at
            if job_status is AnalysisJobStatus.SUCCEEDED
            else AnalysisJobRow.created_at
        )
        job_rows = list(
            session.scalars(
                select(AnalysisJobRow)
                .where(condition)
                .order_by(recency.desc())
                .limit(limit)
            )
        )
        lines.append(
            f'\n{title}: {_count(session, AnalysisJobRow, condition)} total'
        )
        for job_row in job_rows:
            workflow = session.get(WorkflowRow, job_row.workflow_id)
            lines.append(
                f'  job_id={job_row.job_id} '
                f'workflow_id={job_row.workflow_id} '
                f'coverage={workflow.coverage.value if workflow else "-"} '
                f'epiweek={workflow.epiweek if workflow else "-"} '
                f'status={job_row.status.value} '
                f'attempts={job_row.attempts} '
                f'created_at={_timestamp(job_row.created_at)} '
                f'finished_at={_timestamp(job_row.finished_at)}'
            )
        if not job_rows:
            lines.append('  (none)')
    return '\n'.join(lines)


def main() -> None:
    """Print status without exposing connection or query diagnostics."""
    settings = get_settings()
    engine: Engine = create_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        connect_args={
            'connect_timeout': 5,
            'options': (
                '-c statement_timeout=5000 -c default_transaction_read_only=on'
            ),
        },
    )
    try:
        with Session(engine) as session:
            click.echo(report(session))
    except Exception:
        raise click.ClickException(
            'Operational status query failed; database details were suppressed'
        ) from None
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
