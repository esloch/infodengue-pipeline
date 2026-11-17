"""Confirm a manually published analysis without rerunning its science."""

from __future__ import annotations

import hashlib

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import click

from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.db.models import AnalysisJobRow, WorkflowRow
from infodengue_pipeline.db.repositories import SqlWorkflowRepository
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    Epiweek,
    WorkflowStatus,
)


def sha256_file(path: Path) -> str:
    """Hash a file in bounded memory."""
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def artifact_hashes(
    output_dir: Path, output_root: Path, week: Epiweek
) -> dict[str, str]:
    """Validate immutable inputs and generated map outputs."""
    root = output_dir.resolve(strict=True)
    if not root.is_relative_to(output_root.resolve(strict=True)):
        raise ValueError('analysis output is outside configured output root')
    if len(list((root / 'alertas' / str(week)).glob('ale-??-*.RData'))) != 27:
        raise ValueError('expected 27 state RData files')
    br = root / 'alertas' / 'BR' / f'ale-BR-{week}.RData'
    if not br.is_file() or br.stat().st_size == 0:
        raise ValueError('national RData is missing or empty')
    country = root / 'incidence_maps' / 'country'
    for disease in ('dengue', 'chikungunya'):
        png = country / f'incidence_Nacional_{disease}.png'
        if not png.is_file() or png.stat().st_size == 0:
            raise ValueError(f'missing national map for {disease}')
    state_maps = list((root / 'incidence_maps' / 'state').glob('*.png'))
    if len(state_maps) != 54 or any(p.stat().st_size == 0 for p in state_maps):
        raise ValueError('expected 54 nonempty state maps')
    files = {
        'dengue': root / 'sql' / 'output_dengue.sql',
        'chikungunya': root / 'sql' / 'output_chik.sql',
    }
    if any(not p.is_file() or p.stat().st_size == 0 for p in files.values()):
        raise ValueError('required scientific SQL files missing or empty')
    return {disease: sha256_file(path) for disease, path in files.items()}


def validate_persisted_week(engine: Engine, week: Epiweek) -> None:
    """Confirm both complete national datasets exist in scientific DB."""
    tables = {
        'Historico_alerta': text(
            'SELECT count(*) AS rows, '
            'count(DISTINCT municipio_geocodigo) AS municipalities '
            'FROM "Municipio"."Historico_alerta" WHERE "SE" = :week'
        ),
        'Historico_alerta_chik': text(
            'SELECT count(*) AS rows, '
            'count(DISTINCT municipio_geocodigo) AS municipalities '
            'FROM "Municipio"."Historico_alerta_chik" WHERE "SE" = :week'
        ),
    }
    with engine.connect() as connection:
        for table, statement in tables.items():
            result = connection.execute(
                statement, {'week': int(week)}
            ).one()
            if tuple(result) != (5570, 5570):
                raise ValueError(
                    f'incomplete persisted {table} for week {week}: '
                    f'{tuple(result)}'
                )


def confirm(
    sessions: sessionmaker[Session],
    settings: Settings,
    job_id: UUID,
    week: Epiweek,
    history_engine: Engine,
) -> bool:
    """Record external publication once, preserving original job identity.

    SQL and map files must already have been published, and the runtime
    refresh must have succeeded; the operator explicitly attests to that.
    """
    with sessions.begin() as session:
        job = session.scalar(
            select(AnalysisJobRow)
            .where(AnalysisJobRow.job_id == job_id)
            .with_for_update()
        )
        if job is None:
            raise ValueError('analysis job does not exist')
        workflow = session.get(WorkflowRow, job.workflow_id)
        if workflow is None or workflow.epiweek != int(week):
            raise ValueError('job does not match requested epiweek')
        if job.status is not AnalysisJobStatus.SUCCEEDED or job.load:
            raise ValueError('requires successful original load=false job')
        if workflow.status is not WorkflowStatus.SUCCEEDED:
            raise ValueError('workflow is not succeeded')
        repository = SqlWorkflowRepository(session)
        snapshot = repository.get(workflow.coverage, week)
        if (
            snapshot is None
            or not snapshot.readiness.ready
            or snapshot.readiness.input_fingerprint != job.input_fingerprint
            or workflow.queued_fingerprint != job.input_fingerprint
        ):
            raise ValueError('job input fingerprint is no longer current')
        if settings.analysis_initial_epiweek is None:
            raise ValueError('analysis initial week must be configured')
        initial = Epiweek.parse(settings.analysis_initial_epiweek)
        last = repository.last_persisted_analysis_week(
            workflow.coverage, initial
        )
        next_week = last.next() if last is not None else initial
        if job.published_at is None and next_week != week:
            raise ValueError(
                f'cannot skip contiguous publication cursor: {next_week}'
            )
        hashes = artifact_hashes(
            Path(job.output_dir), settings.analysis_output_root, week
        )
        validate_persisted_week(history_engine, week)
        if job.published_at is not None:
            if (
                job.published_dengue_sha256 != hashes['dengue']
                or job.published_chik_sha256 != hashes['chikungunya']
            ):
                raise ValueError(
                    'publication hashes changed since confirmation'
                )
            return False
        job.published_at = datetime.now(timezone.utc)
        job.published_dengue_sha256 = hashes['dengue']
        job.published_chik_sha256 = hashes['chikungunya']
        session.flush()
        return True


@click.command()
@click.option('--job-id', type=click.UUID, required=True)
@click.option('--week', type=int, required=True)
@click.option(
    '--confirm-runtime-refreshed',
    is_flag=True,
    required=True,
    help='Attest successful map publication and prod runtime refresh.',
)
def main(job_id: UUID, week: int, confirm_runtime_refreshed: bool) -> None:
    """Acknowledge an independently completed publication."""
    if not confirm_runtime_refreshed:
        raise click.ClickException('runtime confirmation is required')
    settings = get_settings()
    if settings.environment != 'prod':
        raise click.ClickException('this command requires prod environment')
    if settings.analysis_database_url is None:
        raise click.ClickException('read-only scientific DB URL is required')
    history_engine = create_engine(
        settings.analysis_database_url.get_secret_value(),
        pool_pre_ping=True,
        connect_args={'options': '-c default_transaction_read_only=on'},
        hide_parameters=True,
    )
    try:
        changed = confirm(
            session_factory(),
            settings,
            job_id,
            Epiweek.parse(week),
            history_engine,
        )
        click.echo(
            'Publication confirmed'
            if changed
            else 'Publication already confirmed'
        )
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from None
    finally:
        history_engine.dispose()


if __name__ == '__main__':
    main()
