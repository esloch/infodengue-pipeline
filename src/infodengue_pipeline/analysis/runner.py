"""Finite OCI execution for the AlertaDengueAnalise public job contract."""

import os
import subprocess  # nosec B404
import time

from pathlib import Path

import structlog

from sqlalchemy.engine import make_url

from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobResult,
)

logger = structlog.get_logger(__name__)


class AnalysisDisabledError(RuntimeError):
    """Raised by the explicit disabled runner retained for callers/tests."""


class DisabledAnalysisRunner:
    """Reject analysis when an operator has not configured an OCI image."""

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Reject execution while preserving the AnalysisRunner contract."""
        raise AnalysisDisabledError(
            f'Analysis execution is disabled for job {request.job_id}'
        )


def analysis_argv(
    request: AnalysisJobRequest,
    image: str,
    output_dir: Path,
    runtime: str = 'docker',
    network: str | None = None,
) -> list[str]:
    """Build direct argv for one analysis container invocation."""
    args = [runtime, 'run', '--rm']
    if network:
        args.extend(['--network', network])
    args.extend(
        [
            '--volume',
            f'{output_dir.resolve()}:/outputs',
            '--env',
            'ALERTA_OUT_DIR=/outputs',
            '--env',
            f'ALERTA_JOB_ID={request.job_id}',
            '--env',
            f'ALERTA_INPUT_FINGERPRINT={request.input_fingerprint}',
        ]
    )
    for name in (
        'ALERTA_DB_HOST',
        'ALERTA_DB_PORT',
        'ALERTA_DB_NAME',
        'ALERTA_DB_USER',
        'ALERTA_DB_PASSWORD',
    ):
        args.extend(['--env', name])
    args.extend(
        [
            image,
            'makim',
            'pipeline.refresh-alertas-job',
            '--week',
            str(request.epiweek),
            '--states',
            ','.join(request.states),
            '--cores',
            str(request.cores),
            '--load',
            'true' if request.load else 'false',
        ]
    )
    return args


def validate_outputs(output_dir: Path, request: AnalysisJobRequest) -> None:
    """Require current-run state, BR, and SQL outputs from the contract."""
    week = str(request.epiweek)
    week_dir = output_dir / 'alertas' / week
    if request.coverage.is_national:
        state_outputs = list(week_dir.glob(f'ale-??-{week}.RData'))
    else:
        state_outputs = [
            week_dir / f'ale-{request.coverage.value}-{week}.RData'
        ]
    if not any(
        path.is_file() and path.stat().st_size > 0 for path in state_outputs
    ):
        raise ValueError('missing non-empty current-week state RData')
    br_output = (
        output_dir / 'alertas' / 'BR' / f'ale-BR-{request.epiweek}.RData'
    )
    if not br_output.is_file() or br_output.stat().st_size == 0:
        raise ValueError('missing non-empty consolidated BR RData')
    sql_outputs = [
        output_dir / 'sql' / name
        for name in (
            'output_dengue.sql',
            'output_chik.sql',
            'output_zika.sql',
        )
    ]
    if not any(
        path.is_file() and path.stat().st_size > 0 for path in sql_outputs
    ):
        raise ValueError('missing non-empty generated alert SQL')
    if request.load:
        maps = list((output_dir / 'incidence_maps').rglob('*.png'))
        if not any(
            path.is_file() and path.stat().st_size > 0 for path in maps
        ):
            raise ValueError('missing non-empty incidence map output')


class OCIAnalysisRunner:
    """Execute the pinned image through a local OCI runtime."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Run the finite job and validate exit status plus artifacts."""
        settings = self._settings
        image = request.image or settings.analysis_image
        if not image:
            raise ValueError('analysis image is not configured')
        root = settings.analysis_output_root.resolve()
        output_dir = (
            request.output_dir or (root / str(request.job_id))
        ).resolve()
        if not output_dir.is_relative_to(root):
            raise ValueError('analysis output escapes configured root')
        output_dir.mkdir(parents=True, exist_ok=True)
        args = analysis_argv(
            request,
            image,
            output_dir,
            settings.analysis_runtime,
            settings.analysis_network,
        )
        env = os.environ.copy()
        if settings.alerta_database_url is None:
            raise ValueError('AlertaDengue database URL is not configured')
        database = make_url(settings.alerta_database_url.get_secret_value())
        if not database.host or not database.database or not database.username:
            raise ValueError('AlertaDengue database URL is incomplete')
        env.update(
            {
                'ALERTA_DB_HOST': database.host,
                'ALERTA_DB_PORT': str(database.port or 5432),
                'ALERTA_DB_NAME': database.database,
                'ALERTA_DB_USER': database.username,
                'ALERTA_DB_PASSWORD': database.password or '',
            }
        )
        log_path = output_dir / 'pipeline-runner.log'
        logger.info(
            'analysis_job_started',
            job_id=str(request.job_id),
            workflow_id=(
                str(request.workflow_id) if request.workflow_id else None
            ),
            input_fingerprint=request.input_fingerprint,
            coverage=request.coverage.value,
            epiweek=str(request.epiweek),
            analysis_image=image,
        )
        started = time.monotonic()
        try:
            with log_path.open('ab') as log:
                completed = subprocess.run(  # nosec B603
                    args,
                    check=False,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=settings.analysis_timeout_seconds,
                    env=env,
                )
            if completed.returncode != 0:
                logger.warning(
                    'analysis_job_failed',
                    job_id=str(request.job_id),
                    exit_code=completed.returncode,
                    duration_seconds=round(time.monotonic() - started, 3),
                )
                return AnalysisJobResult(
                    request.job_id,
                    output_dir,
                    False,
                    completed.returncode,
                    'analysis container exited non-zero',
                )
            validate_outputs(output_dir, request)
            logger.info(
                'analysis_job_succeeded',
                job_id=str(request.job_id),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            return AnalysisJobResult(request.job_id, output_dir, True, 0)
        except subprocess.TimeoutExpired:
            with log_path.open('ab') as log:
                log.write(b'analysis execution timed out\n')
            logger.warning(
                'analysis_job_timed_out',
                job_id=str(request.job_id),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            return AnalysisJobResult(
                request.job_id, output_dir, False, 124, 'analysis timed out'
            )
        except (OSError, ValueError) as exc:
            with log_path.open('ab') as log:
                log.write(f'{type(exc).__name__}: {exc}\n'.encode())
            return AnalysisJobResult(
                request.job_id,
                output_dir,
                False,
                None,
                f'{type(exc).__name__}: {exc}'[:512],
            )
