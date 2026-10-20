"""Finite in-container execution of the scientific job contract."""

import os
import signal
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
    """Reject analysis when execution is explicitly disabled."""

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Reject execution while preserving the AnalysisRunner contract."""
        raise AnalysisDisabledError(
            f'Analysis execution is disabled for job {request.job_id}'
        )


def analysis_argv(
    request: AnalysisJobRequest,
) -> list[str]:
    """Build argv for the Makim process in the current container."""
    return [
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


def run_scientific_process(
    args: list[str], env: dict[str, str], log_path: Path, timeout: int
) -> int:
    """Run and bound the entire Makim/R process group."""
    with log_path.open('ab') as log:
        process = subprocess.Popen(  # nosec B603
            args,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


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
    required_sql_outputs = (
        output_dir / 'sql' / 'output_dengue.sql',
        output_dir / 'sql' / 'output_chik.sql',
    )
    missing_sql_outputs = [
        path.name
        for path in required_sql_outputs
        if not path.is_file() or path.stat().st_size == 0
    ]
    if missing_sql_outputs:
        raise ValueError(
            'missing non-empty required SQL outputs: '
            + ', '.join(missing_sql_outputs)
        )
    if request.load:
        maps = list((output_dir / 'incidence_maps').rglob('*.png'))
        if not any(
            path.is_file() and path.stat().st_size > 0 for path in maps
        ):
            raise ValueError('missing non-empty incidence map output')


class InContainerAnalysisRunner:
    """Execute the scientific process in the analysis worker container."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Run the finite job and validate exit status plus artifacts."""
        settings = self._settings
        root = settings.analysis_output_root.resolve()
        output_dir = (
            request.output_dir or (root / str(request.job_id))
        ).resolve()
        if not output_dir.is_relative_to(root):
            raise ValueError('analysis output escapes configured root')
        output_dir.mkdir(parents=True, exist_ok=True)
        args = analysis_argv(request)
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
                'ALERTA_OUT_DIR': str(output_dir),
                'ALERTA_JOB_ID': str(request.job_id),
                'ALERTA_INPUT_FINGERPRINT': request.input_fingerprint,
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
            analysis_image=request.image or settings.analysis_image,
        )
        started = time.monotonic()
        try:
            exit_code = run_scientific_process(
                args, env, log_path, settings.analysis_timeout_seconds
            )
            if exit_code != 0:
                logger.warning(
                    'analysis_job_failed',
                    job_id=str(request.job_id),
                    exit_code=exit_code,
                    duration_seconds=round(time.monotonic() - started, 3),
                )
                return AnalysisJobResult(
                    request.job_id,
                    output_dir,
                    False,
                    exit_code,
                    'scientific process exited non-zero',
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
            # Exception text may include secrets; retain only the type.
            with log_path.open('ab') as log:
                message = f'{type(exc).__name__} during analysis execution\n'
                log.write(message.encode())
            return AnalysisJobResult(
                request.job_id,
                output_dir,
                False,
                None,
                f'{type(exc).__name__} during analysis execution',
            )
