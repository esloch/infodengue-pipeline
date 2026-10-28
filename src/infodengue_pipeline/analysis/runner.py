"""Finite in-container execution of the scientific job contract."""

import os
import shutil
import signal
import subprocess  # nosec B404
import time

from contextlib import contextmanager, nullcontext
from hashlib import sha256
from pathlib import Path
from typing import Iterator

import structlog

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from infodengue_pipeline.config import Settings, get_settings
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobResult,
)
from infodengue_pipeline.production.target import (
    ANALYSIS_E2E_DATABASE,
    ANALYSIS_E2E_HOST,
    ANALYSIS_E2E_ROLE,
    RESTORE_EVIDENCE_SQL,
)

logger = structlog.get_logger(__name__)


class AnalysisLoadLockTimeout(RuntimeError):
    """Raised when another load=true job holds the database lock too long."""


def _database_lock_key(database: str) -> int:
    """Derive one stable signed PostgreSQL advisory-lock key from DB name."""
    value = int.from_bytes(sha256(database.encode()).digest()[:8], 'big')
    return value - (1 << 64) if value >= (1 << 63) else value


@contextmanager
def _analysis_load_lock(
    database_url: str, timeout_seconds: int
) -> Iterator[None]:
    """Serialize mutating analyses against the same PostgreSQL database."""
    database = make_url(database_url).database
    if not database:
        raise ValueError('analysis database URL has no database name')
    key = _database_lock_key(database)
    engine = create_engine(
        database_url,
        poolclass=NullPool,
        hide_parameters=True,
    )
    connection = engine.connect()
    acquired = False
    deadline = time.monotonic() + timeout_seconds
    try:
        while True:
            acquired = bool(
                connection.execute(
                    text('SELECT pg_try_advisory_lock(:key)'), {'key': key}
                ).scalar_one()
            )
            connection.commit()
            if acquired:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AnalysisLoadLockTimeout
            time.sleep(min(1.0, remaining))
        yield
    finally:
        try:
            if acquired:
                connection.execute(
                    text('SELECT pg_advisory_unlock(:key)'), {'key': key}
                )
                connection.commit()
        finally:
            connection.close()
            engine.dispose()


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


def require_staging_load_target(
    settings: Settings, request: AnalysisJobRequest, database_url: str
) -> None:
    """Gate scientific writes in the prod profile."""
    if settings.environment != 'prod' or not request.load:
        return
    parsed = make_url(database_url)
    expected = (
        ANALYSIS_E2E_HOST,
        ANALYSIS_E2E_DATABASE,
        ANALYSIS_E2E_ROLE,
    )
    if (
        not settings.analysis_staging_backup_verified
        or not settings.analysis_staging_backup_sha256
        or len(settings.analysis_staging_backup_sha256) != 64
        or settings.analysis_staging_target_host != expected[0]
        or settings.analysis_staging_target_database != expected[1]
        or settings.analysis_staging_target_role != expected[2]
        or (parsed.host, parsed.database, parsed.username) != expected
        or request.coverage.value != settings.analysis_e2e_coverage
        or str(request.epiweek) != settings.analysis_e2e_epiweek
    ):
        raise ValueError('production analysis staging load gate failed')
    engine = create_engine(database_url, hide_parameters=True)
    try:
        with engine.connect() as connection:
            identity = connection.execute(
                text('SELECT current_database(), current_user')
            ).one()
            if tuple(identity) != expected[1:]:
                raise ValueError(
                    'production analysis staging load gate failed'
                )
            restored = connection.execute(
                text(RESTORE_EVIDENCE_SQL), {'environment': 'staging'}
            ).scalar_one()
            if restored != settings.analysis_staging_backup_sha256:
                raise ValueError(
                    'production analysis staging restore gate failed'
                )
    finally:
        engine.dispose()


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
        if settings.analysis_database_url is None:
            raise ValueError('analysis database URL is not configured')
        database_url = settings.analysis_database_url.get_secret_value()
        require_staging_load_target(settings, request, database_url)
        if settings.environment == 'prod' and request.load:
            if (
                shutil.which('psql', path=env.get('PATH'))
                != '/usr/local/bin/psql'
            ):
                raise ValueError(
                    'production analysis SQL review wrapper missing'
                )
        database = make_url(database_url)
        if not database.host or not database.database or not database.username:
            raise ValueError('analysis database URL is incomplete')
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
        if settings.environment == 'prod' and request.load:
            # The pinned Makim task is image-patched to invoke this wrapper
            # by absolute path. Pass the review gate from validated settings,
            # never from inherited process environment.
            env.update(
                {
                    'INFODENGUE_PIPELINE_ENVIRONMENT': 'prod',
                    'INFODENGUE_PIPELINE_ANALYSIS_LOAD': 'true',
                    'INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256': (
                        settings.analysis_staging_backup_sha256 or ''
                    ),
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
            lock = (
                _analysis_load_lock(
                    database_url, settings.analysis_load_lock_timeout_seconds
                )
                if request.load
                else nullcontext()
            )
            with lock:
                exit_code = run_scientific_process(
                    args, env, log_path, settings.analysis_timeout_seconds
                )
                if exit_code == 0:
                    validate_outputs(output_dir, request)
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
        except AnalysisLoadLockTimeout:
            logger.warning(
                'analysis_load_lock_timed_out', job_id=str(request.job_id)
            )
            return AnalysisJobResult(
                request.job_id,
                output_dir,
                False,
                75,
                'analysis database load lock timed out',
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
        except Exception as exc:
            logger.error(
                'analysis_execution_error',
                job_id=str(request.job_id),
                error_type=type(exc).__name__,
            )
            return AnalysisJobResult(
                request.job_id,
                output_dir,
                False,
                None,
                f'{type(exc).__name__} during analysis execution',
            )
