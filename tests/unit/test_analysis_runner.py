"""In-container command, secret environment and artifact contract tests."""

from contextlib import contextmanager, nullcontext
from pathlib import Path
from subprocess import TimeoutExpired
from uuid import uuid4

import pytest

from infodengue_pipeline.analysis.runner import (
    AnalysisLoadLockTimeout,
    InContainerAnalysisRunner,
    _analysis_load_lock,
    _database_lock_key,
    analysis_argv,
    run_scientific_process,
    validate_outputs,
)
from infodengue_pipeline.config import Settings
from infodengue_pipeline.models import AnalysisJobRequest, Coverage, Epiweek


def request(
    coverage: Coverage = Coverage.MG, load: bool = False
) -> AnalysisJobRequest:
    """Build one valid state or national runner request."""
    return AnalysisJobRequest(
        uuid4(), coverage, Epiweek.parse(202639), 'fingerprint', 3, load
    )


def write_contract_outputs(root: Path, week: str = '202639') -> None:
    """Create non-empty current-run artifacts required by the contract."""
    state = root / 'alertas' / week
    br = root / 'alertas' / 'BR'
    sql = root / 'sql'
    state.mkdir(parents=True)
    br.mkdir(parents=True)
    sql.mkdir(parents=True)
    (state / 'ale-MG-202639.RData').write_bytes(b'state')
    (br / 'ale-BR-202639.RData').write_bytes(b'br')
    (sql / 'output_dengue.sql').write_bytes(b'sql')
    (sql / 'output_chik.sql').write_bytes(b'sql')


def test_argv_maps_br_and_state_scope_without_shell() -> None:
    """Direct args use the finite public entrypoint and safe defaults."""
    state_args = analysis_argv(request())
    national_args = analysis_argv(request(Coverage.BR))
    assert 'MG' in state_args
    assert 'ALL' in national_args
    assert state_args[-10:] == [
        'makim',
        'pipeline.refresh-alertas-job',
        '--week',
        '202639',
        '--states',
        'MG',
        '--cores',
        '3',
        '--load',
        'false',
    ]
    assert state_args[-2:] == ['--load', 'false']
    assert 'sh' not in state_args
    assert 'docker' not in state_args
    assert analysis_argv(request(load=True))[-2:] == ['--load', 'true']


def test_output_validation_requires_contract_artifacts_and_load_maps(
    tmp_path: Path,
) -> None:
    """Empty or missing current-run outputs fail; maps depend on load."""
    no_load = request()
    write_contract_outputs(tmp_path)
    validate_outputs(tmp_path, no_load)
    loaded = request(load=True)
    with pytest.raises(ValueError, match='incidence map'):
        validate_outputs(tmp_path, loaded)
    map_path = tmp_path / 'incidence_maps' / 'map.png'
    map_path.parent.mkdir()
    map_path.write_bytes(b'png')
    validate_outputs(tmp_path, loaded)


def test_output_validation_requires_selected_state_artifact(
    tmp_path: Path,
) -> None:
    """An unrelated state's RData cannot satisfy a state job."""
    write_contract_outputs(tmp_path)
    (tmp_path / 'alertas' / '202639' / 'ale-MG-202639.RData').unlink()
    (tmp_path / 'alertas' / '202639' / 'ale-RJ-202639.RData').write_bytes(
        b'state'
    )
    with pytest.raises(ValueError, match='state RData'):
        validate_outputs(tmp_path, request())


@pytest.mark.parametrize(
    ('filename', 'contents'),
    [
        ('output_dengue.sql', None),
        ('output_chik.sql', None),
        ('output_chik.sql', b''),
    ],
    ids=['missing-dengue', 'missing-chik', 'empty-chik'],
)
def test_output_validation_requires_nonempty_dengue_and_chik_sql(
    tmp_path: Path, filename: str, contents: bytes | None
) -> None:
    """Both required SQL outputs must exist and contain data."""
    write_contract_outputs(tmp_path)
    sql_path = tmp_path / 'sql' / filename
    if contents is None:
        sql_path.unlink()
    else:
        sql_path.write_bytes(contents)
    with pytest.raises(ValueError, match=filename):
        validate_outputs(tmp_path, request())


def test_output_validation_does_not_require_zika_sql(tmp_path: Path) -> None:
    """The public finite-job contract requires dengue and chik only."""
    write_contract_outputs(tmp_path)
    assert not (tmp_path / 'sql' / 'output_zika.sql').exists()
    validate_outputs(tmp_path, request())


def test_zika_sql_alone_does_not_satisfy_required_sql_outputs(
    tmp_path: Path,
) -> None:
    """Optional Zika SQL cannot replace the mandatory pair."""
    write_contract_outputs(tmp_path)
    (tmp_path / 'sql' / 'output_dengue.sql').unlink()
    (tmp_path / 'sql' / 'output_chik.sql').unlink()
    (tmp_path / 'sql' / 'output_zika.sql').write_bytes(b'sql')
    with pytest.raises(
        ValueError, match=r'output_dengue.sql.*output_chik.sql'
    ):
        validate_outputs(tmp_path, request())


def test_runner_passes_secrets_only_in_environment_and_checks_outputs(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    """Password never appears in argv and successful exit validates outputs."""
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=tmp_path / 'outputs',
        analysis_database_url=(
            'postgresql+psycopg://user:secret%40pass@db.example:5433/alerta'
        ),
    )
    seen = {}

    def fake_run(args, env, _log_path, _timeout):
        seen['args'] = args
        seen['env'] = env
        write_contract_outputs(settings.analysis_output_root / str(job.job_id))
        return 0

    job = request()
    job = job.__class__(
        job.job_id,
        job.coverage,
        job.epiweek,
        job.input_fingerprint,
        job.cores,
        job.load,
        image='analysis:pinned',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', fake_run
    )
    result = InContainerAnalysisRunner(settings).run(job)
    assert result.succeeded
    assert 'secret@pass' not in ' '.join(seen['args'])
    assert 'ALERTA_DB_PASSWORD' not in seen['args']
    assert seen['env']['ALERTA_DB_PASSWORD'] == 'secret@pass'
    assert seen['env']['ALERTA_DB_HOST'] == 'db.example'
    assert 'analysis:pinned' not in seen['args']
    assert seen['env']['ALERTA_OUT_DIR'] == str(result.output_dir)
    assert seen['env']['ALERTA_JOB_ID'] == str(job.job_id)
    assert seen['env']['ALERTA_INPUT_FINGERPRINT'] == 'fingerprint'
    assert 'secret@pass' not in caplog.text
    runner_log = result.output_dir / 'pipeline-runner.log'
    assert (
        not runner_log.exists() or 'secret@pass' not in runner_log.read_text()
    )


def test_runner_does_not_decode_database_password_twice(
    monkeypatch, tmp_path: Path
) -> None:
    """A literal percent escape in a decoded password remains unchanged."""
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=tmp_path / 'outputs',
        analysis_database_url=(
            'postgresql+psycopg://user:abc%2540def@db.example/alerta'
        ),
    )
    seen: dict[str, object] = {}

    def fake_run(_args, env, _log_path, _timeout):
        seen['env'] = env
        write_contract_outputs(settings.analysis_output_root / str(job.job_id))
        return 0

    job = request()
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', fake_run
    )
    assert InContainerAnalysisRunner(settings).run(job).succeeded
    assert seen['env']['ALERTA_DB_PASSWORD'] == 'abc%40def'


def test_prod_load_runner_sets_review_gate_from_verified_settings(
    monkeypatch, tmp_path: Path
) -> None:
    """The upstream wrapper receives only the validated clone evidence."""
    digest = 'a' * 64
    settings = Settings(
        _env_file=None,
        environment='prod',
        analysis_output_root=tmp_path / 'outputs',
        analysis_database_url=(
            'postgresql+psycopg://analysis_e2e_writer:secret'
            '@analysis-e2e-db:5432/analysis_e2e'
        ),
        analysis_staging_target_host='analysis-e2e-db',
        analysis_staging_target_database='analysis_e2e',
        analysis_staging_target_role='analysis_e2e_writer',
        analysis_staging_backup_verified=True,
        analysis_staging_backup_sha256=digest,
        analysis_e2e_coverage='MG',
        analysis_e2e_epiweek='202639',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.require_staging_load_target',
        lambda *_args: None,
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.shutil.which',
        lambda *_args, **_kwargs: '/usr/local/bin/psql',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner._analysis_load_lock',
        lambda *_args: nullcontext(),
    )
    seen: dict[str, object] = {}

    def capture(_args, env, _log_path, _timeout):
        seen.update(env)
        output = settings.analysis_output_root / str(job.job_id)
        write_contract_outputs(output)
        maps = output / 'incidence_maps'
        maps.mkdir()
        (maps / 'map.png').write_bytes(b'png')
        return 0

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', capture
    )
    monkeypatch.setenv('INFODENGUE_PIPELINE_ANALYSIS_LOAD', 'false')
    monkeypatch.setenv(
        'INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256', 'b' * 64
    )
    job = request(load=True)
    result = InContainerAnalysisRunner(settings).run(job)

    assert result.succeeded
    assert seen['INFODENGUE_PIPELINE_ENVIRONMENT'] == 'prod'
    assert seen['INFODENGUE_PIPELINE_ANALYSIS_LOAD'] == 'true'
    assert seen['INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256'] == digest


def test_runner_contains_output_path_and_handles_nonzero_or_timeout(
    monkeypatch, tmp_path: Path
) -> None:
    """Output paths stay below root and process failure is authoritative."""
    root = tmp_path / 'out'
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=root,
        analysis_database_url='postgresql://u:p@localhost/db',
    )
    job = request()
    outside = job.__class__(
        job.job_id,
        job.coverage,
        job.epiweek,
        job.input_fingerprint,
        job.cores,
        job.load,
        tmp_path / '..' / 'outside',
    )

    def unexpected(_args, _env, _log_path, _timeout):
        raise AssertionError('runner invoked outside output root')

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        unexpected,
    )
    with pytest.raises(ValueError, match='escapes configured root'):
        InContainerAnalysisRunner(settings).run(outside)

    def fail(_args, _env, _log_path, _timeout):
        return 7

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', fail
    )
    result = InContainerAnalysisRunner(settings).run(job)
    assert not result.succeeded
    assert result.exit_code == 7

    def timeout(_args, _env, _log_path, _timeout):
        raise TimeoutExpired('makim', 1)

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        timeout,
    )
    result = InContainerAnalysisRunner(settings).run(job)
    assert not result.succeeded
    assert result.exit_code == 124


def test_timeout_kills_scientific_process_group(
    monkeypatch, tmp_path: Path
) -> None:
    """A timed out Makim launcher cannot leave R descendants running."""
    seen: list[object] = []

    class FakeProcess:
        pid = 321

        def wait(self, timeout=None):
            seen.append(timeout)
            if timeout is not None:
                raise TimeoutExpired('makim', timeout)
            return -9

    def fake_popen(_args, **kwargs):
        assert kwargs['start_new_session'] is True
        return FakeProcess()

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.subprocess.Popen', fake_popen
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.os.killpg',
        lambda pid, sig: seen.append((pid, sig)),
    )
    with pytest.raises(TimeoutExpired):
        run_scientific_process(
            analysis_argv(request()), {}, tmp_path / 'log', 5
        )
    assert seen == [5, (321, __import__('signal').SIGKILL), None]


def test_analysis_runner_does_not_fall_back_to_source_database(
    monkeypatch, tmp_path: Path
) -> None:
    """Source-only settings cannot start the scientific process."""
    settings = Settings(
        _env_file=None,
        alerta_source_database_url='postgresql://readonly:secret@db/source',
        analysis_output_root=tmp_path / 'outputs',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        lambda *_args: pytest.fail('scientific process started'),
    )
    with pytest.raises(ValueError, match='analysis database URL'):
        InContainerAnalysisRunner(settings).run(request())


def test_prod_load_requires_sql_review_wrapper(
    monkeypatch, tmp_path: Path
) -> None:
    """A changed PATH fails before any scientific process can start."""
    settings = Settings(
        _env_file=None,
        environment='prod',
        analysis_database_url='postgresql://writer:secret@analysis-e2e-db/analysis_e2e',
        analysis_output_root=tmp_path / 'outputs',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.require_staging_load_target',
        lambda *_args: None,
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.shutil.which',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        lambda *_args: pytest.fail('scientific process started'),
    )
    with pytest.raises(ValueError, match='SQL review wrapper missing'):
        InContainerAnalysisRunner(settings).run(request(load=True))


def test_analysis_runner_rejects_incomplete_target_database(
    monkeypatch, tmp_path: Path
) -> None:
    """Analysis requires a host, database name, and user in its URL."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql:///alerta',
        analysis_output_root=tmp_path / 'outputs',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        lambda *_args: pytest.fail('incomplete DB URL started analysis'),
    )
    with pytest.raises(
        ValueError, match='analysis database URL is incomplete'
    ):
        InContainerAnalysisRunner(settings).run(request())


def test_load_false_skips_advisory_lock(monkeypatch, tmp_path: Path) -> None:
    """Read-only scientific jobs do not serialize on the target database."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql://user:pass@db/alerta',
        analysis_output_root=tmp_path / 'outputs',
    )
    job = request()
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner._analysis_load_lock',
        lambda *_args: pytest.fail('load=false acquired advisory lock'),
    )

    def success(_args, _env, _log, _timeout):
        write_contract_outputs(settings.analysis_output_root / str(job.job_id))
        return 0

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', success
    )
    assert InContainerAnalysisRunner(settings).run(job).succeeded


@pytest.mark.parametrize('outcome', ['success', 'failure', 'timeout'])
def test_load_lock_is_held_and_released_for_complete_execution(
    monkeypatch, tmp_path: Path, outcome: str
) -> None:
    """The load lock spans R, output validation, failure, and timeout."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql://user:pass@db/alerta',
        analysis_output_root=tmp_path / 'outputs',
    )
    job = request(load=True)
    lifecycle: list[str] = []

    @contextmanager
    def fake_lock(_url, _timeout):
        lifecycle.append('acquired')
        try:
            yield
        finally:
            lifecycle.append('released')

    def process(_args, _env, _log, _timeout):
        assert lifecycle == ['acquired']
        if outcome == 'timeout':
            raise TimeoutExpired('makim', 1)
        if outcome == 'failure':
            return 7
        output = settings.analysis_output_root / str(job.job_id)
        write_contract_outputs(output)
        maps = output / 'incidence_maps'
        maps.mkdir()
        (maps / 'map.png').write_bytes(b'png')
        return 0

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner._analysis_load_lock', fake_lock
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', process
    )
    result = InContainerAnalysisRunner(settings).run(job)
    assert lifecycle == ['acquired', 'released']
    assert result.succeeded is (outcome == 'success')


def test_database_lock_uses_hash_and_unlocks_after_success_or_exception(
    monkeypatch,
) -> None:
    """The target name alone supplies a stable key and session unlock."""
    import infodengue_pipeline.analysis.runner as runner

    assert _database_lock_key('alerta') == _database_lock_key('alerta')
    assert _database_lock_key('alerta') != _database_lock_key('other')
    assert -(1 << 63) <= _database_lock_key('alerta') < (1 << 63)

    class Result:
        def scalar_one(self):
            return True

    class Connection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params):
            self.statements.append((str(statement), params))
            return Result()

        def commit(self):
            pass

        def close(self):
            pass

    class Engine:
        def __init__(self):
            self.connection = Connection()
            self.disposed = False

        def connect(self):
            return self.connection

        def dispose(self):
            self.disposed = True

    engine = Engine()
    monkeypatch.setattr(runner, 'create_engine', lambda *_a, **_k: engine)
    with _analysis_load_lock('postgresql://u:secret@host/alerta', 1):
        assert len(engine.connection.statements) == 1
        assert 'pg_try_advisory_lock' in engine.connection.statements[0][0]
    assert 'pg_advisory_unlock' in engine.connection.statements[-1][0]
    assert engine.disposed


def test_database_lock_unlocks_when_scientific_body_raises(
    monkeypatch,
) -> None:
    """The PostgreSQL session lock is explicitly released on process errors."""
    import infodengue_pipeline.analysis.runner as runner

    class Result:
        def scalar_one(self):
            return True

    class Connection:
        def __init__(self):
            self.statements: list[str] = []

        def execute(self, statement, _params):
            self.statements.append(str(statement))
            return Result()

        def commit(self):
            pass

        def close(self):
            pass

    class Engine:
        def __init__(self):
            self.connection = Connection()

        def connect(self):
            return self.connection

        def dispose(self):
            pass

    engine = Engine()
    monkeypatch.setattr(runner, 'create_engine', lambda *_a, **_k: engine)
    with pytest.raises(RuntimeError, match='scientific process failed'):
        with _analysis_load_lock('postgresql://u:p@host/alerta', 1):
            raise RuntimeError('scientific process failed')
    assert 'pg_advisory_unlock' in engine.connection.statements[-1]


def test_database_lock_times_out_after_bounded_wait(monkeypatch) -> None:
    """A busy database lock exits after its configured deadline."""
    import infodengue_pipeline.analysis.runner as runner

    state = {'closed': False, 'disposed': False}

    class Result:
        def scalar_one(self):
            return False

    class Connection:
        def execute(self, *_args):
            return Result()

        def commit(self):
            pass

        def close(self):
            state['closed'] = True

    class Engine:
        def connect(self):
            return Connection()

        def dispose(self):
            state['disposed'] = True

    ticks = iter((0.0, 2.0))
    monkeypatch.setattr(runner, 'create_engine', lambda *_a, **_k: Engine())
    monkeypatch.setattr(runner.time, 'monotonic', lambda: next(ticks))
    monkeypatch.setattr(runner.time, 'sleep', lambda _seconds: None)
    with pytest.raises(AnalysisLoadLockTimeout):
        with _analysis_load_lock('postgresql://u:p@host/alerta', 1):
            pytest.fail('busy lock should not be acquired')
    assert state == {'closed': True, 'disposed': True}


def test_database_lock_retries_after_brief_contention(monkeypatch) -> None:
    """A brief lock holder does not fail the job before its deadline."""
    import infodengue_pipeline.analysis.runner as runner

    outcomes = iter((False, True, True))

    class Result:
        def scalar_one(self):
            return next(outcomes)

    class Connection:
        def execute(self, *_args):
            return Result()

        def commit(self):
            pass

        def close(self):
            pass

    class Engine:
        def connect(self):
            return Connection()

        def dispose(self):
            pass

    sleeps: list[float] = []
    monkeypatch.setattr(runner, 'create_engine', lambda *_a, **_k: Engine())
    monkeypatch.setattr(runner.time, 'sleep', sleeps.append)
    with _analysis_load_lock('postgresql://u:p@host/alerta', 2):
        assert sleeps


def test_load_lock_timeout_fails_before_process(
    monkeypatch, tmp_path: Path
) -> None:
    """A busy mutating target fails cleanly without invoking R."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql://user:pass@db/alerta',
        analysis_output_root=tmp_path / 'outputs',
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner._analysis_load_lock',
        lambda *_args: (_ for _ in ()).throw(AnalysisLoadLockTimeout()),
    )
    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process',
        lambda *_args: pytest.fail('R must not start after lock timeout'),
    )
    result = InContainerAnalysisRunner(settings).run(request(load=True))
    assert not result.succeeded
    assert result.exit_code == 75
    assert result.error == 'analysis database load lock timed out'


@pytest.mark.parametrize(
    ('failure', 'expected_error'),
    [
        (
            OSError('password=must-not-leak'),
            'OSError during analysis execution',
        ),
        (
            RuntimeError('password=must-not-leak'),
            'RuntimeError during analysis execution',
        ),
    ],
)
def test_runner_sanitizes_execution_errors(
    monkeypatch, tmp_path: Path, failure: Exception, expected_error: str
) -> None:
    """Execution exception text cannot leak into job state or the log."""
    settings = Settings(
        _env_file=None,
        analysis_database_url='postgresql://user:password@db/alerta',
        analysis_output_root=tmp_path / 'outputs',
    )

    def fail(*_args):
        raise failure

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.run_scientific_process', fail
    )
    result = InContainerAnalysisRunner(settings).run(request())
    assert result.error == expected_error
    assert 'must-not-leak' not in (result.error or '')
    runner_log = result.output_dir / 'pipeline-runner.log'
    assert (
        not runner_log.exists()
        or 'must-not-leak' not in runner_log.read_text()
    )


def test_load_lock_url_requires_database_name() -> None:
    """The lock cannot silently collapse targets without database names."""
    with pytest.raises(ValueError, match='no database name'):
        with _analysis_load_lock('postgresql://user:pass@db', 1):
            pytest.fail('database name is required')
