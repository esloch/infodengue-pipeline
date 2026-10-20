"""In-container command, secret environment and artifact contract tests."""

from pathlib import Path
from subprocess import TimeoutExpired
from uuid import uuid4

import pytest

from infodengue_pipeline.analysis.runner import (
    InContainerAnalysisRunner,
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
    monkeypatch, tmp_path: Path
) -> None:
    """Password never appears in argv and successful exit validates outputs."""
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=tmp_path / 'outputs',
        alerta_database_url=(
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


def test_runner_does_not_decode_database_password_twice(
    monkeypatch, tmp_path: Path
) -> None:
    """A literal percent escape in a decoded password remains unchanged."""
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=tmp_path / 'outputs',
        alerta_database_url=(
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


def test_runner_contains_output_path_and_handles_nonzero_or_timeout(
    monkeypatch, tmp_path: Path
) -> None:
    """Output paths stay below root and process failure is authoritative."""
    root = tmp_path / 'out'
    settings = Settings(
        _env_file=None,
        analysis_image='analysis:test',
        analysis_output_root=root,
        alerta_database_url='postgresql://u:p@localhost/db',
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
