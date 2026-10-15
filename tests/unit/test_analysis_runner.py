"""Public OCI argv, secret environment and artifact contract tests."""

from pathlib import Path
from subprocess import CompletedProcess
from uuid import uuid4

import pytest

from infodengue_pipeline.analysis.runner import (
    OCIAnalysisRunner,
    analysis_argv,
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


def test_argv_maps_br_and_state_scope_without_shell() -> None:
    """OCI args exactly use finite public entrypoint and safe defaults."""
    state_args = analysis_argv(request(), 'image:tag', Path('/tmp/out'))
    national_args = analysis_argv(
        request(Coverage.BR), 'image:tag', Path('/tmp/out')
    )
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

    def fake_run(args, **kwargs):
        seen['args'] = args
        seen['env'] = kwargs['env']
        write_contract_outputs(settings.analysis_output_root / str(job.job_id))
        return CompletedProcess(args, 0)

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
        'infodengue_pipeline.analysis.runner.subprocess.run', fake_run
    )
    result = OCIAnalysisRunner(settings).run(job)
    assert result.succeeded
    assert 'secret@pass' not in ' '.join(seen['args'])
    assert 'ALERTA_DB_PASSWORD' in seen['args']
    assert seen['env']['ALERTA_DB_PASSWORD'] == 'secret@pass'
    assert seen['env']['ALERTA_DB_HOST'] == 'db.example'
    assert 'analysis:pinned' in seen['args']


def test_runner_contains_output_path_and_handles_nonzero_or_timeout(
    monkeypatch, tmp_path: Path
) -> None:
    """OCI paths stay below root and process failure is authoritative."""
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

    def unexpected(_args, **_kwargs):
        raise AssertionError('runner invoked outside output root')

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.subprocess.run', unexpected
    )
    with pytest.raises(ValueError, match='escapes configured root'):
        OCIAnalysisRunner(settings).run(outside)

    def fail(_args, **_kwargs):
        return CompletedProcess([], 7)

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.subprocess.run', fail
    )
    assert not OCIAnalysisRunner(settings).run(job).succeeded

    def timeout(_args, **_kwargs):
        raise __import__('subprocess').TimeoutExpired('docker', 1)

    monkeypatch.setattr(
        'infodengue_pipeline.analysis.runner.subprocess.run', timeout
    )
    assert not OCIAnalysisRunner(settings).run(job).succeeded
