"""Production validation checks must stay finite and read-only."""

import json
import sys

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.production import validate as probes

from scripts import validate_prod as host

_IMAGE = 'example/analysis@sha256:' + 'a' * 64


def settings(tmp_path: Path, **changes: object) -> Settings:
    """Build a safe production-shaped settings object for isolated probes."""
    defaults: dict[str, object] = {
        'environment': 'prod',
        'analysis_image': _IMAGE,
        'alerta_source_database_url': 'postgresql://reader:secret@source/db',
        'analysis_database_url': 'postgresql://analyst:secret@analysis/db',
        'incoming_root': tmp_path,
        'analysis_output_root': tmp_path,
    }
    defaults.update(changes)
    return Settings(_env_file=None, **defaults)


def test_ingestion_success_uses_read_only_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Check all core dependencies without a write-capable adapter call."""
    database = MagicMock()
    monkeypatch.setattr(probes, 'check_database', database)
    store = MagicMock()
    store.bucket_exists.return_value = True
    pool = MagicMock()
    pool_options: dict[str, object] = {}

    def make_pool(**options: object) -> MagicMock:
        pool_options.update(options)
        return pool

    monkeypatch.setattr(probes.urllib3, 'PoolManager', make_pool)
    monkeypatch.setattr(
        probes,
        'build_object_store',
        lambda _s, *, http_client: store if http_client is pool else None,
    )
    connection = MagicMock()
    connection.__enter__.return_value = connection
    broker_options: dict[str, object] = {}

    def make_connection(*_args: object, **options: object) -> MagicMock:
        broker_options.update(options)
        return connection

    monkeypatch.setattr(probes, 'Connection', make_connection)

    result = probes.validate('ingestion', settings(tmp_path))

    assert 'pipeline PostgreSQL and migrations: ok' in result
    assert 'RabbitMQ: ok' in result
    assert 'MinIO bucket: ok' in result
    assert 'incoming directory: ok' in result
    assert database.call_count == 2
    assert database.call_args_list[0].kwargs == {'migration': True}
    store.bucket_exists.assert_called_once_with()
    assert connection.connect.call_count == 1
    assert connection.connect.call_args.kwargs == {}
    assert broker_options['connect_timeout'] == 5
    assert broker_options['transport_options'] == {
        'read_timeout': 5,
        'write_timeout': 5,
    }
    assert pool_options['retries'] is False
    timeout = pool_options['timeout']
    assert isinstance(timeout, probes.Timeout)
    assert timeout.connect_timeout == 5
    pool.clear.assert_called_once_with()


def test_analysis_success_and_permission_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Analysis checks its own DB and output permissions in its worker."""
    database = MagicMock()
    monkeypatch.setattr(probes, 'check_database', database)
    assert 'analysis output directory: ok' in probes.validate(
        'analysis', settings(tmp_path)
    )
    database.assert_called_once_with('postgresql://analyst:secret@analysis/db')
    monkeypatch.setattr(probes.os, 'access', lambda *_a: False)
    with pytest.raises(RuntimeError, match='analysis output directory'):
        probes.validate('analysis', settings(tmp_path))


@pytest.mark.parametrize(
    ('change', 'message'),
    [
        ({'environment': 'staging'}, 'environment must be prod'),
        ({'analysis_load': True}, 'analysis_load must be false'),
        ({'analysis_image': 'mutable:latest'}, 'reviewed digest'),
        ({'alerta_source_database_url': None}, 'source database'),
    ],
)
def test_configuration_refusal(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    """Unsafe or incomplete settings fail before connecting to services."""
    with pytest.raises(ValueError, match=message):
        probes.validate('ingestion', settings(tmp_path, **change))


def test_unavailable_dependency_hides_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A driver error cannot leak a connection URL into operator output."""
    monkeypatch.setattr(
        probes,
        'check_database',
        lambda *_a, **_k: (_ for _ in ()).throw(
            RuntimeError('postgresql://reader:secret@source/db')
        ),
    )
    with pytest.raises(RuntimeError, match='pipeline PostgreSQL') as error:
        probes.validate('ingestion', settings(tmp_path))
    assert 'secret' not in str(error.value)


def test_minio_timeout_names_bucket_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stalled bucket probe reports MinIO and hides SDK diagnostics."""
    monkeypatch.setattr(probes, 'check_database', lambda *_a, **_k: None)
    connection = MagicMock()
    connection.__enter__.return_value = connection
    monkeypatch.setattr(probes, 'Connection', lambda *_a, **_k: connection)
    store = MagicMock()
    store.bucket_exists.side_effect = TimeoutError('password=secret')
    monkeypatch.setattr(probes, 'build_object_store', lambda *_a, **_k: store)
    with pytest.raises(RuntimeError, match='MinIO bucket') as error:
        probes.validate('ingestion', settings(tmp_path))
    assert 'secret' not in str(error.value)


def test_database_probe_issues_only_selects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Migration inspection executes SELECT statements and disposes engine."""
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalars.return_value.all.return_value = [
        'current_head'
    ]
    options: dict[str, object] = {}

    def make_engine(_url: str, **kwargs: object) -> MagicMock:
        options.update(kwargs)
        return engine

    monkeypatch.setattr(probes, 'create_engine', make_engine)
    monkeypatch.setattr(
        probes, 'expected_migration_heads', lambda: {'current_head'}
    )

    probes.check_database('postgresql://reader:secret@host/db', migration=True)

    statements = [
        str(call.args[0]) for call in connection.execute.call_args_list
    ]
    assert statements == [
        'SELECT 1',
        'SELECT version_num FROM alembic_version',
        'SELECT 1 FROM pipeline.analysis_job LIMIT 1',
    ]
    engine.dispose.assert_called_once_with()
    assert options['connect_args'] == {
        'connect_timeout': 5,
        'options': '-c statement_timeout=5000',
    }


def test_database_revision_failure_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An outdated migration is rejected without applying migrations."""
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalars.return_value.all.return_value = [
        'old'
    ]
    monkeypatch.setattr(probes, 'create_engine', lambda *_a, **_k: engine)
    monkeypatch.setattr(probes, 'expected_migration_heads', lambda: {'new'})
    with pytest.raises(RuntimeError, match='migrations are not at head'):
        probes.check_database('postgresql://reader@host/db', migration=True)
    engine.dispose.assert_called_once_with()


def test_migration_heads_come_from_checked_in_scripts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The expected revision is read from Alembic instead of a constant."""
    script = MagicMock()
    script.get_heads.return_value = ['future_revision']
    monkeypatch.setattr(
        probes.ScriptDirectory, 'from_config', lambda _config: script
    )
    assert probes.expected_migration_heads() == {'future_revision'}


@pytest.mark.parametrize(
    'image',
    [
        'example/analysis@sha256:1234',
        'example/analysis@sha256:' + 'g' * 64,
        _IMAGE + '-suffix',
        'example/analysis@sha256:' + 'A' * 64,
        '@sha256:' + 'a' * 64,
    ],
)
def test_incomplete_or_invalid_image_is_rejected(
    tmp_path: Path, image: str
) -> None:
    """Both configuration layers require an anchored 64-digit hex digest."""
    assert not probes.valid_analysis_image(image)
    with pytest.raises(ValueError, match='analysis image'):
        probes.validate('analysis', settings(tmp_path, analysis_image=image))


def test_host_config_and_running_services() -> None:
    """Host checks topology, image provenance, state, and concurrency."""
    image = _IMAGE
    config = {
        'services': {
            name: {
                'image': image,
                'environment': {
                    'INFODENGUE_PIPELINE_ENVIRONMENT': 'prod',
                    'INFODENGUE_PIPELINE_ANALYSIS_LOAD': 'false',
                    'INFODENGUE_PIPELINE_ANALYSIS_IMAGE': image,
                },
                'command': ['--concurrency=1'],
            }
            for name in host._WORKERS
        }
    }
    assert host.validate_config(config) == image
    rows = [
        {'Service': name, 'State': 'running', 'Health': 'healthy'}
        for name in host._WORKERS
    ] + [{'Service': 'migrate', 'ExitCode': 0}]
    host.validate_running(json.dumps(rows))
    config['services']['worker-analysis']['command'] = ['--concurrency=2']
    with pytest.raises(RuntimeError, match=r'worker-analysis.*concurrency'):
        host.validate_config(config)
    rows[0]['Health'] = 'unhealthy'
    with pytest.raises(RuntimeError, match=r'worker-ingestion.*healthy'):
        host.validate_running(json.dumps(rows))


def test_host_rejects_partial_analysis_digest() -> None:
    """The host blocks invalid images before any live worker probe."""
    image = 'example/analysis@sha256:short'
    config = {
        'services': {
            name: {
                'image': image,
                'environment': {
                    'INFODENGUE_PIPELINE_ENVIRONMENT': 'prod',
                    'INFODENGUE_PIPELINE_ANALYSIS_LOAD': 'false',
                    'INFODENGUE_PIPELINE_ANALYSIS_IMAGE': image,
                },
                'command': ['--concurrency=1'],
            }
            for name in host._WORKERS
        }
    }
    with pytest.raises(RuntimeError, match='analysis image'):
        host.validate_config(config)


def test_host_main_runs_only_inspection_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The host orchestrator never starts services or executes migrations."""
    (tmp_path / '.env.prod').touch()
    monkeypatch.chdir(tmp_path)
    image = _IMAGE
    config = {
        'services': {
            name: {
                'image': image,
                'environment': {
                    'INFODENGUE_PIPELINE_ENVIRONMENT': 'prod',
                    'INFODENGUE_PIPELINE_ANALYSIS_LOAD': 'false',
                    'INFODENGUE_PIPELINE_ANALYSIS_IMAGE': image,
                },
                'command': ['--concurrency=1'],
            }
            for name in host._WORKERS
        }
    }
    rows = [
        {'Service': name, 'State': 'running', 'Health': 'healthy'}
        for name in host._WORKERS
    ] + [{'Service': 'migrate', 'ExitCode': 0}]
    calls: list[tuple[str, ...]] = []

    def fake_command(*args: str) -> str:
        calls.append(args)
        if args[0] == 'config':
            return json.dumps(config)
        if args[0] == 'ps':
            return json.dumps(rows)
        return 'probe: ok\n'

    monkeypatch.setattr(host, 'command', fake_command)
    host.main()
    assert [call[0] for call in calls] == ['config', 'ps', 'exec', 'exec']
    assert calls[2][2] == 'worker-ingestion'
    assert calls[3][2] == 'worker-analysis'
    assert capsys.readouterr().out.endswith('production validation passed\n')


def test_compose_failure_suppresses_secret_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compose stderr stays out of validation exceptions."""
    result = MagicMock(returncode=1, stderr='password=secret', stdout='')
    monkeypatch.setattr(host.subprocess, 'run', lambda *_a, **_k: result)
    with pytest.raises(RuntimeError, match='Compose config failed') as error:
        host.command('config')
    assert 'secret' not in str(error.value)


@pytest.mark.parametrize(
    ('args', 'boundary', 'seconds'),
    [
        (('config', '--format', 'json'), 'Compose config', 15),
        (('ps', '--all', '--format', 'json'), 'Compose ps', 15),
        (('exec', '-T', 'worker-analysis'), 'worker-analysis', 60),
    ],
)
def test_compose_timeout_names_boundary_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
    args: tuple[str, ...],
    boundary: str,
    seconds: int,
) -> None:
    """Every Compose operation has a bounded, credential-safe failure."""
    seen: list[int] = []

    def timeout(*_args: object, **kwargs: object) -> None:
        seen.append(kwargs['timeout'])  # type: ignore[arg-type]
        raise host.subprocess.TimeoutExpired(
            cmd='docker password=secret',
            timeout=seconds,
            output='password=secret',
        )

    monkeypatch.setattr(host.subprocess, 'run', timeout)
    with pytest.raises(RuntimeError, match=boundary) as error:
        host.command(*args)
    assert seen == [seconds]
    assert 'secret' not in str(error.value)


def test_worker_failure_preserves_named_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The host displays a worker's sanitized failure label."""
    result = MagicMock(
        returncode=1,
        stderr='password=secret',
        stdout=(
            'production validation failed: '
            'MinIO bucket: unavailable or invalid\n'
        ),
    )
    monkeypatch.setattr(host.subprocess, 'run', lambda *_a, **_k: result)
    with pytest.raises(RuntimeError, match='MinIO bucket') as error:
        host.command('exec', '-T', 'worker-ingestion')
    assert 'secret' not in str(error.value)


def test_worker_entrypoint_reports_boundary_without_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CLI emits concise output and a failing exit status."""
    monkeypatch.setattr(sys, 'argv', ['validate', 'analysis'])
    monkeypatch.setattr(probes, 'get_settings', lambda: settings(tmp_path))
    monkeypatch.setattr(probes, 'check_database', lambda *_a: None)
    probes.main()
    assert 'analysis output directory: ok' in capsys.readouterr().out

    def unavailable(*_args: object) -> None:
        raise RuntimeError('postgresql://analyst:secret@analysis/db')

    monkeypatch.setattr(probes, 'check_database', unavailable)
    with pytest.raises(SystemExit) as error:
        probes.main()
    assert error.value.code == 1
    output = capsys.readouterr().out
    assert 'analysis database: unavailable or invalid' in output
    assert 'secret' not in output
