"""Guards around staging fixture submission and clean acceptance."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.models import AnalysisJobStatus, Coverage, Epiweek
from infodengue_pipeline.staging.e2e import (
    ValidationResult,
    report,
    require_current_run,
)
from infodengue_pipeline.staging.runner import (
    object_key,
    preflight,
    require_absent,
    require_clean_result,
    run,
    submit,
    writer_client,
)


def settings() -> Settings:
    """Build valid staging settings without reading a private file."""
    return Settings(
        _env_file=None,
        environment='staging',
        minio_prefix='fixtures/',
        minio_endpoint='minio.test:9000',
        minio_bucket='fixtures-test',
        alerta_source_database_url='postgresql://readonly:secret@source/db',
    )


@pytest.mark.parametrize('suffix', ['.csv', '.dbf'])
def test_preflight_accepts_supported_file_and_streams_hash(tmp_path, suffix):
    """Hash supported files without relying on their names for metadata."""
    path = tmp_path / f'anything{suffix}'
    path.write_bytes(b'abc')
    fixture = preflight(settings(), path, 10)
    assert fixture.sha256 == (
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
    )
    assert fixture.size == 3
    assert fixture.suffix == suffix


@pytest.mark.parametrize('kind', ['missing', 'directory', 'unsupported'])
def test_preflight_rejects_invalid_path(tmp_path, kind):
    """Reject missing, directory, and unsupported fixture paths."""
    path = tmp_path / ('bad.txt' if kind == 'unsupported' else 'bad.dbf')
    if kind == 'directory':
        path.mkdir()
    elif kind == 'unsupported':
        path.write_bytes(b'abc')
    with pytest.raises(ValueError, match='fixture'):
        preflight(settings(), path, 10)


@pytest.mark.parametrize(
    ('change', 'message'),
    [
        ({'environment': 'dev'}, 'environment'),
        ({'analysis_load': True}, 'analysis_load'),
        ({'alerta_source_database_url': None}, 'source database'),
        ({'minio_prefix': ''}, 'prefix'),
        ({'minio_prefix': '../escape/'}, 'unsafe MinIO prefix'),
    ],
)
def test_preflight_rejects_unsafe_settings(tmp_path, change, message):
    """Stop before object submission when staging settings are unsafe."""
    path = tmp_path / 'test.dbf'
    path.write_bytes(b'abc')
    with pytest.raises(ValueError, match=message):
        preflight(settings().model_copy(update=change), path, 10)


def test_writer_credentials_are_dedicated(monkeypatch):
    """Never fall back to normal service credentials."""
    monkeypatch.delenv('STAGING_E2E_MINIO_ACCESS_KEY', raising=False)
    monkeypatch.delenv('STAGING_E2E_MINIO_SECRET_KEY', raising=False)
    with pytest.raises(ValueError, match='writer credentials'):
        writer_client(settings())
    monkeypatch.setenv('STAGING_E2E_MINIO_ACCESS_KEY', 'writer')
    with pytest.raises(ValueError, match='writer credentials'):
        writer_client(settings())
    monkeypatch.setenv('STAGING_E2E_MINIO_SECRET_KEY', 'writer-secret')
    from infodengue_pipeline.staging import runner

    seen = {}

    def fake_minio(endpoint, **kwargs):
        seen.update(endpoint=endpoint, **kwargs)
        return object()

    monkeypatch.setattr(runner, 'Minio', fake_minio)
    writer_client(settings())
    assert seen == {
        'endpoint': 'minio.test:9000',
        'access_key': 'writer',
        'secret_key': 'writer-secret',
        'secure': False,
    }


def test_key_is_unique_contained_and_preserves_suffix(tmp_path):
    """Different invocation nonces cannot address the same object."""
    path = tmp_path / 'a.dbf'
    path.write_bytes(b'abc')
    fixture = preflight(settings(), path, 10)
    now = datetime(2026, 10, 22, tzinfo=timezone.utc)
    one = object_key('fixtures/', fixture, now=now, nonce='one')
    two = object_key('fixtures/', fixture, now=now, nonce='two')
    assert one != two
    assert one.startswith('fixtures/e2e-')
    assert one.endswith('.dbf')
    assert '..' not in one


def test_existing_key_rejected_and_upload_size_checked(tmp_path):
    """Only exact-object stat is needed, and mismatched uploads fail."""
    path = tmp_path / 'a.csv'
    path.write_bytes(b'abc')
    fixture = preflight(settings(), path, 10)

    class Client:
        def __init__(self):
            self.calls = []

        def stat_object(self, bucket, key):
            self.calls.append(('stat', bucket, key))
            return SimpleNamespace(size=2)

        def fput_object(self, bucket, key, filename):
            self.calls.append(('put', bucket, key, filename))

    client = Client()
    with pytest.raises(RuntimeError, match='already exists'):
        require_absent(client, 'fixtures-test', 'fixtures/existing.csv')
    assert len(client.calls) == 1
    from infodengue_pipeline.staging import runner

    original = runner.require_absent
    runner.require_absent = lambda *_args: None
    try:
        with pytest.raises(RuntimeError, match='size'):
            submit(settings(), fixture, client, 'fixtures/new.csv')
    finally:
        runner.require_absent = original
    assert (
        'put',
        'fixtures-test',
        'fixtures/new.csv',
        str(path),
    ) in client.calls


def test_fresh_sha_rejected_before_upload(tmp_path, monkeypatch):
    """Any upstream run status blocks the fixture SHA before MinIO calls."""
    path = tmp_path / 'a.dbf'
    path.write_bytes(b'abc')
    from infodengue_pipeline.staging import runner

    client = object()
    monkeypatch.setattr(runner, 'writer_client', lambda _settings: client)
    monkeypatch.setattr(
        runner,
        'build_source',
        lambda _url: SimpleNamespace(exists_sha256=lambda _sha: True),
    )
    monkeypatch.setattr(
        runner,
        'submit',
        lambda *_args: pytest.fail('uploaded reused fixture'),
    )
    with pytest.raises(RuntimeError, match='already exists'):
        run(settings(), path, 10)


def test_validator_freshness_and_progress(capsys):
    """Old completion fails and stage lines are flushed."""
    now = datetime.now(timezone.utc)
    with pytest.raises(RuntimeError, match='predates'):
        require_current_run(now - timedelta(seconds=1), now)
    require_current_run(now + timedelta(seconds=1), now)
    report('workflow readiness', waiting=True)
    report('workflow ready')
    output = capsys.readouterr().out
    assert 'waiting: workflow readiness' in output
    assert '[OK] workflow ready' in output


def test_clean_attempt_requirement():
    """Only one successful execution attempt qualifies as clean acceptance."""
    identity = uuid4()
    result = ValidationResult(
        'fixtures/key.dbf',
        'a' * 64,
        identity,
        Coverage.MG,
        Epiweek.parse(202412),
        identity,
        identity,
        AnalysisJobStatus.SUCCEEDED,
        1,
        Path('/tmp/output'),
    )
    require_clean_result(result)
    with pytest.raises(RuntimeError, match='not clean'):
        require_clean_result(
            ValidationResult(
                result.object_key,
                result.sha256,
                result.run_id,
                result.coverage,
                result.epiweek,
                result.workflow_id,
                result.job_id,
                result.job_status,
                2,
                result.output_dir,
            )
        )


def test_runner_submits_and_reuses_validator(tmp_path, monkeypatch):
    """A fresh fixture flows through upload and shared validation once."""
    from infodengue_pipeline.staging import runner

    path = tmp_path / 'a.dbf'
    path.write_bytes(b'abc')
    events = []
    monkeypatch.setattr(
        runner,
        'writer_client',
        lambda _settings: events.append('writer') or object(),
    )
    monkeypatch.setattr(
        runner,
        'build_source',
        lambda _url: SimpleNamespace(exists_sha256=lambda _sha: False),
    )
    monkeypatch.setattr(
        runner,
        'submit',
        lambda _settings, _fixture, _client, key: events.append(
            ('upload', key)
        ),
    )
    identity = uuid4()
    result = ValidationResult(
        'fixtures/key.dbf',
        'a' * 64,
        identity,
        Coverage.MG,
        Epiweek.parse(202412),
        identity,
        identity,
        AnalysisJobStatus.SUCCEEDED,
        1,
        Path('/tmp/output'),
    )
    monkeypatch.setattr(
        runner,
        'validate_e2e',
        lambda key, timeout, configured: (
            events.append(('validate', key)) or result
        ),
    )
    assert run(settings(), path, 10) is result
    assert events[0] == 'writer'
    assert events[1][0] == 'upload'
    assert events[2] == ('validate', events[1][1])


def test_preflight_rejects_timeout_and_key_traversal(tmp_path):
    """Reject unbounded waits and unsafe configured key prefixes."""
    path = tmp_path / 'a.dbf'
    path.write_bytes(b'abc')
    with pytest.raises(ValueError, match='timeout'):
        preflight(settings(), path, 0)
    fixture = preflight(settings(), path, 1)
    with pytest.raises(ValueError, match='unsafe MinIO prefix'):
        object_key('../escape/', fixture)


def test_validator_accepts_missing_materialization_time():
    """Allow legacy ingress rows without a materialization time."""
    require_current_run(datetime.now(timezone.utc), None)


def test_exact_key_not_found_allows_verified_upload(tmp_path):
    """An absent exact object key can be uploaded and verified by stat."""
    from minio.error import S3Error

    path = tmp_path / 'a.csv'
    path.write_bytes(b'abc')
    fixture = preflight(settings(), path, 1)

    class Client:
        def __init__(self):
            self.calls = 0

        def stat_object(self, _bucket, _key):
            self.calls += 1
            if self.calls == 1:
                raise S3Error(None, 'NoSuchKey', None, None, None, None)
            return SimpleNamespace(size=3)

        def fput_object(self, _bucket, _key, _path):
            return None

    client = Client()
    submit(settings(), fixture, client, 'fixtures/new.csv')
    assert client.calls == 2
