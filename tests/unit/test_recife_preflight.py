"""Fail-closed tests for the operator-facing Recife preflight."""

from pathlib import Path

from scripts.recife_preflight import ATTESTATIONS, evaluate


def evidence(tmp_path: Path) -> dict[str, object]:
    """Build complete safe metadata without network or database access."""
    target = ('pr30', '10.0.0.2', 5432)
    probe = {
        'database': target[0],
        'server_address': target[1],
        'server_port': target[2],
    }
    values: dict[str, object] = {
        'commit_sha': 'a' * 40,
        'images': {
            name: 'repo/' + name + '@sha256:' + 'a' * 64
            for name in ('core', 'analysis', 'bulletin')
        },
        'project': 'infodengue-pipeline-pr30',
        'minio_prefix': 'pr30/recife/',
        'week': 202644,
        'object_key': 'pr30/recife/dengue.csv',
        'object_sha256': 'b' * 64,
        'object_size': 100,
        'approved_target': probe,
        'writer_probe': {**probe, 'role': 'rehearsal-analysis'},
        'history_probe': {**probe, 'role': 'rehearsal-history'},
        'source_probe': {
            'database': 'source',
            'server_address': '10.0.0.1',
            'server_port': 5432,
            'role': 'rehearsal-source',
        },
        'capacity_path': str(tmp_path),
        'required_free_bytes': 1,
    }
    values.update({name: True for name in ATTESTATIONS})
    return values


def statuses(result: dict[str, object]) -> dict[str, str]:
    """Map stable check identifiers to their statuses."""
    return {item['id']: item['status'] for item in result['checks']}


def test_complete_evidence_passes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Require all independent gates before a pass decision."""
    monkeypatch.setattr(
        'scripts.recife_preflight.subprocess.check_output',
        lambda args, **_k: (
            b''
            if 'diff' in args
            else 'a' * 40 + '\n'
            if 'rev-parse' in args
            else 'feat/intramunicipal-bulletin-recife\n'
        ),
    )
    result = evaluate(evidence(tmp_path), tmp_path)
    assert result['status'] == 'PASS'


def test_unsafe_evidence_blocks_without_secrets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Reject shared DB, mutable image, bad week and prefix collision."""
    monkeypatch.setattr(
        'scripts.recife_preflight.subprocess.check_output',
        lambda args, **_k: (
            b''
            if 'diff' in args
            else 'a' * 40 + '\n'
            if 'rev-parse' in args
            else 'feat/intramunicipal-bulletin-recife\n'
        ),
    )
    values = evidence(tmp_path)
    values['source_probe'] = {
        **values['writer_probe'],
        'role': 'rehearsal-source',
    }
    values['images']['analysis'] = 'analysis:latest'
    values['week'] = 202654
    values['object_key'] = 'other/dengue.csv'
    values['required_free_bytes'] = 10**30
    values['minio_prefix_empty_and_exclusive'] = False
    result = evaluate(values, tmp_path)
    checks = statuses(result)
    assert result['status'] == 'BLOCKED'
    for name in (
        'source_isolation',
        'analysis_image',
        'selected_week',
        'sinan_object',
        'disk_and_wal_capacity',
        'minio_prefix_empty_and_exclusive',
    ):
        assert checks[name] == 'BLOCKED'
    assert 'secret' not in str(result)
