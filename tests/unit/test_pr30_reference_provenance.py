"""Known staging reference provenance checks."""

import json

from pathlib import Path

import pytest

from scripts import validate_pr30_reference as reference


def test_record_states_known_hashes_and_unknown_approval() -> None:
    """Record only supplied hashes and an explicit unknown approval state."""
    record = json.loads(reference.RECORD.read_text())
    assert record['adapted_sha256'] == (
        '4c59f5c1f0071d652ed470a5472432e269c2bcceb0a18c584887e12c583b8b7e'
    )
    assert record['source_sha256'] == (
        '5a4262f62d750007ea61eaa6425e4c06071fdee5ce57000003898e4a6b45c26e'
    )
    assert record['approval_status'] == 'not documented'


def test_hash_validation_checks_optional_original_without_mutating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validate both known byte streams and leave the source unchanged."""
    adapted = tmp_path / 'reference.rds'
    source = tmp_path / 'ref_RF.RData'
    adapted.write_bytes(b'adapted')
    source.write_bytes(b'original')
    record = {
        'adapted_sha256': reference.sha256(adapted),
        'source_sha256': reference.sha256(source),
    }
    manifest = tmp_path / 'provenance.json'
    manifest.write_text(json.dumps(record))
    monkeypatch.setattr(reference, 'RECORD', manifest)
    reference.validate(adapted, source)
    assert source.read_bytes() == b'original'
    source.write_bytes(b'changed')
    with pytest.raises(ValueError, match='upstream'):
        reference.validate(adapted, source)
    adapted.write_bytes(b'changed')
    with pytest.raises(ValueError, match='adapted'):
        reference.validate(adapted)
