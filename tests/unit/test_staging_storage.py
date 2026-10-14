"""Safety checks for the staging filesystem validation script."""

from pathlib import Path

import pytest

from infodengue_pipeline.models import SinanMetadata
from infodengue_pipeline.storage.filesystem import (
    FilesystemCanonicalStorage,
)

from scripts.validate_staging_storage import run_validation, validation_root


def _valid_root(tmp_path: Path) -> Path:
    return tmp_path / 'staging-data' / 'sinan' / 'pipeline-validation'


def _environment(root: Path) -> dict[str, str]:
    return {
        'INFODENGUE_PIPELINE_ENVIRONMENT': 'staging',
        'INFODENGUE_PIPELINE_CANONICAL_ROOT': str(root),
    }


def test_rejects_non_staging_environment(tmp_path: Path) -> None:
    """Only explicit staging configuration is accepted."""
    env = _environment(_valid_root(tmp_path))
    env['INFODENGUE_PIPELINE_ENVIRONMENT'] = 'prod'
    with pytest.raises(RuntimeError, match='environment=staging'):
        validation_root(env)


def test_rejects_missing_explicit_root() -> None:
    """The validation never guesses a filesystem root."""
    with pytest.raises(RuntimeError, match='CANONICAL_ROOT is required'):
        validation_root({'INFODENGUE_PIPELINE_ENVIRONMENT': 'staging'})


@pytest.mark.parametrize(
    'root',
    [
        '/',
        '/mnt/storagebox-infodengue',
        '/opt/services/ingestion/sinan/imported',
        '/opt/services/infodengue-pipeline/staging-data/sinan/other',
    ],
)
def test_rejects_unsafe_or_wrong_root(root: str) -> None:
    """Known production and nonvalidation paths are refused."""
    with pytest.raises(RuntimeError):
        validation_root(_environment(Path(root)))


def test_accepts_explicit_local_staging_validation_root(
    tmp_path: Path,
) -> None:
    """The designated local staging root is accepted."""
    root = _valid_root(tmp_path)
    assert validation_root(_environment(root)) == root.resolve()


def test_canonical_destinations_cover_all_diseases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All three canonical disease destinations stay under the root."""
    root = _valid_root(tmp_path)
    destinations: list[Path] = []
    original = FilesystemCanonicalStorage.destination

    def capture(
        self: FilesystemCanonicalStorage, metadata: SinanMetadata
    ) -> Path:
        path = original(self, metadata)
        destinations.append(path)
        return path

    monkeypatch.setattr(FilesystemCanonicalStorage, 'destination', capture)
    run_validation(root)
    assert [path.name for path in destinations] == [
        'DenInfodengue_BR_202639.csv',
        'ChikInfodengue_BR_202639.csv',
        'Infodengue_BR_202639.csv',
    ]
    assert [path.parent.parent.parent.name for path in destinations] == [
        'dengue',
        'chik',
        'zika',
    ]
    assert all(path.is_relative_to(root) for path in destinations)


def test_containment_rejects_symlinked_parent(tmp_path: Path) -> None:
    """A symlink cannot redirect writes outside the validation tree."""
    root = _valid_root(tmp_path)
    root.mkdir(parents=True)
    outside = tmp_path / 'outside'
    outside.mkdir()
    (root / 'br').symlink_to(outside, target_is_directory=True)
    with pytest.raises((RuntimeError, ValueError), match=r'escapes|outside'):
        run_validation(root)
    assert not list(outside.iterdir())


def test_cleanup_removes_only_validation_owned_files(tmp_path: Path) -> None:
    """Cleanup preserves unrelated files and the validation root."""
    root = _valid_root(tmp_path)
    root.mkdir(parents=True)
    unrelated_dir = root / 'keep'
    unrelated_dir.mkdir()
    unrelated = unrelated_dir / 'unrelated.csv'
    unrelated.write_bytes(b'preserve me')
    run_validation(root)
    assert unrelated.read_bytes() == b'preserve me'
    assert unrelated_dir.is_dir()
    assert root.is_dir()
    assert sorted(path.name for path in unrelated_dir.iterdir()) == [
        'unrelated.csv'
    ]
