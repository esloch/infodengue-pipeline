"""Validate canonical filesystem storage on the staging server's local disk."""

from __future__ import annotations

import hashlib
import os

from pathlib import Path

from infodengue_pipeline.adapters.filesystem.canonical_storage import (
    FilesystemCanonicalStorage,
)
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek

_VALIDATION_SUFFIX = Path('staging-data/sinan/pipeline-validation')
_REFUSED_ROOTS = (
    Path('/mnt/storagebox-infodengue'),
    Path('/mnt/storagebox-infodengue/sinan'),
    Path('/mnt/storagebox-infodengue/sinan/imported'),
    Path('/'),
    Path('/opt/services/ingestion/sinan/imported'),
    Path('/mnt/storagebox-staging/sinan/imported'),
)
_FIXTURES = (
    (Disease.DENGUE, b'NU_NOTIFIC,ID_AGRAVO,SEM_NOT\nsmoke,A90,202639\n'),
    (
        Disease.CHIKUNGUNYA,
        b'NU_NOTIFIC,ID_AGRAVO,SEM_NOT\nsmoke,A92.0,202639\n',
    ),
    (Disease.ZIKA, b'NU_NOTIFIC,ID_AGRAVO,SEM_NOT\nsmoke,A928,202639\n'),
)


def validation_root(
    environ: dict[str, str] | os._Environ[str] = os.environ,
) -> Path:
    """Validate staging environment and return its explicit canonical root."""
    if environ.get('INFODENGUE_PIPELINE_ENVIRONMENT') != 'staging':
        raise RuntimeError('storage smoke requires environment=staging')
    raw_root = environ.get('INFODENGUE_PIPELINE_CANONICAL_ROOT')
    if not raw_root:
        raise RuntimeError('INFODENGUE_PIPELINE_CANONICAL_ROOT is required')
    root = Path(raw_root).resolve()
    if root in _REFUSED_ROOTS:
        raise RuntimeError(f'refusing unsafe canonical root: {root}')
    if root == Path('/'):
        raise RuntimeError('refusing filesystem root')
    if not root.as_posix().endswith('/' + _VALIDATION_SUFFIX.as_posix()):
        raise RuntimeError(
            'canonical root must end with '
            '/staging-data/sinan/pipeline-validation'
        )
    return root


def _ensure_parent(parent: Path, root: Path, created_dirs: list[Path]) -> None:
    """Create missing parent components, checking each remains under root."""
    missing: list[Path] = []
    current = parent
    while not current.exists():
        if current == root or not current.is_relative_to(root):
            raise RuntimeError('destination parent escapes validation root')
        missing.append(current)
        current = current.parent
    if not current.resolve().is_relative_to(root):
        raise RuntimeError(
            'destination parent resolves outside validation root'
        )
    for directory in reversed(missing):
        directory.mkdir()
        if not directory.resolve().is_relative_to(root):
            raise RuntimeError(
                'created directory resolves outside validation root'
            )
        created_dirs.append(directory)


def run_smoke(root: Path) -> None:
    """Write, verify, and clean up tiny canonical CSV fixtures."""
    root.mkdir(parents=True, exist_ok=True)
    root = root.resolve()
    if not root.as_posix().endswith('/' + _VALIDATION_SUFFIX.as_posix()):
        raise RuntimeError('canonical root has an invalid validation suffix')
    storage = FilesystemCanonicalStorage(root)
    week = Epiweek.parse(202639)
    created_files: list[Path] = []
    created_dirs: list[Path] = []
    try:
        for disease, body in _FIXTURES:
            filename = f'{disease.filename_prefix}_BR_{week}.csv'
            destination = storage.destination(
                country='br',
                file_type='csv',
                disease=disease,
                epiweek=week,
                filename=filename,
            )
            if not destination.is_relative_to(root):
                raise RuntimeError(
                    'canonical destination escapes validation root'
                )
            _ensure_parent(destination.parent, root, created_dirs)
            if destination.exists():
                raise FileExistsError(
                    f'refusing to overwrite existing file: {destination}'
                )
            with destination.open('xb') as output:
                created_files.append(destination)
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            if not destination.resolve().is_relative_to(root):
                raise RuntimeError(
                    'canonical file resolves outside validation root'
                )
            destination.stat()
            actual = destination.read_bytes()
            if actual != body:
                raise RuntimeError(
                    f'filesystem readback differs for {destination}'
                )
            if (
                hashlib.sha256(actual).digest()
                != hashlib.sha256(body).digest()
            ):
                raise RuntimeError(
                    f'filesystem SHA-256 differs for {destination}'
                )
    finally:
        for file_path in reversed(created_files):
            if file_path.resolve().is_relative_to(root):
                file_path.unlink(missing_ok=True)
        for directory in reversed(created_dirs):
            if directory.resolve().is_relative_to(root):
                try:
                    directory.rmdir()
                except OSError:
                    pass


def main() -> None:
    """Run the staging filesystem validation using the explicit root."""
    run_smoke(validation_root())
    print('staging filesystem smoke passed (local filesystem only)')


if __name__ == '__main__':
    main()
