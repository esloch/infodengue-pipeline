"""Canonical path and object ingress boundaries."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from infodengue_pipeline.adapters.filesystem.canonical_storage import (
    FilesystemCanonicalStorage,
)
from infodengue_pipeline.adapters.minio.lifecycle import (
    configure_development_lifecycle,
)
from infodengue_pipeline.adapters.minio.object_store import MinioObjectStore
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek


@pytest.mark.parametrize(
    ('disease', 'filename', 'folder'),
    [
        (Disease.DENGUE, 'DenInfodengue_BR_202639.csv', 'dengue'),
        (Disease.CHIKUNGUNYA, 'ChikInfodengue_BR_202639.csv', 'chik'),
        (Disease.ZIKA, 'Infodengue_BR_202639.csv', 'zika'),
    ],
)
def test_canonical_destination(
    tmp_path: Path,
    disease: Disease,
    filename: str,
    folder: str,
) -> None:
    """The path follows country, format, disease, year and epiweek."""
    storage = FilesystemCanonicalStorage(tmp_path)
    path = storage.destination(
        country='br',
        file_type='csv',
        disease=disease,
        epiweek=Epiweek.parse(202639),
        filename=filename,
    )
    expected = tmp_path / 'br' / 'csv' / folder / '2026' / '202639'
    assert path == expected / filename
    assert path.is_relative_to(tmp_path)


@pytest.mark.parametrize('country', ['../br', 'BR', 'br/../../'])
def test_canonical_rejects_invalid_country(
    tmp_path: Path, country: str
) -> None:
    """Country identifiers cannot influence the directory hierarchy."""
    storage = FilesystemCanonicalStorage(tmp_path)
    with pytest.raises(ValueError):
        storage.destination(
            country=country,
            file_type='csv',
            disease=Disease.DENGUE,
            epiweek=Epiweek.parse(202639),
            filename='DenInfodengue_BR_202639.csv',
        )


def test_canonical_rejects_filename_traversal(tmp_path: Path) -> None:
    """Only the canonical filename can become a destination."""
    storage = FilesystemCanonicalStorage(tmp_path)
    with pytest.raises(ValueError):
        storage.destination(
            country='br',
            file_type='csv',
            disease=Disease.DENGUE,
            epiweek=Epiweek.parse(202639),
            filename='../../escape.csv',
        )


def test_minio_adapter_contract(tmp_path: Path) -> None:
    """The adapter returns SDK independent metadata and bounds downloads."""
    client = Mock()
    client.bucket_exists.return_value = True
    client.stat_object.return_value = Mock(size=4, etag='etag')
    client.list_objects.return_value = [
        Mock(object_name='a.csv', size=4, etag='etag')
    ]
    store = MinioObjectStore(client, 'sinan-infodengue-dev', tmp_path)
    assert store.bucket_exists()
    assert store.stat('a.csv').key == 'a.csv'
    assert store.list()[0].size == 4
    assert store.materialize('a.csv', tmp_path / 'a.csv').etag == 'etag'
    client.fget_object.assert_called_once()
    with pytest.raises(ValueError):
        store.materialize('a.csv', tmp_path.parent / 'escape.csv')


def test_lifecycle_is_restricted_to_dev() -> None:
    """A test or bootstrap cannot alter a production bucket."""
    client = Mock()
    with pytest.raises(ValueError):
        configure_development_lifecycle(
            client, bucket='sinan-infodengue', environment='prod'
        )
    client.set_bucket_lifecycle.assert_not_called()
