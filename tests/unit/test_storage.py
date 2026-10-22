"""Canonical path construction and transient object ingress boundaries."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from infodengue_pipeline.models import (
    Coverage,
    Disease,
    Epiweek,
    FileFormat,
    SinanMetadata,
)
from infodengue_pipeline.storage.filesystem import FilesystemCanonicalStorage
from infodengue_pipeline.storage.ingress import object_identity
from infodengue_pipeline.storage.minio import (
    MinioObjectStore,
    build_object_store,
    configure_development_lifecycle,
)


@pytest.mark.parametrize(
    ('coverage', 'disease', 'file_format', 'expected'),
    [
        (
            Coverage.MG,
            Disease.DENGUE,
            FileFormat.DBF,
            'mg/dbf/dengue/2024/202452/DenInfodengue_MG_202452.dbf',
        ),
        (
            Coverage.MG,
            Disease.CHIKUNGUNYA,
            FileFormat.DBF,
            'mg/dbf/chik/2024/202452/ChikInfodengue_MG_202452.dbf',
        ),
        (
            Coverage.BR,
            Disease.DENGUE,
            FileFormat.CSV,
            'br/csv/dengue/2026/202639/DenInfodengue_BR_202639.csv',
        ),
        (
            Coverage.SP,
            Disease.ZIKA,
            FileFormat.CSV,
            'sp/csv/zika/2026/202639/Infodengue_SP_202639.csv',
        ),
    ],
)
def test_canonical_base_destination(
    tmp_path: Path,
    coverage: Coverage,
    disease: Disease,
    file_format: FileFormat,
    expected: str,
) -> None:
    """Canonical base names and paths follow validated metadata."""
    storage = FilesystemCanonicalStorage(tmp_path)
    metadata = SinanMetadata(
        disease,
        coverage,
        Epiweek.parse(202452 if coverage is Coverage.MG else 202639),
        file_format,
        'anything.dbf',
    )
    destination = storage.base_destination(metadata)
    assert destination == tmp_path / expected
    assert destination.is_relative_to(tmp_path)


def test_arbitrary_source_name_does_not_change_destination(
    tmp_path: Path,
) -> None:
    """Incoming names are audit data and never affect canonical routing."""
    storage = FilesystemCanonicalStorage(tmp_path)
    metadata = SinanMetadata(
        Disease.DENGUE,
        Coverage.MG,
        Epiweek.parse(202452),
        FileFormat.DBF,
        'DENGON_MG_1.dbf',
    )
    other = SinanMetadata(
        Disease.DENGUE,
        Coverage.MG,
        Epiweek.parse(202452),
        FileFormat.DBF,
        'dados_final.dbf',
    )
    traversal_name = SinanMetadata(
        Disease.DENGUE,
        Coverage.MG,
        Epiweek.parse(202452),
        FileFormat.DBF,
        '../../outside.dbf',
    )
    assert storage.base_destination(metadata) == storage.base_destination(
        other
    )
    assert storage.base_destination(metadata) == storage.base_destination(
        traversal_name
    )


@pytest.mark.parametrize('coverage', ['../MG', 'br', 'XX'])
def test_coverage_validation(coverage: str) -> None:
    """Only explicitly supported coverage values can be normalized."""
    with pytest.raises(ValueError):
        Coverage(coverage)


def test_minio_materialization_is_atomic_and_contained(tmp_path: Path) -> None:
    """A complete download becomes visible with one atomic replacement."""
    client = Mock()
    client.stat_object.return_value = Mock(
        size=4, etag='etag', version_id='version-1'
    )

    def download(_bucket: str, _key: str, path: str, **_kwargs: str) -> None:
        Path(path).write_bytes(b'data')

    client.fget_object.side_effect = download
    client.bucket_exists.return_value = True
    client.list_objects.return_value = [
        Mock(
            object_name='raw.csv', size=4, etag='etag', version_id='version-1'
        )
    ]
    store = MinioObjectStore(client, 'sinan-infodengue-dev', tmp_path)
    target = tmp_path / 'nested' / 'raw.csv'
    materialized = store.materialize('../arbitrary/raw.csv', target)
    assert store.bucket_exists()
    assert store.list()[0].version_id == 'version-1'
    assert materialized.bucket == 'sinan-infodengue-dev'
    assert materialized.key == '../arbitrary/raw.csv'
    assert materialized.etag == 'etag'
    assert materialized.version_id == 'version-1'
    assert target.read_bytes() == b'data'
    assert client.fget_object.call_args.kwargs == {'version_id': 'version-1'}
    assert list(target.parent.glob('*.part-*')) == []
    with pytest.raises(ValueError):
        store.materialize('x', tmp_path.parent / 'escape')


def test_unversioned_minio_sentinel_has_one_identity(tmp_path: Path) -> None:
    """MinIO list's null sentinel must match unversioned stat metadata."""
    client = Mock()
    client.list_objects.return_value = [
        Mock(object_name='raw.dbf', size=4, etag='etag', version_id='null')
    ]
    client.stat_object.return_value = Mock(
        size=4, etag='etag', version_id=None
    )

    def download(_bucket: str, _key: str, path: str) -> None:
        Path(path).write_bytes(b'data')

    client.fget_object.side_effect = download
    store = MinioObjectStore(client, 'bucket', tmp_path)
    listed = store.list()[0]
    stated = store.stat('raw.dbf', version_id='null')
    target = tmp_path / 'raw.dbf'
    materialized = store.materialize('raw.dbf', target, version_id='null')

    assert listed.version_id is None
    assert stated.version_id is None
    assert object_identity(listed) == object_identity(stated)
    assert materialized.version_id is None
    assert target.read_bytes() == b'data'
    assert list(tmp_path.glob('*.part-*')) == []
    assert all(
        'version_id' not in call.kwargs
        for call in client.stat_object.call_args_list
    )
    assert 'version_id' not in client.fget_object.call_args.kwargs


def test_stat_normalizes_sdk_null_and_real_versions(tmp_path: Path) -> None:
    """Both SDK response boundaries normalize only the literal sentinel."""
    client = Mock()
    client.stat_object.return_value = Mock(
        size=4, etag='etag', version_id='null'
    )
    store = MinioObjectStore(client, 'bucket', tmp_path)
    assert store.stat('raw.dbf').version_id is None

    client.stat_object.return_value.version_id = 'version-1'
    assert store.stat('raw.dbf', 'version-1').version_id == 'version-1'
    assert client.stat_object.call_args.kwargs == {'version_id': 'version-1'}


def test_minio_failure_cleans_temporary_and_preserves_final(
    tmp_path: Path,
) -> None:
    """Failed and incomplete downloads never leave a visible final file."""
    client = Mock()
    client.stat_object.return_value = Mock(
        size=9, etag='etag', version_id=None
    )

    def partial(_bucket: str, _key: str, path: str) -> None:
        Path(path).write_bytes(b'bad')
        raise RuntimeError('download failed')

    client.fget_object.side_effect = partial
    store = MinioObjectStore(client, 'bucket', tmp_path)
    target = tmp_path / 'out.csv'
    with pytest.raises(RuntimeError):
        store.materialize('object.csv', target)
    assert not target.exists()
    assert list(tmp_path.glob('*.part-*')) == []


def test_lifecycle_is_restricted_to_dev() -> None:
    """A bootstrap cannot alter a production bucket."""
    client = Mock()
    with pytest.raises(ValueError):
        configure_development_lifecycle(
            client, bucket='sinan-infodengue', environment='prod'
        )
    client.set_bucket_lifecycle.assert_not_called()


def test_lifecycle_sets_seven_day_expiration() -> None:
    """Development ingress objects expire after seven days."""
    client = Mock()
    configure_development_lifecycle(
        client, bucket='sinan-infodengue-dev', environment='dev'
    )
    client.set_bucket_lifecycle.assert_called_once()


def test_object_store_builder_uses_configured_settings(
    monkeypatch, tmp_path
) -> None:
    """The MinIO factory binds credentials, bucket, and incoming root."""
    from infodengue_pipeline.config import Settings
    from infodengue_pipeline.storage import minio

    client = Mock()
    monkeypatch.setattr(minio, 'Minio', Mock(return_value=client))
    settings = Settings(
        _env_file=None,
        minio_endpoint='minio:9000',
        minio_access_key='access',
        minio_secret_key='secret',
        minio_bucket='sinan-infodengue-dev',
        incoming_root=tmp_path,
    )
    store = build_object_store(settings)
    assert isinstance(store, MinioObjectStore)
    minio.Minio.assert_called_once()
