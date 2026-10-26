"""Regression checks for the mutating production-profile gates."""

import gzip
import hashlib
import json

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.models import Coverage, Epiweek
from infodengue_pipeline.production.analysis_e2e import (
    Target,
    require_profile,
    run,
    validate_artifacts,
    verify_backup,
    verify_target,
)
from pydantic import SecretStr


def settings(**overrides: object) -> Settings:
    """Make a non-secret synthetic rehearsal configuration."""
    values: dict[str, object] = {
        'environment': 'prod',
        'analysis_load': True,
        'minio_prefix': 'issue10/',
        'analysis_database_url': SecretStr(
            'postgresql+psycopg://scoped:secret@staging-db:5432/alerta_stage'
        ),
        'alerta_source_database_url': SecretStr(
            'postgresql+psycopg://reader:secret@staging-db:5432/alerta_stage'
        ),
    }
    values.update(overrides)
    return Settings.model_validate(values)


def test_profile_rejections() -> None:
    """The command requires both the prod profile and mutating load flag."""
    with pytest.raises(ValueError, match='environment is not prod'):
        require_profile(settings(environment='staging'))
    with pytest.raises(ValueError, match='analysis_load=false'):
        require_profile(settings(analysis_load=False))


def test_target_rejection_without_connection() -> None:
    """A mismatched operator target fails before a network probe."""
    with pytest.raises(ValueError, match='target is not staging'):
        verify_target(settings(), Target('wrong', 'alerta_stage', 'scoped'))


def test_backup_requires_digest_and_restore_evidence(tmp_path: Path) -> None:
    """A dump file alone cannot satisfy the backup gate."""
    backup = tmp_path / 'backup.dump'
    backup.write_bytes(b'PGDMP example')
    digest = hashlib.sha256(backup.read_bytes()).hexdigest()
    proof = tmp_path / 'restore.json'
    target = Target('staging-db', 'alerta_stage', 'scoped')
    with pytest.raises(ValueError, match='backup not verified'):
        verify_backup(backup, digest, proof, target)
    proof.write_text(
        json.dumps(
            {
                'backup_sha256': digest,
                'source_host': target.host,
                'source_database': target.database,
                'source_role': target.role,
                'source_environment': 'staging',
                'restore_test_succeeded': True,
                'restored_database': 'alerta_restore_test',
            }
        )
    )
    verify_backup(backup, digest, proof, target)
    with pytest.raises(ValueError, match='backup not verified'):
        verify_backup(backup, '0' * 64, proof, target)


def test_missing_execution_gate_does_not_probe_or_upload(
    tmp_path: Path,
) -> None:
    """An omitted operator execution flag stops before any side effects."""
    with patch(
        'infodengue_pipeline.production.analysis_e2e.verify_target'
    ) as probe:
        with pytest.raises(ValueError, match='execution gate missing'):
            run(
                settings(),
                tmp_path / 'fixture.csv',
                Target('staging-db', 'alerta_stage', 'scoped'),
                tmp_path / 'backup',
                '0' * 64,
                tmp_path / 'proof',
                execute=False,
            )
    probe.assert_not_called()


def test_freshness_failure_preserves_object_store(tmp_path: Path) -> None:
    """A previously ingested fixture never reaches object upload."""
    fixture = tmp_path / 'fixture.csv'
    fixture.write_text('synthetic')
    with (
        patch('infodengue_pipeline.production.analysis_e2e.verify_target'),
        patch('infodengue_pipeline.production.analysis_e2e.verify_backup'),
        patch(
            'infodengue_pipeline.production.analysis_e2e.build_source'
        ) as source,
        patch(
            'infodengue_pipeline.production.analysis_e2e.writer_client'
        ) as writer,
    ):
        source.return_value.exists_sha256.return_value = True
        with pytest.raises(ValueError, match='SHA already ingested'):
            run(
                settings(),
                fixture,
                Target('staging-db', 'alerta_stage', 'scoped'),
                tmp_path / 'backup',
                '0' * 64,
                tmp_path / 'proof',
                execute=True,
            )
        writer.assert_not_called()


def test_success_reuses_existing_ingestion_validator(tmp_path: Path) -> None:
    """One upload flows through the existing E2E correlation path."""
    fixture = tmp_path / 'fixture.csv'
    fixture.write_text('synthetic')
    outcome = Mock(attempts=1)
    with (
        patch('infodengue_pipeline.production.analysis_e2e.verify_target'),
        patch('infodengue_pipeline.production.analysis_e2e.verify_backup'),
        patch(
            'infodengue_pipeline.production.analysis_e2e.build_source'
        ) as source,
        patch('infodengue_pipeline.production.analysis_e2e.writer_client'),
        patch('infodengue_pipeline.production.analysis_e2e.require_absent'),
        patch('infodengue_pipeline.production.analysis_e2e.submit') as upload,
        patch(
            'infodengue_pipeline.production.analysis_e2e.validate_artifacts'
        ),
        patch(
            'infodengue_pipeline.production.analysis_e2e.validate_e2e'
        ) as validate,
    ):
        source.return_value.exists_sha256.return_value = False
        validate.return_value = outcome
        result = run(
            settings(),
            fixture,
            Target('staging-db', 'alerta_stage', 'scoped'),
            tmp_path / 'backup',
            '0' * 64,
            tmp_path / 'proof',
            execute=True,
        )
        assert result is outcome
        upload.assert_called_once()
        assert validate.call_args.kwargs == {
            'environment': 'prod',
            'load': True,
        }


def test_duplicate_attempts_fail(tmp_path: Path) -> None:
    """The command rejects any scientific rerun."""
    fixture = tmp_path / 'fixture.csv'
    fixture.write_text('synthetic')
    with (
        patch('infodengue_pipeline.production.analysis_e2e.verify_target'),
        patch('infodengue_pipeline.production.analysis_e2e.verify_backup'),
        patch(
            'infodengue_pipeline.production.analysis_e2e.build_source'
        ) as source,
        patch('infodengue_pipeline.production.analysis_e2e.writer_client'),
        patch('infodengue_pipeline.production.analysis_e2e.require_absent'),
        patch('infodengue_pipeline.production.analysis_e2e.submit'),
        patch(
            'infodengue_pipeline.production.analysis_e2e.validate_e2e'
        ) as validate,
    ):
        source.return_value.exists_sha256.return_value = False
        validate.return_value = Mock(attempts=2)
        with pytest.raises(RuntimeError, match='attempts=2'):
            run(
                settings(),
                fixture,
                Target('staging-db', 'alerta_stage', 'scoped'),
                tmp_path / 'backup',
                '0' * 64,
                tmp_path / 'proof',
                execute=True,
            )


def test_live_target_identity_is_verified() -> None:
    """The effective database identity must match the declared target."""
    target = Target('staging-db', 'alerta_stage', 'scoped')
    with patch(
        'infodengue_pipeline.production.analysis_e2e.create_engine'
    ) as create:
        connection = (
            create.return_value.connect.return_value.__enter__.return_value
        )
        connection.execute.return_value.one.return_value = (
            'alerta_stage',
            'scoped',
        )
        verify_target(settings(), target)
        connection.execute.return_value.one.return_value = ('wrong', 'scoped')
        with pytest.raises(ValueError, match='target is not staging'):
            verify_target(settings(), target)


def _artifact_result(tmp_path: Path) -> Mock:
    """Create minimal structural artifacts for acceptance checks."""
    week = '202412'
    state = tmp_path / 'alertas' / week / f'ale-MG-{week}.RData'
    national = tmp_path / 'alertas' / 'BR' / f'ale-BR-{week}.RData'
    for path in (state, national):
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, 'wb') as stream:
            stream.write(b'RDX3\nX\n')
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    for disease, table in (
        ('dengue', 'Historico_alerta'),
        ('chik', 'Historico_alerta_chik'),
    ):
        (sql_dir / f'output_{disease}.sql').write_text(
            f'INSERT INTO "Municipio"."{table}" VALUES (202412);'
        )
    maps = tmp_path / 'incidence_maps'
    maps.mkdir()
    (maps / 'map.png').write_bytes(
        b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR'
        + b'\x00\x00\x00\x01\x00\x00\x00\x01'
    )
    return Mock(
        output_dir=tmp_path, coverage=Coverage.MG, epiweek=Epiweek.parse(week)
    )


def test_structural_artifacts_and_exclusions(tmp_path: Path) -> None:
    """Dengue and chik are required; Zika may be absent."""
    result = _artifact_result(tmp_path)
    validate_artifacts(result)
    zika = tmp_path / 'sql' / 'output_zika.sql'
    zika.write_text(
        'INSERT INTO "Municipio"."Historico_alerta_zika" VALUES (202412);'
    )
    validate_artifacts(result)
    zika.write_text('DELETE FROM "Municipio"."Historico_alerta_zika";')
    with pytest.raises(RuntimeError, match='invalid zika SQL target'):
        validate_artifacts(result)


def test_artifact_failures_are_component_specific(tmp_path: Path) -> None:
    """Missing and malformed artifacts name their failed component."""
    result = _artifact_result(tmp_path)
    state = tmp_path / 'alertas' / '202412' / 'ale-MG-202412.RData'
    state.unlink()
    with pytest.raises(RuntimeError, match='RData missing'):
        validate_artifacts(result)
    state.write_bytes(b'bad')
    with pytest.raises(RuntimeError, match='invalid RData'):
        validate_artifacts(result)
    with gzip.open(state, 'wb') as stream:
        stream.write(b'RDX3\nX\n')
    dengue = tmp_path / 'sql' / 'output_dengue.sql'
    dengue.unlink()
    with pytest.raises(RuntimeError, match='dengue SQL missing'):
        validate_artifacts(result)
    dengue.write_text('INSERT INTO "Municipio"."wrong" VALUES (202412);')
    with pytest.raises(RuntimeError, match='invalid dengue SQL target'):
        validate_artifacts(result)
    dengue.write_text(
        'INSERT INTO "Municipio"."Historico_alerta" VALUES (202412);'
    )
    image = tmp_path / 'incidence_maps' / 'map.png'
    image.unlink()
    with pytest.raises(RuntimeError, match='maps missing'):
        validate_artifacts(result)
    image.write_bytes(b'bad')
    with pytest.raises(RuntimeError, match='invalid map'):
        validate_artifacts(result)
