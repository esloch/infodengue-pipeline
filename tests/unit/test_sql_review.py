"""Pre-application SQL review of the pinned scientific writer contract."""

from pathlib import Path
from unittest.mock import patch

import pytest

from infodengue_pipeline.production.sql_review import (
    SqlReviewError,
    _validated_arguments,
    main,
    review_and_apply,
    review_sql,
)


def _argv(path: Path) -> list[str]:
    return [
        '-v',
        'ON_ERROR_STOP=1',
        '-h',
        'analysis-e2e-db',
        '-p',
        '5432',
        '-U',
        'analysis_e2e_writer',
        '-d',
        'analysis_e2e',
        '-f',
        str(path),
    ]


def _sql(
    table: str = 'Historico_alerta', constraint: str = 'alertas_unicos'
) -> str:
    return (
        f'INSERT INTO "Municipio"."{table}" '
        '("SE",municipio_geocodigo,municipio_nome) '
        "VALUES (202412,3106200,'Belo; Horizonte') "
        f'ON CONFLICT ON CONSTRAINT {constraint} '
        'DO UPDATE SET "SE"=excluded."SE",'
        'municipio_geocodigo=excluded.municipio_geocodigo;\n'
    )


def test_historical_upserts_review_before_application(tmp_path: Path) -> None:
    """Accepted SQL retains the upstream window and quoted city values."""
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'output_dengue.sql'
    path.write_text(_sql() + _sql().replace('202412', '202411'))
    assert review_sql(path, tmp_path) == 2


@pytest.mark.parametrize(
    'sql',
    [
        _sql('wrong'),
        _sql(constraint='wrong'),
        _sql().replace('202412', 'NULL'),
        _sql().replace('"SE"=excluded."SE"', '"SE"=0'),
        _sql() + 'DELETE FROM "Municipio"."Historico_alerta";',
        _sql().replace("'Belo; Horizonte'", "'unterminated"),
    ],
)
def test_rejects_unexpected_sql(tmp_path: Path, sql: str) -> None:
    """The review fails before invoking the database client."""
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'output_dengue.sql'
    path.write_text(sql)
    with pytest.raises(SqlReviewError):
        review_sql(path, tmp_path)


def test_rejects_missing_file_and_unexpected_path(tmp_path: Path) -> None:
    """The review accepts only the current output directory's SQL files."""
    missing = tmp_path / 'sql' / 'output_dengue.sql'
    with pytest.raises(SqlReviewError, match='missing'):
        review_sql(missing, tmp_path)
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'other.sql'
    path.write_text(_sql())
    with pytest.raises(SqlReviewError, match='path'):
        review_sql(path, tmp_path)


def test_psql_destination_is_fixed(tmp_path: Path) -> None:
    """Operator arguments cannot redirect SQL to another database."""
    path = tmp_path / 'output_dengue.sql'
    assert _validated_arguments(_argv(path))[-1] == path
    wrong = _argv(path)
    wrong[3] = 'production-db'
    with pytest.raises(SqlReviewError, match='isolated staging clone'):
        _validated_arguments(wrong)
    with pytest.raises(SqlReviewError, match='arguments'):
        _validated_arguments(['-f', str(path)])
    wrong_port = _argv(path)
    wrong_port[5] = '5433'
    with pytest.raises(SqlReviewError, match='arguments'):
        _validated_arguments(wrong_port)


def test_review_precedes_real_psql(tmp_path: Path, monkeypatch) -> None:
    """No SQL reaches psql until file and live clone marker are checked."""
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'output_dengue.sql'
    path.write_text(_sql())
    monkeypatch.setenv('INFODENGUE_PIPELINE_ENVIRONMENT', 'prod')
    monkeypatch.setenv('INFODENGUE_PIPELINE_ANALYSIS_LOAD', 'true')
    monkeypatch.setenv(
        'INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256', 'a' * 64
    )
    monkeypatch.setenv('ALERTA_OUT_DIR', str(tmp_path))
    monkeypatch.setenv('PGPASSWORD', 'test-only')
    with (
        patch(
            'infodengue_pipeline.production.sql_review.psycopg.connect'
        ) as connect,
        patch(
            'infodengue_pipeline.production.sql_review.subprocess.run'
        ) as apply,
    ):
        connection = connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [
            ('analysis_e2e', 'analysis_e2e_writer'),
            ('a' * 64,),
            (False, False, False, False, False),
            (False,),
            (True,),
            (True,),
            (False,),
        ]
        apply.return_value.returncode = 0
        assert review_and_apply(_argv(path)) == 0
        apply.assert_called_once()
        assert apply.call_args.args[0][0] == (
            '/opt/conda/envs/alertadengueanalise/bin/psql'
        )
        cursor.fetchone.side_effect = [
            ('analysis_e2e', 'analysis_e2e_writer'),
            ('wrong',),
        ]
        with pytest.raises(SqlReviewError, match='backup identity'):
            review_and_apply(_argv(path))
        apply.assert_called_once()


def test_refuses_role_membership_before_application(
    tmp_path: Path, monkeypatch
) -> None:
    """A limited role cannot inherit unreviewed database privileges."""
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'output_dengue.sql'
    path.write_text(_sql())
    monkeypatch.setenv('INFODENGUE_PIPELINE_ENVIRONMENT', 'prod')
    monkeypatch.setenv('INFODENGUE_PIPELINE_ANALYSIS_LOAD', 'true')
    monkeypatch.setenv(
        'INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256', 'a' * 64
    )
    monkeypatch.setenv('ALERTA_OUT_DIR', str(tmp_path))
    monkeypatch.setenv('PGPASSWORD', 'test-only')
    with (
        patch(
            'infodengue_pipeline.production.sql_review.psycopg.connect'
        ) as connect,
        patch(
            'infodengue_pipeline.production.sql_review.subprocess.run'
        ) as apply,
    ):
        cursor = (
            connect.return_value.__enter__.return_value.cursor.return_value
        )
        cursor.__enter__.return_value.fetchone.side_effect = [
            ('analysis_e2e', 'analysis_e2e_writer'),
            ('a' * 64,),
            (False, False, False, False, False),
            (True,),
        ]
        with pytest.raises(SqlReviewError, match='memberships'):
            review_and_apply(_argv(path))
        apply.assert_not_called()


def test_cli_reports_refusal(monkeypatch, capsys) -> None:
    """The psql shim returns a refusal status without exposing details."""
    monkeypatch.setattr(
        'infodengue_pipeline.production.sql_review.review_and_apply',
        lambda argv: (_ for _ in ()).throw(SqlReviewError('unsafe target')),
    )
    assert main() == 75
    assert 'unsafe target' in capsys.readouterr().err


@pytest.mark.parametrize(
    ('checks', 'message'),
    [
        ([(True, False, False, False, False)], 'elevated'),
        (
            [(False, False, False, False, False), (False,), (False,)],
            'table grants',
        ),
        (
            [
                (False, False, False, False, False),
                (False,),
                (True,),
                (False,),
            ],
            'other Municipio tables',
        ),
        (
            [
                (False, False, False, False, False),
                (False,),
                (True,),
                (True,),
                (True,),
            ],
            'owns target tables',
        ),
    ],
)
def test_refuses_broad_database_permissions(
    tmp_path: Path, monkeypatch, checks, message: str
) -> None:
    """The writer must have only the documented historical-table grants."""
    sql_dir = tmp_path / 'sql'
    sql_dir.mkdir()
    path = sql_dir / 'output_dengue.sql'
    path.write_text(_sql())
    monkeypatch.setenv('INFODENGUE_PIPELINE_ENVIRONMENT', 'prod')
    monkeypatch.setenv('INFODENGUE_PIPELINE_ANALYSIS_LOAD', 'true')
    monkeypatch.setenv(
        'INFODENGUE_PIPELINE_ANALYSIS_STAGING_BACKUP_SHA256', 'a' * 64
    )
    monkeypatch.setenv('ALERTA_OUT_DIR', str(tmp_path))
    monkeypatch.setenv('PGPASSWORD', 'test-only')
    with (
        patch(
            'infodengue_pipeline.production.sql_review.psycopg.connect'
        ) as connect,
        patch(
            'infodengue_pipeline.production.sql_review.subprocess.run'
        ) as apply,
    ):
        cursor = connect.return_value.__enter__().cursor.return_value
        cursor.__enter__.return_value.fetchone.side_effect = [
            ('analysis_e2e', 'analysis_e2e_writer'),
            ('a' * 64,),
            *checks,
        ]
        with pytest.raises(SqlReviewError, match=message):
            review_and_apply(_argv(path))
        apply.assert_not_called()
