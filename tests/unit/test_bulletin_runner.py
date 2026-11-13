"""Checks for the manual Recife bulletin's source-data boundary."""

import csv
import json
import subprocess

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from infodengue_pipeline.bulletin import runner


def test_separate_source_and_history_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Notifications stay on source; both histories use the loaded target."""
    source_engine = MagicMock()
    history_engine = MagicMock()
    source = source_engine.connect.return_value.__enter__.return_value
    target = history_engine.connect.return_value
    source.execute.return_value.mappings.return_value = [notification()]
    target.execute.side_effect = [
        MagicMock(mappings=lambda: [history()]),
        MagicMock(mappings=lambda: []),
    ]
    monkeypatch.setattr(
        runner,
        'create_engine',
        lambda url, **_kwargs: (
            source_engine if url == 'source' else history_engine
        ),
    )
    runner.load_validated_source_data('source', 202630, 'target')
    assert source.execute.call_args.args[0] is runner.NOTIFICATION_SQL
    params = source.execute.call_args.args[1]
    assert params['digitization_start'].isoformat() == '2018-01-01'
    assert params['digitization_end_exclusive'].isoformat() == '2026-08-02'
    assert 'READ ONLY' in source.exec_driver_sql.call_args.args[0]
    assert 'READ ONLY' in target.exec_driver_sql.call_args.args[0]
    assert [call.args[0] for call in target.execute.call_args_list] == [
        runner.HISTORY_SQL,
        runner.CHIK_HISTORY_SQL,
    ]


def test_notification_sql_accepts_integer_week_columns() -> None:
    """Regex checks cast integer-backed staging columns to text."""
    sql = str(runner.NOTIFICATION_SQL)
    assert "ano_notif::text ~ '^[0-9]{4}$'" in sql
    assert "se_notif::text ~ '^[0-9]{1,2}$'" in sql
    assert 'dt_digita < :digitization_end_exclusive' in sql
    assert 'dt_digita >= :digitization_start' in sql
    assert 'DISTINCT' not in sql
    assert "'A90', 'A92', 'A920'" in sql


def notification(**changes: object) -> dict[str, object]:
    """Return one complete synthetic source row."""
    row: dict[str, object] = {
        'municipio_geocodigo': 2611606,
        'nu_notific': '000123-A',
        'cid10_codigo': 'A90',
        'ano_notif': '2026',
        'se_notif': '30',
        'se_sin_pri': '30',
        'dt_notific': '2026-07-29',
        'dt_sin_pri': '2026-07-28',
        'dt_digita': '2026-07-30',
        'id_bairro': '001',
        'nm_bairro': 'Centro',
    }
    row.update(changes)
    return row


def history(**changes: object) -> dict[str, object]:
    """Return one complete synthetic alert row."""
    row: dict[str, object] = {
        'SE': 202630,
        'data_iniSE': '2026-07-26',
        'casos': 3,
        'p_inc100k': 1.2,
        'nivel': 1,
        'Rt': None,
        'casos_est': None,
        'receptivo': None,
        'tempmin': 24,
        'pop': 1653461,
    }
    row.update(changes)
    return row


def test_notifications_preserve_identifiers_and_original_week(
    tmp_path: Path,
) -> None:
    """Text identifiers and source dates survive CSV export unchanged."""
    rows = [notification()]
    runner.validate_notifications(rows, 202630)
    path = tmp_path / 'input.csv'
    runner._csv(path, rows, runner.NOTIFICATION_COLUMNS)
    with path.open(newline='') as stream:
        exported = next(iter(csv.DictReader(stream)))
    assert exported['nu_notific'] == '000123-A'
    assert exported['ano_notif'] == '2026'
    assert exported['se_notif'] == '30'
    assert exported['dt_notific'] == '2026-07-29'


def test_repeated_identifiers_preserve_every_source_row(
    tmp_path: Path,
) -> None:
    """A source identifier can represent distinct clinical and place rows."""
    rows = [
        notification(se_notif='31', dt_digita='2026-08-08'),
        notification(
            se_notif='30',
            se_sin_pri='29',
            dt_sin_pri='2026-07-20',
            id_bairro='002',
            nm_bairro='Bongi',
            dt_digita='2026-08-07',
        ),
    ]
    runner.validate_notifications(rows, 202631)
    path = tmp_path / 'notifications.csv'
    runner._csv(path, rows, runner.NOTIFICATION_COLUMNS)
    with path.open(newline='') as stream:
        exported = list(csv.DictReader(stream))
    assert len(exported) == 2
    assert [row['nu_notific'] for row in exported] == ['000123-A'] * 2
    assert [row['id_bairro'] for row in exported] == ['001', '002']


def test_digitization_boundary_and_notification_week_semantics() -> None:
    """A week 31 notification can have a week 30 symptom onset."""
    assert (
        runner.digitization_end_exclusive(202631).isoformat() == '2026-08-09'
    )
    runner.validate_notifications(
        [notification(se_notif='31', se_sin_pri='30', dt_digita='2026-08-08')],
        202631,
    )
    with pytest.raises(ValueError, match='digitization after'):
        runner.validate_notifications(
            [notification(se_notif='31', dt_digita='2026-08-09')], 202631
        )
    runner.validate_notifications(
        [notification(se_notif='31', dt_digita='2018-01-01')], 202631
    )
    with pytest.raises(ValueError, match='before historical window'):
        runner.validate_notifications(
            [notification(se_notif='31', dt_digita='2017-12-31')], 202631
        )
    with pytest.raises(ValueError, match='notification after'):
        runner.validate_notifications(
            [notification(se_notif='32', dt_digita='2026-08-08')], 202631
        )


def test_cdc_week_boundary() -> None:
    """The cutoff handles a CDC week that ends in the next calendar year."""
    from epiweeks import Week

    end = runner.digitization_end_exclusive(202652)
    assert end == Week(2026, 52, system='cdc').enddate() + timedelta(days=1)
    assert end.isoformat() == '2027-01-03'


@pytest.mark.parametrize(
    'rows, message',
    [
        ([], 'no Recife notifications'),
        ([notification(se_notif='54')], 'invalid epidemiological week'),
        ([notification(dt_notific=None)], 'notification date missing'),
        ([notification(nu_notific=None)], 'identifier missing'),
        ([notification(municipio_geocodigo=1)], 'outside Recife'),
        ([notification(dt_digita=None)], 'digitization date missing'),
        ([notification(dt_digita='not-a-date')], 'digitization date invalid'),
        ([notification(cid10_codigo='A928')], 'unsupported'),
    ],
)
def test_notifications_reject_incomplete_inputs(
    rows: list[dict[str, object]], message: str
) -> None:
    """The renderer never receives incomplete synthetic notifications."""
    with pytest.raises(ValueError, match=message):
        runner.validate_notifications(rows, 202630)


def test_history_requires_reference_week_and_population() -> None:
    """The municipal denominator and the chosen week must be observed."""
    assert runner.validate_history([history()], 202630) == 1653461
    with pytest.raises(ValueError, match='lacks reference week'):
        runner.validate_history([history(SE=202629)], 202630)
    with pytest.raises(ValueError, match='population'):
        runner.validate_history([history(pop=None)], 202630)


def test_run_writes_isolated_manifest_and_omits_unobserved_chik(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated attempts get separate outputs without fake chik history."""
    config = tmp_path / 'config.json'
    config.write_text('{"geocode":2611606}')
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic reference')
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: ([notification()], [history()], []),
    )

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        qmd = Path(command[7])
        qmd.write_text('---\ntitle: Test\n---\n')
        (qmd.parent / 'package-version.txt').write_text('0.1.1')
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, 'run', fake_run)
    first = runner.run(
        week=202630,
        reference=reference,
        output_root=tmp_path / 'output',
        config=config,
        database_url='unused',
        render=False,
    )
    second = runner.run(
        week=202630,
        reference=reference,
        output_root=tmp_path / 'output',
        config=config,
        database_url='unused',
        render=False,
    )
    assert first != second
    manifest = json.loads(first.read_text())
    assert manifest['status'] == 'succeeded'
    assert manifest['package_version'] == '0.1.1'
    assert 'history_chikungunya' not in manifest['outputs']
    assert manifest['input_hashes']['notifications']


@pytest.mark.parametrize('code', ['A92', 'A92.0', 'A920'])
def test_run_rejects_missing_chik_history_before_r(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: str
) -> None:
    """Observed chik notifications require observed chik alert history."""
    config = tmp_path / 'config.json'
    config.write_text('{"geocode":2611606}')
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic reference')
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: (
            [notification(cid10_codigo=code)],
            [history()],
            [],
        ),
    )
    with pytest.raises(ValueError, match='no Recife dengue alert history'):
        runner.run(
            week=202630,
            reference=reference,
            output_root=tmp_path / 'output',
            config=config,
            database_url='unused',
            render=False,
        )
    manifest = next((tmp_path / 'output').glob('*/manifest.json'))
    assert json.loads(manifest.read_text())['status'] == 'failed'


def test_run_exports_observed_chik_and_records_rendered_pdf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real chik source creates its own history and a PDF manifest entry."""
    config = tmp_path / 'config.json'
    config.write_text('{"geocode":2611606}')
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic reference')
    notifications = [
        notification(dt_sin_pri=None),
        notification(
            cid10_codigo='A92.0', nu_notific='000456-B', nm_bairro=None
        ),
    ]
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: (notifications, [history()], [history(casos=1)]),
    )

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if command[0] == 'Rscript':
            qmd = Path(command[7])
            qmd.write_text('---\ntitle: Test\n---\n')
            (qmd.parent / 'package-version.txt').write_text('0.1.1')
        else:
            Path(command[2]).with_suffix('.pdf').write_bytes(b'%PDF-test')
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, 'run', fake_run)
    manifest_path = runner.run(
        week=202630,
        reference=reference,
        output_root=tmp_path / 'output',
        config=config,
        database_url='unused',
    )
    manifest = json.loads(manifest_path.read_text())
    assert manifest['status'] == 'succeeded'
    assert manifest['row_counts']['history_chikungunya'] == 1
    assert manifest['notification_coverage']['missing_onset_dates'] == 1
    assert manifest['notification_coverage']['missing_neighborhood_names'] == 1
    assert Path(manifest['outputs']['history_chikungunya']).is_file()
    assert Path(manifest['outputs']['pdf']).read_bytes() == b'%PDF-test'


def test_generator_failure_records_diagnostic_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed R command leaves a failed manifest and named log."""
    config = tmp_path / 'config.json'
    config.write_text('{"geocode":2611606}')
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic reference')
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: ([notification()], [history()], []),
    )

    def fail(_command: list[str], **_kwargs: object) -> None:
        raise subprocess.CalledProcessError(1, 'Rscript')

    monkeypatch.setattr(subprocess, 'run', fail)
    with pytest.raises(subprocess.CalledProcessError):
        runner.run(
            week=202630,
            reference=reference,
            output_root=tmp_path / 'output',
            config=config,
            database_url='unused',
        )
    manifest_path = next((tmp_path / 'output').glob('*/manifest.json'))
    manifest = json.loads(manifest_path.read_text())
    assert manifest['status'] == 'failed'
    assert manifest['error_type'] == 'CalledProcessError'
    assert Path(manifest['outputs']['generate_log']).is_file()


def test_onset_and_neighborhood_coverage_keeps_source_rows() -> None:
    """Source gaps are counted without applying the scientific case filter."""
    rows = [
        notification(se_notif='31', dt_sin_pri=None, id_bairro=None),
        notification(
            se_notif='31',
            dt_sin_pri='2026-08-01',
            dt_notific='2026-07-29',
            nm_bairro=None,
        ),
        notification(se_notif='31', dt_sin_pri='bad-date'),
    ]
    runner.validate_notifications(rows, 202631)
    runner.validate_notifications([notification(cid10_codigo='A92')], 202630)
    assert runner.notification_coverage(rows) == {
        'missing_onset_dates': 1,
        'invalid_onset_dates': 2,
        'missing_neighborhood_ids': 1,
        'missing_neighborhood_names': 1,
        'distinct_neighborhood_ids': 1,
    }
