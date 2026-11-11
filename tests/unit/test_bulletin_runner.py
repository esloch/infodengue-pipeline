"""Checks for the manual Recife bulletin's source-data boundary."""

import csv
import json
import subprocess

from pathlib import Path
from types import SimpleNamespace

import pytest

from infodengue_pipeline.bulletin import runner


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


@pytest.mark.parametrize(
    'rows, message',
    [
        ([], 'no Recife notifications'),
        ([notification(se_notif='54')], 'invalid epidemiological week'),
        ([notification(dt_notific=None)], 'notification date missing'),
        ([notification(), notification()], 'duplicate notification'),
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


def test_run_rejects_missing_chik_history_before_r(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
            [notification(cid10_codigo='A92.0')],
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
        notification(),
        notification(cid10_codigo='A92.0', nu_notific='000456-B'),
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
    assert Path(manifest['outputs']['history_chikungunya']).is_file()
    assert Path(manifest['outputs']['pdf']).read_bytes() == b'%PDF-test'
