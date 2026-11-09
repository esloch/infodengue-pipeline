"""Checks for the bootstrap command line entrypoint."""

from click.testing import CliRunner
from infodengue_pipeline.cli import app
from infodengue_pipeline.config import Settings


def test_version() -> None:
    """The CLI exposes the package version."""
    result = CliRunner().invoke(app, ['--version'])
    assert result.exit_code == 0
    assert '0.1.0' in result.output


def test_historical_reconciliation_requires_explicit_confirmation(
    monkeypatch,
) -> None:
    """The operator command cannot run without the history-only gate."""
    settings = Settings(
        _env_file=None,
        environment='staging',
        alerta_source_database_url=(
            'postgresql+psycopg://readonly:secret@source/alerta'
        ),
    )
    monkeypatch.setattr(
        'infodengue_pipeline.cli.get_settings', lambda: settings
    )
    result = CliRunner().invoke(
        app,
        [
            'reconcile-historical',
            '--object-key',
            'approved.csv',
        ],
    )
    assert result.exit_code != 0
    assert '--confirm' in result.output
