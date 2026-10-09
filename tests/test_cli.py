"""Checks for the bootstrap command line entrypoint."""

from click.testing import CliRunner
from infodengue_pipeline.cli import app


def test_version() -> None:
    """The CLI exposes the package version."""
    result = CliRunner().invoke(app, ['--version'])
    assert result.exit_code == 0
    assert '0.1.0' in result.output
