"""Module with CLI functions."""

import click

from infodengue_pipeline import __version__


@click.command()
@click.option(
    '--version',
    is_flag=True,
    help='Show the version of the installed Infodengue Pipeline tool.',
)
def app(version: bool) -> None:
    """Run the application."""
    if version:
        return click.echo(__version__)
    click.echo('Infodengue Pipeline is experimental.')


if __name__ == '__main__':
    app()
