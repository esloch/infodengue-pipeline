#!/usr/bin/env python3
"""Pin the upstream SQL apply commands to the review wrapper."""

from pathlib import Path

CALL = '            psql \\\n'
REPLACEMENT = '            /usr/local/bin/psql \\\n'


def patch(path: Path) -> None:
    """Replace exactly the two pinned SQL application commands."""
    source = path.read_text(encoding='utf-8')
    count = source.count(CALL)
    if count != 2:
        raise SystemExit(
            f'expected exactly two pinned psql call sites, found {count}'
        )
    path.write_text(source.replace(CALL, REPLACEMENT), encoding='utf-8')


if __name__ == '__main__':
    patch(Path('/opt/alertadengueanalise/.makim.yaml'))
