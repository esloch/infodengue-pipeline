"""Build-time pinning of upstream SQL execution to the review boundary."""

from pathlib import Path

import pytest

from scripts.patch_analysis_sql_callsite import CALL, REPLACEMENT, patch


def test_patch_pins_both_sql_apply_sites_to_absolute_wrapper(
    tmp_path: Path,
) -> None:
    """Dengue and chik SQL calls bypass PATH resolution entirely."""
    source = tmp_path / '.makim.yaml'
    source.write_text(f'run: |\n{CALL}{CALL}')

    patch(source)

    assert source.read_text() == f'run: |\n{REPLACEMENT}{REPLACEMENT}'


@pytest.mark.parametrize('calls', [0, 1, 3])
def test_patch_fails_closed_when_upstream_task_shape_changes(
    tmp_path: Path, calls: int
) -> None:
    """An upstream revision change cannot silently drop SQL review."""
    source = tmp_path / '.makim.yaml'
    source.write_text(CALL * calls)

    with pytest.raises(SystemExit, match='expected exactly two'):
        patch(source)
