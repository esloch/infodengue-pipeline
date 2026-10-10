"""Canonical SINAN file path construction."""

import re

from pathlib import Path

from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek


class FilesystemCanonicalStorage:
    """Build paths under a configured nonproduction canonical root."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def destination(
        self,
        *,
        country: str,
        file_type: str,
        disease: Disease,
        epiweek: Epiweek,
        filename: str,
    ) -> Path:
        """Validate and return a contained canonical destination."""
        if not re.fullmatch(r'[a-z]{2}', country):
            raise ValueError('country must be a lowercase ISO code')
        if file_type not in {'csv', 'dbf'}:
            raise ValueError('file_type must be csv or dbf')
        expected = (
            f'{disease.filename_prefix}_{country.upper()}_'
            f'{epiweek}.{file_type}'
        )
        if filename != expected:
            raise ValueError(f'filename must be {expected}')
        destination = (
            self._root
            / country
            / file_type
            / disease.storage_name
            / str(epiweek.year)
            / str(epiweek)
            / filename
        ).resolve()
        if not destination.is_relative_to(self._root):
            raise ValueError('canonical destination escapes configured root')
        return destination
