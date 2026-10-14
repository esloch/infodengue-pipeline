"""Canonical SINAN file path construction."""

from pathlib import Path

from infodengue_pipeline.models import SinanMetadata


class FilesystemCanonicalStorage:
    """Build paths under a configured nonproduction canonical root."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def base_destination(self, metadata: SinanMetadata) -> Path:
        """Return the deterministic base path before collision resolution."""
        coverage = metadata.coverage.value.lower()
        file_type = metadata.file_format.value
        disease = metadata.disease
        epiweek = metadata.epiweek
        filename = (
            f'{disease.filename_prefix}_{metadata.coverage.value}_'
            f'{epiweek}.{file_type}'
        )
        destination = (
            self._root
            / coverage
            / file_type
            / disease.storage_name
            / str(epiweek.year)
            / str(epiweek)
            / filename
        ).resolve()
        if not destination.is_relative_to(self._root):
            raise ValueError('canonical destination escapes configured root')
        return destination
