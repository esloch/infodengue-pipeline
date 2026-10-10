"""SINAN diseases owned by the pipeline."""

from enum import StrEnum


class Disease(StrEnum):
    """Supported CID-10 disease identities."""

    DENGUE = 'A90'
    CHIKUNGUNYA = 'A92'

    @property
    def storage_name(self) -> str:
        """Return the canonical storage directory."""
        return 'dengue' if self is Disease.DENGUE else 'chik'

    @property
    def filename_prefix(self) -> str:
        """Return the canonical SINAN filename prefix."""
        return 'DenInfodengue' if self is Disease.DENGUE else 'ChikInfodengue'
