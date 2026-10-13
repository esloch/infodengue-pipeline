"""SINAN diseases owned by the pipeline."""

from enum import StrEnum


class Disease(StrEnum):
    """Supported diseases keyed by their canonical CID-10 code."""

    DENGUE = 'A90'
    CHIKUNGUNYA = 'A92.0'
    ZIKA = 'A928'

    @property
    def canonical_cid10(self) -> str:
        """Return the canonical CID-10 code for this disease."""
        return self.value

    @property
    def storage_name(self) -> str:
        """Return the canonical storage directory."""
        return _DISEASE_METADATA[self]['storage_name']

    @property
    def filename_prefix(self) -> str:
        """Return the canonical SINAN filename prefix."""
        return _DISEASE_METADATA[self]['filename_prefix']


_DISEASE_METADATA: dict[Disease, dict[str, str]] = {
    Disease.DENGUE: {
        'storage_name': 'dengue',
        'filename_prefix': 'DenInfodengue',
    },
    Disease.CHIKUNGUNYA: {
        'storage_name': 'chik',
        'filename_prefix': 'ChikInfodengue',
    },
    # The legacy mover uses this prefix for diseases outside dengue and chik.
    Disease.ZIKA: {
        'storage_name': 'zika',
        'filename_prefix': 'Infodengue',
    },
}


def disease_from_cid10(code: str) -> Disease:
    """Resolve a canonical CID-10 code or raise a clear error."""
    try:
        return Disease(code)
    except ValueError as exc:
        supported = ', '.join(disease.value for disease in Disease)
        raise ValueError(
            f'unsupported disease CID-10 code {code!r}; expected one of '
            f'{supported}'
        ) from exc
