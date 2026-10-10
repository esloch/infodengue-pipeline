"""Validated epidemiological week identity."""

from dataclasses import dataclass

from epiweeks import Week


@dataclass(frozen=True, order=True)
class Epiweek:
    """A CDC epidemiological week serialized as ``YYYYWW``."""

    year: int
    week: int

    def __post_init__(self) -> None:
        """Reject impossible weeks, including invalid year boundaries."""
        if not 1900 <= self.year <= 9999:
            raise ValueError('Epiweek year must be between 1900 and 9999')
        if not 1 <= self.week <= 53:
            raise ValueError('Epiweek number must be between 1 and 53')
        week = Week(self.year, self.week)
        if week.startdate().year > self.year + 1:
            raise ValueError('Invalid epidemiological week')
        if Week.fromdate(week.startdate()) != week:
            raise ValueError('Invalid epidemiological week for year')

    @classmethod
    def parse(cls, value: int | str) -> 'Epiweek':
        """Parse a six digit epiweek and validate its calendar meaning."""
        raw = str(value)
        if len(raw) != 6 or not raw.isascii() or not raw.isdigit():
            raise ValueError('Epiweek must use YYYYWW')
        return cls(int(raw[:4]), int(raw[4:]))

    def __int__(self) -> int:
        """Return the stable six digit integer representation."""
        return self.year * 100 + self.week

    def __str__(self) -> str:
        """Return the stable six digit string representation."""
        return f'{self.year:04d}{self.week:02d}'
