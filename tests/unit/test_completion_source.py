"""AlertaDengue read-only PostgreSQL contract adapter tests."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from infodengue_pipeline.ingestion.completion import (
    PostgresCompletedIngestionSource,
    build_source,
)


class FakeResult:
    """Expose SQLAlchemy-like row mappings for adapter tests."""

    def __init__(self, row) -> None:
        self._row = row

    def mappings(self):
        """Return this fake result as a mapping result."""
        return self

    def first(self):
        """Return the first configured row."""
        return self._row


class FakeConnection:
    """Record source queries and return one configured database row."""

    def __init__(self, row) -> None:
        self.row = row
        self.statements = []

    def __enter__(self):
        """Begin the fake database connection context."""
        return self

    def __exit__(self, *_args) -> None:
        """Close the fake database connection context."""

    def execute(self, statement, parameters=None):
        """Record SQL and distinguish transaction setup from the query."""
        self.statements.append((str(statement), parameters))
        if 'SELECT' in str(statement):
            return FakeResult(self.row)
        return FakeResult(None)


class FakeEngine:
    """Provide one configured connection to the source adapter."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self) -> FakeConnection:
        """Return the configured source connection."""
        return self.connection


def test_source_maps_contract_fields_and_enforces_read_only() -> None:
    """Map a completed source run and begin a read-only transaction."""
    run_id = uuid4()
    finished_at = datetime(2026, 10, 14, tzinfo=timezone.utc)
    connection = FakeConnection(
        {
            'id': run_id,
            'status': 'completed',
            'uf': 'MG',
            'disease': 'A90',
            'canonical_cid10': 'A90',
            'delivery_se': 202639,
            'sha256': 'a' * 64,
            'finished_at': finished_at,
        }
    )
    run = PostgresCompletedIngestionSource(FakeEngine(connection)).by_sha256(
        'a' * 64
    )
    assert run is not None
    assert run.run_id == run_id
    assert run.uf == 'MG'
    assert run.disease == 'A90'
    assert run.canonical_cid10 == 'A90'
    assert run.delivery_se == 202639
    assert connection.statements[0][0] == 'SET TRANSACTION READ ONLY'
    query, values = connection.statements[1]
    assert 'WHERE sha256 = :sha256 AND status = :status' in query
    assert values == {'sha256': 'a' * 64, 'status': 'completed'}


def test_source_ignores_missing_run_and_missing_finish_time() -> None:
    """A result without a complete successful timestamp cannot be bridged."""
    source = PostgresCompletedIngestionSource(FakeEngine(FakeConnection(None)))
    assert source.by_sha256('not-found') is None
    row = {
        'id': uuid4(),
        'status': 'completed',
        'uf': 'SP',
        'disease': 'A90',
        'canonical_cid10': None,
        'delivery_se': 202639,
        'sha256': 'b' * 64,
        'finished_at': None,
    }
    source = PostgresCompletedIngestionSource(FakeEngine(FakeConnection(row)))
    assert source.by_sha256('b' * 64) is None


def test_source_normalizes_naive_timestamp_to_utc() -> None:
    """A naive PostgreSQL timestamp is interpreted as UTC."""
    row = {
        'id': uuid4(),
        'status': 'completed',
        'uf': 'SP',
        'disease': 'A90',
        'canonical_cid10': None,
        'delivery_se': 202639,
        'sha256': 'c' * 64,
        'finished_at': datetime(2026, 10, 14),
    }
    run = PostgresCompletedIngestionSource(
        FakeEngine(FakeConnection(row))
    ).by_sha256('c' * 64)
    assert run is not None
    assert run.finished_at.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    ('disease', 'canonical', 'expected'),
    [
        ('A90', 'A90', 'A90'),
        ('A92', 'A92.0', 'A92.0'),
        ('A92', 'A928', 'A928'),
        ('A90', None, 'A90'),
    ],
)
def test_resolve_run_disease(disease, canonical, expected) -> None:
    """Resolve each supported canonical identity and legacy dengue."""
    from infodengue_pipeline.ingestion.completion import resolve_run_disease

    assert resolve_run_disease(disease, canonical).value == expected


@pytest.mark.parametrize(
    ('disease', 'canonical', 'message'),
    [
        ('A92', None, 'ambiguous A92'),
        ('A90', 'A92.0', 'inconsistent'),
        ('A92', 'A90', 'inconsistent'),
        ('A92', 'UNKNOWN', 'unsupported disease CID-10'),
    ],
)
def test_resolve_run_disease_rejects_invalid_metadata(
    disease, canonical, message
) -> None:
    """Reject ambiguous, inconsistent, and unsupported source metadata."""
    import pytest

    from infodengue_pipeline.ingestion.completion import resolve_run_disease

    with pytest.raises(ValueError, match=message):
        resolve_run_disease(disease, canonical)


def test_build_source_creates_adapter_without_sibling_imports() -> None:
    """The concrete source adapter is built from a standalone URL."""
    source = build_source('postgresql+psycopg://readonly:secret@localhost/db')
    assert isinstance(source, PostgresCompletedIngestionSource)
    source._engine.dispose()
