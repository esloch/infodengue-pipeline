"""Domain validation and readiness behavior."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.db.models import WorkflowRow
from infodengue_pipeline.events import IngestionCompleted
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    Coverage,
    Disease,
    Epiweek,
    FileFormat,
    IngestionInput,
    Readiness,
    SinanMetadata,
)
from pydantic import ValidationError

COMPLETED_AT = datetime(2026, 10, 14, tzinfo=timezone.utc)


def ingestion_input(fingerprint: str, *, seconds: int = 0) -> IngestionInput:
    """Create ordered ingestion inputs for readiness assertions."""
    return IngestionInput(
        uuid4(), fingerprint, COMPLETED_AT + timedelta(seconds=seconds)
    )


def test_workflow_identity_uses_unique_index_only() -> None:
    """The unique coverage/week constraint supplies the lookup index."""
    table = WorkflowRow.__table__
    assert {constraint.name for constraint in table.constraints} >= {
        'uq_workflow_coverage_epiweek'
    }
    assert {index.name for index in table.indexes}.isdisjoint(
        {'ix_pipeline_workflow_coverage_epiweek'}
    )


def test_disease_mapping() -> None:
    """CID-10 codes map to canonical names and prefixes."""
    assert Disease.DENGUE.value == 'A90'
    assert Disease.DENGUE.canonical_cid10 == 'A90'
    assert Disease.DENGUE.storage_name == 'dengue'
    assert Disease.DENGUE.filename_prefix == 'DenInfodengue'
    assert Disease.CHIKUNGUNYA.value == 'A92.0'
    assert Disease.CHIKUNGUNYA.canonical_cid10 == 'A92.0'
    assert Disease.CHIKUNGUNYA.storage_name == 'chik'
    assert Disease.CHIKUNGUNYA.filename_prefix == 'ChikInfodengue'
    assert Disease.ZIKA.value == 'A928'
    assert Disease.ZIKA.canonical_cid10 == 'A928'
    assert Disease.ZIKA.storage_name == 'zika'
    assert Disease.ZIKA.filename_prefix == 'Infodengue'


def test_required_disease_configuration(monkeypatch) -> None:
    """Required diseases parse canonical codes and reject invalid sets."""
    monkeypatch.setenv(
        'INFODENGUE_PIPELINE_BR_REQUIRED_DISEASES', 'A90,A92.0,A928'
    )
    from_env = Settings(_env_file=None)
    assert from_env.br_required_diseases == frozenset(Disease)
    monkeypatch.delenv('INFODENGUE_PIPELINE_BR_REQUIRED_DISEASES')
    defaults = Settings(_env_file=None)
    assert defaults.br_required_diseases == frozenset(
        {Disease.DENGUE, Disease.CHIKUNGUNYA}
    )
    settings = Settings(
        _env_file=None,
        br_required_diseases='A90,A92.0,A928',
    )
    assert settings.br_required_diseases == frozenset(Disease)
    with pytest.raises(ValidationError, match='unsupported disease'):
        Settings(_env_file=None, br_required_diseases='A90,A92')
    with pytest.raises(ValidationError, match='duplicate codes'):
        Settings(_env_file=None, br_required_diseases='A90,A90')
    with pytest.raises(ValidationError, match='must not be empty'):
        Settings(_env_file=None, br_required_diseases='')


def test_default_database_url_matches_development_compose(monkeypatch) -> None:
    """The default database credentials match the local Compose service."""
    monkeypatch.delenv('INFODENGUE_PIPELINE_DATABASE_URL', raising=False)
    settings = Settings(_env_file=None)
    assert settings.database_url.get_secret_value() == (
        'postgresql+psycopg://pipeline_dev:pipeline_dev_only@localhost:5434/'
        'pipeline_dev'
    )


def test_default_rabbitmq_url_matches_development_compose(monkeypatch) -> None:
    """The default broker credentials match the local Compose service."""
    monkeypatch.delenv('INFODENGUE_PIPELINE_RABBITMQ_URL', raising=False)
    settings = Settings(_env_file=None)
    assert settings.rabbitmq_url.get_secret_value() == (
        'amqp://pipeline_dev:pipeline_dev_only@localhost:5673//'
    )


@pytest.mark.parametrize('value', [202600, 202654, '20261', '2026AA'])
def test_invalid_epiweek(value: int | str) -> None:
    """Malformed and impossible weeks cannot enter domain state."""
    with pytest.raises(ValueError):
        Epiweek.parse(value)


def test_epiweek_round_trip() -> None:
    """Week serialization remains stable across integer and text forms."""
    week = Epiweek.parse(202639)
    assert str(week) == '202639'
    assert int(week) == 202639


def test_event_round_trip(completed_event) -> None:
    """The versioned event survives JSON serialization."""
    event = completed_event()
    restored = IngestionCompleted.model_validate_json(event.model_dump_json())
    assert restored == event
    assert restored.event_type == 'sinan.ingestion.completed'
    assert restored.version == 1
    assert restored.coverage is Coverage.BR
    assert restored.completed_at.utcoffset() is not None


def test_zika_event_round_trip(completed_event) -> None:
    """The existing v1 contract serializes Zika as its canonical code."""
    event = completed_event(Disease.ZIKA)
    restored = IngestionCompleted.model_validate_json(event.model_dump_json())
    assert restored == event
    assert restored.disease is Disease.ZIKA
    assert restored.model_dump(mode='json')['disease'] == 'A928'


def test_event_rejects_naive_time(completed_event) -> None:
    """Completion timestamps must include a time zone."""
    payload = completed_event().model_dump()
    payload['completed_at'] = datetime(2026, 1, 1)
    with pytest.raises(ValidationError):
        IngestionCompleted.model_validate(payload)


def test_event_factory_has_stable_identity_and_explicit_time() -> None:
    """A run maps to one event ID, independent of other payload fields."""
    run_id = uuid4()
    kwargs = dict(
        run_id=run_id,
        disease=Disease.DENGUE,
        coverage=Coverage.BR,
        epiweek=Epiweek.parse(202639),
        fingerprint='source',
        completed_at=COMPLETED_AT,
    )
    first = IngestionCompleted.new(**kwargs)
    assert first == IngestionCompleted.new(**kwargs)
    assert first.completed_at == COMPLETED_AT
    assert (
        first.event_id
        != IngestionCompleted.new(**{**kwargs, 'run_id': uuid4()}).event_id
    )
    with pytest.raises(TypeError):
        IngestionCompleted.new(
            **{
                key: value
                for key, value in kwargs.items()
                if key != 'completed_at'
            }
        )
    with pytest.raises(ValidationError):
        IngestionCompleted.new(
            **{**kwargs, 'completed_at': datetime(2026, 10, 14)}
        )


@pytest.mark.parametrize('fingerprint', ['', '   ', 'x' * 129])
def test_event_rejects_invalid_fingerprint(fingerprint: str) -> None:
    """The event identity fits the persisted fingerprint capacity."""
    with pytest.raises(ValidationError):
        IngestionCompleted.new(
            run_id=uuid4(),
            disease=Disease.DENGUE,
            coverage=Coverage.BR,
            epiweek=Epiweek.parse(202639),
            fingerprint=fingerprint,
            completed_at=COMPLETED_AT,
        )


def test_input_order_is_monotonic_and_equal_times_converge() -> None:
    """Late delivery and duplicates preserve the newest disease input."""
    week = Epiweek.parse(202639)
    older = ingestion_input('older')
    newer = ingestion_input('newer', seconds=1)
    first = Readiness(Coverage.BR, week).with_input(Disease.DENGUE, older)
    assert first.inputs[Disease.DENGUE] == older
    latest = first.with_input(Disease.DENGUE, newer)
    assert latest.inputs[Disease.DENGUE] == newer
    assert latest.with_input(Disease.DENGUE, older) is latest
    assert latest.with_input(Disease.DENGUE, newer) is latest
    tied = IngestionInput(uuid4(), 'tie', newer.completed_at)
    forward = latest.with_input(Disease.DENGUE, tied)
    reverse = (
        Readiness(Coverage.BR, week)
        .with_input(Disease.DENGUE, tied)
        .with_input(Disease.DENGUE, newer)
    )
    assert forward.inputs == reverse.inputs
    assert forward.inputs[Disease.DENGUE].run_id.int == max(
        newer.run_id.int, tied.run_id.int
    )
    with pytest.raises(ValueError, match='timezone aware'):
        IngestionInput(uuid4(), 'bad', datetime(2026, 10, 14))


def test_readiness_fingerprint_changes_with_inputs() -> None:
    """The identity includes every available input and its disease code."""
    week = Epiweek.parse(202639)
    dengue = ingestion_input('d')
    chik = ingestion_input('c')
    waiting = Readiness(Coverage.BR, week).with_input(Disease.DENGUE, dengue)
    assert not waiting.ready
    assert waiting.input_fingerprint is None
    ready = waiting.with_input(Disease.CHIKUNGUNYA, chik)
    assert ready.ready
    assert ready.input_fingerprint is not None
    repeated = (
        Readiness(Coverage.BR, week)
        .with_input(Disease.DENGUE, dengue)
        .with_input(Disease.CHIKUNGUNYA, chik)
    )
    assert ready.input_fingerprint == repeated.input_fingerprint
    changed = ready.with_input(Disease.DENGUE, ingestion_input('d', seconds=1))
    assert changed.input_fingerprint != ready.input_fingerprint


def test_readiness_fingerprint_is_deterministic_for_arrival_order() -> None:
    """Arrival order cannot alter a fingerprint for the same inputs."""
    week = Epiweek.parse(202639)
    inputs = {
        Disease.DENGUE: ingestion_input('d'),
        Disease.CHIKUNGUNYA: ingestion_input('c'),
        Disease.ZIKA: ingestion_input('z'),
    }
    forward = Readiness(Coverage.BR, week)
    reverse = Readiness(Coverage.BR, week)
    for disease in Disease:
        forward = forward.with_input(disease, inputs[disease])
    for disease in reversed(tuple(Disease)):
        reverse = reverse.with_input(disease, inputs[disease])
    assert forward.ready and reverse.ready
    assert forward.input_fingerprint == reverse.input_fingerprint


def test_optional_zika_changes_analysis_input_fingerprint() -> None:
    """Optional Zika participates when available to the analysis job."""
    week = Epiweek.parse(202639)
    readiness = Readiness(Coverage.BR, week).with_input(
        Disease.DENGUE, ingestion_input('d')
    )
    readiness = readiness.with_input(Disease.CHIKUNGUNYA, ingestion_input('c'))
    baseline = readiness.input_fingerprint
    with_zika = readiness.with_input(Disease.ZIKA, ingestion_input('z'))
    changed_zika = with_zika.with_input(
        Disease.ZIKA, ingestion_input('z2', seconds=1)
    )
    assert baseline is not None
    assert with_zika.input_fingerprint != baseline
    assert changed_zika.input_fingerprint != with_zika.input_fingerprint


def test_analysis_request_rejects_empty_fingerprint() -> None:
    """Future OCI execution requires a nonempty stable input identity."""
    with pytest.raises(ValueError):
        AnalysisJobRequest(uuid4(), Coverage.BR, Epiweek.parse(202639), '')


def test_analysis_scope_is_derived_from_coverage() -> None:
    """National and UF deliveries map to the corresponding analysis scope."""
    national = AnalysisJobRequest(
        uuid4(), Coverage.BR, Epiweek.parse(202639), 'hash'
    )
    state = AnalysisJobRequest(
        uuid4(), Coverage.MG, Epiweek.parse(202639), 'hash'
    )
    assert national.states == ('ALL',)
    assert state.states == ('MG',)


def test_coverage_validates_finite_delivery_scope() -> None:
    """Coverage accepts BR and the supported Brazilian state set only."""
    assert Coverage.BR.is_national
    assert not Coverage.MG.is_national
    with pytest.raises(ValueError):
        Coverage('XX')
    with pytest.raises(ValueError):
        Coverage('../MG')


def test_state_readiness_and_optional_chik_change_hash() -> None:
    """State dengue is ready; later optional chik changes analysis identity."""
    week = Epiweek.parse(202639)
    dengue = ingestion_input('d')
    ready = Readiness(
        Coverage.MG,
        week,
        {Disease.DENGUE: dengue},
        frozenset({Disease.DENGUE}),
    )
    assert ready.ready
    before = ready.input_fingerprint
    with_chik = ready.with_input(Disease.CHIKUNGUNYA, ingestion_input('c'))
    assert with_chik.input_fingerprint != before


def test_default_readiness_policy_depends_on_coverage() -> None:
    """Bare readiness uses dengue plus chik for BR and dengue for a UF."""
    week = Epiweek.parse(202639)
    assert Readiness(Coverage.BR, week).required_diseases == frozenset(
        {Disease.DENGUE, Disease.CHIKUNGUNYA}
    )
    assert Readiness(Coverage.MG, week).required_diseases == frozenset(
        {Disease.DENGUE}
    )


def test_fingerprint_includes_coverage() -> None:
    """Identical national and state deliveries have distinct hashes."""
    week = Epiweek.parse(202639)
    inputs = {Disease.DENGUE: ingestion_input('same')}
    national = Readiness(
        Coverage.BR, week, inputs, frozenset({Disease.DENGUE})
    )
    state = Readiness(Coverage.MG, week, inputs, frozenset({Disease.DENGUE}))
    assert national.input_fingerprint != state.input_fingerprint


def test_sinan_metadata_retains_filename_as_provenance() -> None:
    """Normalized SINAN facts coexist with an audit-only source name."""
    metadata = SinanMetadata(
        Disease.DENGUE,
        Coverage.MG,
        Epiweek.parse(202452),
        FileFormat.DBF,
        'datos_final.dbf',
    )
    assert metadata.source_name == 'datos_final.dbf'
    assert metadata.coverage is Coverage.MG
