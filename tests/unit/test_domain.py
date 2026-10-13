"""Domain validation and readiness behavior."""

from datetime import datetime
from uuid import uuid4

import pytest

from infodengue_pipeline.config import Settings
from infodengue_pipeline.domain.disease import Disease
from infodengue_pipeline.domain.epiweek import Epiweek
from infodengue_pipeline.domain.events import IngestionCompleted
from infodengue_pipeline.domain.workflow import IngestionInput, Readiness
from infodengue_pipeline.ports.contracts import AnalysisJobRequest
from pydantic import ValidationError


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
        'INFODENGUE_PIPELINE_REQUIRED_DISEASES', 'A90,A92.0,A928'
    )
    from_env = Settings(_env_file=None)
    assert from_env.required_diseases == frozenset(Disease)
    monkeypatch.delenv('INFODENGUE_PIPELINE_REQUIRED_DISEASES')
    defaults = Settings(_env_file=None)
    assert defaults.required_diseases == frozenset(
        {Disease.DENGUE, Disease.CHIKUNGUNYA}
    )
    settings = Settings(
        _env_file=None,
        required_diseases='A90,A92.0,A928',
    )
    assert settings.required_diseases == frozenset(Disease)
    with pytest.raises(ValidationError, match='unsupported disease'):
        Settings(_env_file=None, required_diseases='A90,A92')
    with pytest.raises(ValidationError, match='duplicate codes'):
        Settings(_env_file=None, required_diseases='A90,A90')
    with pytest.raises(ValidationError, match='must not be empty'):
        Settings(_env_file=None, required_diseases='')


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


def test_readiness_fingerprint_changes_with_inputs() -> None:
    """The identity includes every available input and its disease code."""
    week = Epiweek.parse(202639)
    dengue = IngestionInput(uuid4(), 'd')
    chik = IngestionInput(uuid4(), 'c')
    waiting = Readiness(week).with_input(Disease.DENGUE, dengue)
    assert not waiting.ready
    assert waiting.input_fingerprint is None
    ready = waiting.with_input(Disease.CHIKUNGUNYA, chik)
    assert ready.ready
    assert ready.input_fingerprint is not None
    repeated = (
        Readiness(week)
        .with_input(Disease.DENGUE, dengue)
        .with_input(Disease.CHIKUNGUNYA, chik)
    )
    assert ready.input_fingerprint == repeated.input_fingerprint
    changed = ready.with_input(Disease.DENGUE, IngestionInput(uuid4(), 'd'))
    assert changed.input_fingerprint != ready.input_fingerprint


def test_readiness_fingerprint_is_deterministic_for_arrival_order() -> None:
    """Arrival order cannot alter a fingerprint for the same inputs."""
    week = Epiweek.parse(202639)
    inputs = {
        Disease.DENGUE: IngestionInput(uuid4(), 'd'),
        Disease.CHIKUNGUNYA: IngestionInput(uuid4(), 'c'),
        Disease.ZIKA: IngestionInput(uuid4(), 'z'),
    }
    forward = Readiness(week)
    reverse = Readiness(week)
    for disease in Disease:
        forward = forward.with_input(disease, inputs[disease])
    for disease in reversed(tuple(Disease)):
        reverse = reverse.with_input(disease, inputs[disease])
    assert forward.ready and reverse.ready
    assert forward.input_fingerprint == reverse.input_fingerprint


def test_optional_zika_changes_analysis_input_fingerprint() -> None:
    """Optional Zika participates when available to the analysis job."""
    week = Epiweek.parse(202639)
    readiness = Readiness(week).with_input(
        Disease.DENGUE, IngestionInput(uuid4(), 'd')
    )
    readiness = readiness.with_input(
        Disease.CHIKUNGUNYA, IngestionInput(uuid4(), 'c')
    )
    baseline = readiness.input_fingerprint
    with_zika = readiness.with_input(
        Disease.ZIKA, IngestionInput(uuid4(), 'z')
    )
    changed_zika = with_zika.with_input(
        Disease.ZIKA, IngestionInput(uuid4(), 'z2')
    )
    assert baseline is not None
    assert with_zika.input_fingerprint != baseline
    assert changed_zika.input_fingerprint != with_zika.input_fingerprint


def test_analysis_request_rejects_unsafe_parameters() -> None:
    """Future OCI execution can only accept bounded typed inputs."""
    with pytest.raises(ValueError):
        AnalysisJobRequest(uuid4(), Epiweek.parse(202639), 'hash', (';rm',), 1)
