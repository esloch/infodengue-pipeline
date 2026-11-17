"""Confirm published SQL without re-running scientific analysis."""

import pytest

from sqlalchemy import create_engine, text

from infodengue_pipeline.config import Settings
from infodengue_pipeline.db.jobs import AnalysisJobService
from infodengue_pipeline.db.models import AnalysisJobRow
from infodengue_pipeline.db.repositories import SqlWorkflowRepository
from infodengue_pipeline.models import (
    AnalysisJobResult,
    Coverage,
    Disease,
    Epiweek,
)
from infodengue_pipeline.production import confirm_publication as publication
from infodengue_pipeline.services import CoordinateEpiweek


class SuccessfulRunner:
    """Simulate a successful, read-only scientific execution."""

    def run(self, request):
        """Return a finite scientific job success."""
        return AnalysisJobResult(request.job_id, request.output_dir, True)


def build_job(sessions, work_factory, completed_event, tmp_path):
    """Persist one BR read-only success and create required artifacts."""
    week = Epiweek.parse(202645)
    coordinator = CoordinateEpiweek(work_factory, initial_analysis_week=week)
    coordinator.record(completed_event(Disease.DENGUE, week=202645))
    coordinator.record(completed_event(Disease.CHIKUNGUNYA, week=202645))
    service = AnalysisJobService(
        sessions,
        worker_image='analysis:test',
        cores=8,
        load=False,
        output_root=tmp_path,
        initial_analysis_week=week,
    )
    job_id = service.claim_ready(Coverage.BR, week)
    assert job_id is not None
    assert service.execute(job_id, SuccessfulRunner())

    root = tmp_path / str(job_id) / 'attempt-3'
    with sessions.begin() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        job.output_dir = str(root)

    for index in range(27):
        state = f'{index:02d}'
        path = root / 'alertas' / '202645' / f'ale-{state}-202645.RData'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'state')
    br = root / 'alertas' / 'BR' / 'ale-BR-202645.RData'
    br.parent.mkdir(parents=True)
    br.write_bytes(b'national')

    sql = root / 'sql'
    sql.mkdir()
    (sql / 'output_dengue.sql').write_bytes(b'dengue')
    (sql / 'output_chik.sql').write_bytes(b'chik')

    for disease in ('dengue', 'chikungunya'):
        path = (
            root
            / 'incidence_maps'
            / 'country'
            / (f'incidence_Nacional_{disease}.png')
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'country')
    state_dir = root / 'incidence_maps' / 'state'
    state_dir.mkdir()
    for index in range(54):
        (state_dir / f'{index:02d}.png').write_bytes(b'state')

    settings = Settings(
        _env_file=None,
        environment='prod',
        analysis_initial_epiweek=202645,
        analysis_output_root=tmp_path,
    )
    return settings, job_id, root


def test_confirm_advances_cursor_once(
    sessions, work_factory, completed_event, tmp_path, monkeypatch
):
    """A published load=false job becomes a contiguous persisted week."""
    settings, job_id, _root = build_job(
        sessions, work_factory, completed_event, tmp_path
    )
    monkeypatch.setattr(
        publication, 'validate_persisted_week', lambda *_: None
    )
    week = Epiweek.parse(202645)
    assert publication.confirm(sessions, settings, job_id, week, object())
    assert not publication.confirm(sessions, settings, job_id, week, object())
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        assert job.load is False
        assert job.published_at is not None
        assert job.published_dengue_sha256 is not None
        assert job.published_chik_sha256 is not None
        assert (
            SqlWorkflowRepository(session).last_persisted_analysis_week(
                Coverage.BR, week
            )
            == week
        )


def test_missing_map_rejects_confirmation(
    sessions, work_factory, completed_event, tmp_path, monkeypatch
):
    """Missing publication artifacts must not advance the cursor."""
    settings, job_id, root = build_job(
        sessions, work_factory, completed_event, tmp_path
    )
    monkeypatch.setattr(
        publication, 'validate_persisted_week', lambda *_: None
    )
    missing = (
        root / 'incidence_maps' / 'country' / 'incidence_Nacional_dengue.png'
    )
    missing.unlink()
    with pytest.raises(ValueError, match='missing national map'):
        publication.confirm(
            sessions, settings, job_id, Epiweek.parse(202645), object()
        )
    with sessions() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None and job.published_at is None


def test_publication_requires_correct_week(
    sessions, work_factory, completed_event, tmp_path, monkeypatch
):
    """Do not allow a successful job to publish another epiweek."""
    settings, job_id, _root = build_job(
        sessions, work_factory, completed_event, tmp_path
    )
    monkeypatch.setattr(
        publication, 'validate_persisted_week', lambda *_: None
    )
    with pytest.raises(ValueError, match='requested epiweek'):
        publication.confirm(
            sessions, settings, job_id, Epiweek.parse(202644), object()
        )


def test_confirm_requires_current_input_fingerprint(
    sessions, work_factory, completed_event, tmp_path, monkeypatch
):
    """A superseded ingestion fingerprint cannot publish."""
    settings, job_id, _root = build_job(
        sessions, work_factory, completed_event, tmp_path
    )
    monkeypatch.setattr(
        publication, 'validate_persisted_week', lambda *_: None
    )
    with sessions.begin() as session:
        job = session.get(AnalysisJobRow, job_id)
        assert job is not None
        job.input_fingerprint = 'stale'
    with pytest.raises(ValueError, match='fingerprint'):
        publication.confirm(
            sessions, settings, job_id, Epiweek.parse(202645), object()
        )


def test_published_sql_must_not_change(
    sessions, work_factory, completed_event, tmp_path, monkeypatch
):
    """A repeated confirmation refuses artifact drift."""
    settings, job_id, root = build_job(
        sessions, work_factory, completed_event, tmp_path
    )
    monkeypatch.setattr(
        publication, 'validate_persisted_week', lambda *_: None
    )
    assert publication.confirm(
        sessions, settings, job_id, Epiweek.parse(202645), object()
    )
    (root / 'sql' / 'output_dengue.sql').write_bytes(b'drift')
    with pytest.raises(ValueError, match='hashes changed'):
        publication.confirm(
            sessions, settings, job_id, Epiweek.parse(202645), object()
        )


def test_history_validation_checks_both_diseases() -> None:
    """Require the full national municipality set in both histories."""
    engine = create_engine('sqlite+pysqlite:///:memory:')
    try:
        with engine.begin() as connection:
            connection.execute(
                text('ATTACH DATABASE :name AS "Municipio"'),
                {'name': ':memory:'},
            )
            for table in ('Historico_alerta', 'Historico_alerta_chik'):
                connection.execute(
                    text(
                        f'CREATE TABLE "Municipio"."{table}" '
                        '("SE" integer, municipio_geocodigo integer)'
                    )
                )
                connection.execute(
                    text(
                        f'INSERT INTO "Municipio"."{table}" '
                        '("SE", municipio_geocodigo) '
                        'VALUES (:week, :municipality)'
                    ),
                    [
                        {'week': 202645, 'municipality': code}
                        for code in range(5570)
                    ],
                )

        publication.validate_persisted_week(
            engine, Epiweek.parse(202645)
        )

        with engine.begin() as connection:
            connection.execute(
                text(
                    'DELETE FROM "Municipio"."Historico_alerta_chik" '
                    'WHERE municipio_geocodigo = 0'
                )
            )
        with pytest.raises(ValueError, match='Historico_alerta_chik'):
            publication.validate_persisted_week(
                engine, Epiweek.parse(202645)
            )
    finally:
        engine.dispose()


def test_artifacts_must_be_inside_configured_output_root(
    tmp_path,
) -> None:
    """Reject a missing or unrelated job output directory."""
    with pytest.raises(FileNotFoundError):
        publication.artifact_hashes(
            tmp_path / 'missing',
            tmp_path,
            Epiweek.parse(202645),
        )
