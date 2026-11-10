"""Readiness contract for the manually run Recife bulletin."""

import json

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from infodengue_pipeline.bulletin import runner
from infodengue_pipeline.bulletin.readiness import CheckBulletinReadiness
from infodengue_pipeline.db.models import (
    AnalysisJobRow,
    WorkflowInputRow,
    WorkflowRow,
)
from infodengue_pipeline.models import (
    AnalysisJobStatus,
    Coverage,
    Disease,
    Epiweek,
    IngestionInput,
    Readiness,
    WorkflowStatus,
)
from sqlalchemy.orm import Session, sessionmaker

WEEK = Epiweek.parse(202630)


def notification(**changes: object) -> dict[str, object]:
    """Return one complete synthetic Recife notification."""
    row: dict[str, object] = {
        'municipio_geocodigo': 2611606,
        'nu_notific': '000123-A',
        'cid10_codigo': 'A90',
        'ano_notif': '2026',
        'se_notif': '30',
        'se_sin_pri': '30',
        'dt_notific': '2026-07-29',
        'dt_sin_pri': '2026-07-28',
        'dt_digita': '2026-07-30',
        'id_bairro': '001',
        'nm_bairro': 'Centro',
    }
    row.update(changes)
    return row


def history(**changes: object) -> dict[str, object]:
    """Return one complete synthetic Recife history row."""
    row: dict[str, object] = {
        'SE': 202630,
        'data_iniSE': '2026-07-26',
        'casos': 3,
        'p_inc100k': 1.2,
        'nivel': 1,
        'Rt': None,
        'casos_est': None,
        'receptivo': None,
        'tempmin': 24,
        'pop': 1653461,
    }
    row.update(changes)
    return row


def valid_outputs(root: Path, *, load: bool = True) -> None:
    """Write the minimum nonempty artifacts checked by the analysis runner."""
    (root / 'alertas' / str(WEEK)).mkdir(parents=True)
    (root / 'alertas' / 'BR').mkdir(parents=True)
    (root / 'sql').mkdir()
    (root / 'alertas' / str(WEEK) / f'ale-PE-{WEEK}.RData').write_bytes(
        b'state RData'
    )
    (root / 'alertas' / 'BR' / f'ale-BR-{WEEK}.RData').write_bytes(b'BR RData')
    (root / 'sql' / 'output_dengue.sql').write_text('SELECT 1;')
    (root / 'sql' / 'output_chik.sql').write_text('SELECT 1;')
    if load:
        maps = root / 'incidence_maps'
        maps.mkdir()
        (maps / 'map.png').write_bytes(b'map')


def seed_current_job(
    sessions: sessionmaker[Session],
    output_dir: Path,
    *,
    workflow_status: WorkflowStatus = WorkflowStatus.SUCCEEDED,
    queued_fingerprint: str | None = None,
    job_status: AnalysisJobStatus = AnalysisJobStatus.SUCCEEDED,
    load: bool = True,
    exit_code: int | None = 0,
    finished: bool = True,
) -> tuple[UUID, str]:
    """Persist one PE workflow and its current fingerprint's analysis job."""
    workflow_id = uuid4()
    input_row = IngestionInput(
        uuid4(),
        'dengue-source-sha',
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )
    fingerprint = Readiness(
        Coverage.PE, WEEK, {Disease.DENGUE: input_row}
    ).input_fingerprint
    assert fingerprint is not None
    job_id = uuid4()
    with sessions.begin() as session:
        workflow = WorkflowRow(
            workflow_id=workflow_id,
            coverage=Coverage.PE,
            epiweek=int(WEEK),
            status=workflow_status,
            queued_fingerprint=(
                queued_fingerprint
                if queued_fingerprint is not None
                else fingerprint
            ),
        )
        workflow.inputs.append(
            WorkflowInputRow(
                disease=Disease.DENGUE,
                run_id=input_row.run_id,
                fingerprint=input_row.fingerprint,
                completed_at=input_row.completed_at,
            )
        )
        session.add(workflow)
        session.add(
            AnalysisJobRow(
                job_id=job_id,
                workflow_id=workflow_id,
                input_fingerprint=fingerprint,
                status=job_status,
                image='analysis:test',
                cores=1,
                load=load,
                output_dir=str(output_dir),
                attempts=1,
                exit_code=exit_code,
                finished_at=(datetime.now(timezone.utc) if finished else None),
            )
        )
    return job_id, fingerprint


def set_valid_source_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide current-week rows through the runner's shared validator."""
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: ([notification()], [history()], []),
    )


def test_readiness_accepts_current_loaded_analysis_and_complete_inputs(
    sessions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Current loaded analysis, artifacts, DB rows and reference are enough."""
    output_dir = tmp_path / 'analysis'
    valid_outputs(output_dir)
    job_id, fingerprint = seed_current_job(sessions, output_dir)
    reference = tmp_path / 'recife-neighborhoods.rds'
    reference.write_bytes(b'synthetic neighborhood reference')
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    set_valid_source_data(monkeypatch)

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, reference, config)

    assert result.eligible
    assert result.reasons == ()
    assert result.analysis_job_id == job_id
    assert result.input_fingerprint == fingerprint


def test_readiness_rejects_missing_persisted_analysis_workflow(
    sessions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Available renderer inputs cannot replace persisted scientific state."""
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic neighborhood reference')
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    set_valid_source_data(monkeypatch)

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, reference, config)

    assert not result.eligible
    assert 'persisted PE analysis workflow is missing' in result.reasons


def test_readiness_rejects_missing_analysis_job_for_current_fingerprint(
    sessions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An old success cannot substitute for the workflow's current job."""
    output_dir = tmp_path / 'analysis'
    valid_outputs(output_dir)
    job_id, _ = seed_current_job(sessions, output_dir)
    with sessions.begin() as session:
        session.delete(session.get(AnalysisJobRow, job_id))
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic neighborhood reference')
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    set_valid_source_data(monkeypatch)

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, reference, config)

    assert not result.eligible
    assert 'current PE analysis job is missing' in result.reasons


@pytest.mark.parametrize(
    'job_options, reason',
    [
        (
            {'load': False},
            'without applying database history',
        ),
        (
            {'job_status': AnalysisJobStatus.FAILED, 'exit_code': 1},
            'has not successfully finished',
        ),
        (
            {'workflow_status': WorkflowStatus.READY},
            'workflow has not succeeded',
        ),
        (
            {'queued_fingerprint': 'stale-fingerprint'},
            'workflow fingerprint is stale',
        ),
    ],
)
def test_readiness_rejects_noncurrent_or_unapplied_analysis(
    sessions,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_options: dict[str, object],
    reason: str,
) -> None:
    """Generated SQL alone and stale or failed jobs cannot authorize render."""
    output_dir = tmp_path / 'analysis'
    valid_outputs(output_dir, load=job_options.get('load', True))
    seed_current_job(sessions, output_dir, **job_options)
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic neighborhood reference')
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    set_valid_source_data(monkeypatch)

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, reference, config)

    assert not result.eligible
    assert any(reason in item for item in result.reasons)


def test_readiness_rejects_missing_scientific_artifacts(
    sessions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Success is insufficient when its current artifacts have vanished."""
    output_dir = tmp_path / 'analysis'
    valid_outputs(output_dir)
    (output_dir / 'sql' / 'output_dengue.sql').unlink()
    seed_current_job(sessions, output_dir)
    reference = tmp_path / 'reference.rds'
    reference.write_bytes(b'synthetic neighborhood reference')
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    set_valid_source_data(monkeypatch)

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, reference, config)

    assert not result.eligible
    assert any('artifacts are incomplete' in item for item in result.reasons)


def test_readiness_rejects_missing_reference_and_renderer_week(
    sessions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both the neighborhood artifact and current database week are needed."""
    output_dir = tmp_path / 'analysis'
    valid_outputs(output_dir)
    seed_current_job(sessions, output_dir)
    config = tmp_path / 'recife.json'
    config.write_text(json.dumps({'municipio_geocodigo': 2611606}))
    monkeypatch.setattr(
        runner,
        'export_from_database',
        lambda _url, _week: (
            [notification(se_notif='29')],
            [history()],
            [],
        ),
    )

    result = CheckBulletinReadiness(
        sessions, 'postgresql://read-only.test/dengue'
    ).check(WEEK, tmp_path / 'missing-neighborhoods.rds', config)

    assert not result.eligible
    assert any('neighborhood sf reference' in item for item in result.reasons)
    assert any('reference week' in item for item in result.reasons)
