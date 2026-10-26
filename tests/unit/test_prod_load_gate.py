"""Scientific worker staging-target gate checks."""

from unittest.mock import patch
from uuid import uuid4

import pytest

from infodengue_pipeline.analysis.runner import require_staging_load_target
from infodengue_pipeline.config import Settings
from infodengue_pipeline.models import AnalysisJobRequest, Coverage, Epiweek

URL = 'postgresql+psycopg://scoped:secret@staging-db:5432/alerta_stage'


def _settings(**changes: object) -> Settings:
    values: dict[str, object] = {
        'environment': 'prod',
        'analysis_load': True,
        'analysis_staging_target_host': 'staging-db',
        'analysis_staging_target_database': 'alerta_stage',
        'analysis_staging_target_role': 'scoped',
        'analysis_staging_backup_verified': True,
        'analysis_e2e_coverage': 'MG',
        'analysis_e2e_epiweek': '202412',
    }
    values.update(changes)
    return Settings.model_validate(values)


def _request(
    *, load: bool = True, coverage: Coverage = Coverage.MG
) -> AnalysisJobRequest:
    return AnalysisJobRequest(
        uuid4(), coverage, Epiweek.parse('202412'), 'fingerprint', load=load
    )


def test_load_gate_refuses_missing_backup_and_wrong_scope() -> None:
    """The worker fails before launching R for unsafe settings."""
    for settings, request in (
        (_settings(analysis_staging_backup_verified=False), _request()),
        (_settings(analysis_e2e_coverage='RJ'), _request()),
        (_settings(analysis_e2e_epiweek='202413'), _request()),
        (_settings(), _request(coverage=Coverage.RJ)),
    ):
        with pytest.raises(ValueError, match='staging load gate failed'):
            require_staging_load_target(settings, request, URL)


def test_load_gate_requires_configured_and_effective_target() -> None:
    """Matching URL text alone does not establish the effective role."""
    with pytest.raises(ValueError, match='staging load gate failed'):
        require_staging_load_target(
            _settings(), _request(), URL.replace('scoped', 'other')
        )
    with patch('infodengue_pipeline.analysis.runner.create_engine') as create:
        connection = (
            create.return_value.connect.return_value.__enter__.return_value
        )
        connection.execute.return_value.one.return_value = (
            'alerta_stage',
            'scoped',
        )
        require_staging_load_target(_settings(), _request(), URL)
        connection.execute.return_value.one.return_value = (
            'alerta_stage',
            'wrong',
        )
        with pytest.raises(ValueError, match='staging load gate failed'):
            require_staging_load_target(_settings(), _request(), URL)


def test_nonmutating_request_skips_load_gate() -> None:
    """Read-only jobs retain their existing behavior."""
    require_staging_load_target(_settings(), _request(load=False), URL)
