"""Read-only correlation of one approved production-profile rehearsal."""

import argparse

from pathlib import Path

from infodengue_pipeline.bulletin.readiness import CheckBulletinReadiness
from infodengue_pipeline.config import get_settings
from infodengue_pipeline.db.engine import session_factory
from infodengue_pipeline.db.models import BulletinJobRow
from infodengue_pipeline.models import (
    BulletinJobStatus,
    BulletinKind,
    Epiweek,
)
from infodengue_pipeline.staging.e2e import (
    validate_rehearsal_e2e,
    wait_for,
)
from sqlalchemy import select


def main() -> None:
    """Wait for a persisted bulletin result tied to the selected input."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--object-key', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--week', required=True, type=int)
    parser.add_argument('--timeout-seconds', type=float, default=7200)
    args = parser.parse_args()
    settings = get_settings()
    week = Epiweek.parse(args.week)
    result = validate_rehearsal_e2e(
        args.object_key, args.sha256, args.timeout_seconds, settings
    )
    if result.epiweek != week:
        raise RuntimeError('selected week differs from the ingested PE week')
    if result.coverage.value != 'PE':
        raise RuntimeError('selected ingestion did not produce PE coverage')
    source = settings.alerta_source_database_url
    if source is None:
        raise RuntimeError('source database is not configured')
    sessions = session_factory()
    readiness = CheckBulletinReadiness(
        sessions, source.get_secret_value()
    ).check(week, settings.bulletin_reference, settings.bulletin_config)
    if not readiness.eligible or readiness.analysis_job_id != result.job_id:
        reasons = '; '.join(readiness.reasons) or 'analysis job mismatch'
        raise RuntimeError(f'Recife bulletin readiness blocked: {reasons}')

    def terminal_job() -> BulletinJobRow | None:
        with sessions() as session:
            rows = session.scalars(
                select(BulletinJobRow).where(
                    BulletinJobRow.analysis_job_id == result.job_id,
                    BulletinJobRow.kind
                    == BulletinKind.RECIFE_INTRAMUNICIPAL,
                    BulletinJobRow.municipality_geocode == 2611606,
                    BulletinJobRow.renderer_revision
                    == settings.bulletin_renderer_revision,
                )
            ).all()
            if len(rows) > 1:
                raise RuntimeError('multiple logical bulletin jobs found')
            if not rows or rows[0].status not in {
                BulletinJobStatus.SUCCEEDED,
                BulletinJobStatus.FAILED,
            }:
                return None
            return rows[0]

    job = wait_for(
        'persisted bulletin result', args.timeout_seconds, terminal_job
    )
    if job.status is not BulletinJobStatus.SUCCEEDED:
        raise RuntimeError(f'bulletin job failed: {job.last_error}')
    if (
        job.dispatched_at is None
        or job.started_at is None
        or job.finished_at is None
        or job.attempts != 1
        or job.exit_code != 0
        or not job.artifact_path
    ):
        raise RuntimeError('bulletin job persistence is incomplete')
    artifact = Path(job.artifact_path)
    if not artifact.is_file() or artifact.stat().st_size == 0:
        raise RuntimeError('persisted artifact is missing or empty')
    with artifact.open('rb') as pdf:
        signature = pdf.read(5)
    if signature != b'%PDF-':
        raise RuntimeError('persisted artifact is not a valid nonempty PDF')
    print('PRODUCTION-PROFILE BULLETIN E2E: PASS')
    print(f'week={week}')
    print(f'sha256={result.sha256}')
    print(f'scientific_job_id={result.job_id}')
    print(f'bulletin_job_id={job.job_id}')
    print(f'artifact_path={artifact}')


if __name__ == '__main__':
    try:
        main()
    except (TimeoutError, RuntimeError, ValueError, OSError) as exc:
        raise SystemExit(
            f'PRODUCTION-PROFILE BULLETIN E2E: BLOCKED: {exc}'
        ) from None
