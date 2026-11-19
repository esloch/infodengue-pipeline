"""Read-only execution readiness for a Recife intramunicipal bulletin."""

import argparse
import hashlib

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from infodengue_pipeline.analysis.runner import validate_outputs
from infodengue_pipeline.bulletin.runner import (
    load_validated_source_data,
    validate_config,
    validate_reference,
)
from infodengue_pipeline.db.models import AnalysisJobRow
from infodengue_pipeline.db.repositories import SqlWorkflowRepository
from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobStatus,
    Coverage,
    Epiweek,
    WorkflowStatus,
)


@dataclass(frozen=True)
class BulletinReadinessResult:
    """Read-only eligibility decision and any reasons it is blocked."""

    eligible: bool
    reasons: tuple[str, ...] = ()
    analysis_job_id: UUID | None = None
    input_fingerprint: str | None = None


class CheckBulletinReadiness:
    """Check persisted analysis, artifacts, renderer data and reference."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        source_database_url: str | None,
        history_database_url: str | None = None,
        reference_sha256: str | None = None,
    ) -> None:
        self._sessions = sessions
        self._source_database_url = source_database_url
        self._history_database_url = history_database_url
        self._reference_sha256 = reference_sha256

    def check(
        self,
        week: Epiweek | int | str,
        reference: Path,
        config: Path = Path('configs/bulletin/recife.json'),
    ) -> BulletinReadinessResult:
        """Return whether all current inputs permit bulletin execution."""
        try:
            epiweek = (
                week if isinstance(week, Epiweek) else Epiweek.parse(week)
            )
        except ValueError:
            return BulletinReadinessResult(
                False, ('invalid epidemiological week',)
            )

        reasons: list[str] = []
        try:
            if self._reference_sha256 is not None:
                digest = hashlib.sha256(reference.read_bytes()).hexdigest()
                if digest != self._reference_sha256:
                    raise ValueError(
                        'approved Recife reference checksum differs'
                    )
            validate_reference(reference)
        except (OSError, ValueError) as exc:
            reasons.append(str(exc))
        try:
            validate_config(config)
        except (OSError, ValueError) as exc:
            reasons.append(f'invalid Recife bulletin configuration: {exc}')

        analysis_job_id: UUID | None = None
        input_fingerprint: str | None = None
        try:
            with self._sessions() as session:
                snapshot = SqlWorkflowRepository(session).get(
                    Coverage.PE, epiweek
                )
                if snapshot is None:
                    reasons.append('persisted PE analysis workflow is missing')
                else:
                    input_fingerprint = snapshot.readiness.input_fingerprint
                    if not snapshot.readiness.ready:
                        reasons.append(
                            'current PE analysis inputs are incomplete'
                        )
                    if (
                        input_fingerprint is None
                        or snapshot.queued_fingerprint != input_fingerprint
                    ):
                        reasons.append(
                            'PE analysis workflow fingerprint is stale'
                        )
                    if snapshot.status is not WorkflowStatus.SUCCEEDED:
                        reasons.append(
                            'current PE analysis workflow has not succeeded'
                        )

                    if input_fingerprint is not None:
                        job = session.scalar(
                            select(AnalysisJobRow).where(
                                AnalysisJobRow.workflow_id
                                == snapshot.workflow_id,
                                AnalysisJobRow.input_fingerprint
                                == input_fingerprint,
                            )
                        )
                    else:
                        job = None

                    if job is None:
                        reasons.append('current PE analysis job is missing')
                    else:
                        analysis_job_id = job.job_id
                        if (
                            job.status is not AnalysisJobStatus.SUCCEEDED
                            or job.exit_code != 0
                            or job.finished_at is None
                        ):
                            reasons.append(
                                'current PE analysis job has not '
                                'successfully finished'
                            )
                        if not job.load:
                            reasons.append(
                                'current analysis generated artifacts '
                                'without applying database history'
                            )
                        if (
                            job.status is AnalysisJobStatus.SUCCEEDED
                            and job.exit_code == 0
                            and job.finished_at is not None
                        ):
                            request = AnalysisJobRequest(
                                job_id=job.job_id,
                                coverage=Coverage.PE,
                                epiweek=epiweek,
                                input_fingerprint=job.input_fingerprint,
                                cores=job.cores,
                                load=job.load,
                                output_dir=Path(job.output_dir),
                                image=job.image,
                                workflow_id=snapshot.workflow_id,
                            )
                            try:
                                validate_outputs(Path(job.output_dir), request)
                            except (OSError, ValueError) as exc:
                                reasons.append(
                                    f'current analysis artifacts are '
                                    f'incomplete: {exc}'
                                )
        except Exception as exc:
            reasons.append(
                f'pipeline analysis state could not be checked '
                f'({type(exc).__name__})'
            )

        if self._source_database_url is None:
            reasons.append('alert source database URL is not configured')
        else:
            try:
                if self._history_database_url is None:
                    load_validated_source_data(
                        self._source_database_url, int(epiweek)
                    )
                else:
                    load_validated_source_data(
                        self._source_database_url,
                        int(epiweek),
                        self._history_database_url,
                    )
            except ValueError as exc:
                reasons.append(
                    f'current renderer database inputs are invalid ({exc})'
                )
            except Exception as exc:
                reasons.append(
                    'current renderer database inputs could not be checked '
                    f'({type(exc).__name__})'
                )

        return BulletinReadinessResult(
            eligible=not reasons,
            reasons=tuple(reasons),
            analysis_job_id=analysis_job_id,
            input_fingerprint=input_fingerprint,
        )


def main() -> None:
    """Run the existing read-only readiness check for one production week."""
    from infodengue_pipeline.bulletin.authorization import bulletin_authorized
    from infodengue_pipeline.config import get_settings
    from infodengue_pipeline.db.engine import session_factory

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--week', required=True, type=int)
    args = parser.parse_args()
    settings = get_settings()
    if not bulletin_authorized(settings):
        raise SystemExit('bulletin production authorization is not valid')
    source = settings.alerta_source_database_url
    history = settings.bulletin_history_database_url
    if source is None or history is None:
        raise SystemExit('bulletin source/history URLs are not configured')
    readiness = CheckBulletinReadiness(
        session_factory(),
        source.get_secret_value(),
        history.get_secret_value(),
        settings.bulletin_reference_sha256,
    ).check(
        args.week,
        settings.bulletin_reference,
        settings.bulletin_config,
    )
    if not readiness.eligible:
        for reason in readiness.reasons:
            print(f'BLOCKED: {reason}')
        raise SystemExit(1)
    print(f'RECIFE BULLETIN READINESS: PASS week={args.week}')
    print(f'analysis_job_id={readiness.analysis_job_id}')
    print(f'input_fingerprint={readiness.input_fingerprint}')


if __name__ == '__main__':
    main()
