"""Safe Phase 0 analysis execution boundary."""

from infodengue_pipeline.models import (
    AnalysisJobRequest,
    AnalysisJobResult,
)


class AnalysisDisabledError(RuntimeError):
    """Raised when scientific execution is unavailable in Phase 0."""


class DisabledAnalysisRunner:
    """Prevent accidental scientific jobs before the OCI adapter exists."""

    def run(self, request: AnalysisJobRequest) -> AnalysisJobResult:
        """Reject execution while preserving the runner contract."""
        raise AnalysisDisabledError(
            f'Analysis execution is disabled for job {request.job_id}'
        )
