"""CLI for guarded one-command staging acceptance."""

import argparse

from pathlib import Path

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.staging.runner import run


def main() -> int:
    """Submit one fresh fixture and summarize its validated outcome."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--timeout-seconds', type=float, default=7200)
    args = parser.parse_args()
    result = run(get_settings(), args.fixture, args.timeout_seconds)
    print('STAGING E2E: PASS', flush=True)
    for name in (
        'object_key',
        'sha256',
        'run_id',
        'coverage',
        'epiweek',
        'workflow_id',
        'job_id',
        'attempts',
    ):
        value = getattr(result, name)
        print(f'{name}={getattr(value, "value", value)}', flush=True)
    print('analysis_load=false', flush=True)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (TimeoutError, RuntimeError, ValueError, OSError) as exc:
        raise SystemExit(f'STAGING E2E: FAIL: {exc}') from exc
