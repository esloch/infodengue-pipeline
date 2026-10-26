"""Finite operator entry point for the staging-only production profile."""

import argparse

from pathlib import Path

from infodengue_pipeline.config import get_settings
from infodengue_pipeline.production.analysis_e2e import Target, run


def main() -> int:
    """Run one gated fixture and print only non-secret identifiers."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--expected-host', required=True)
    parser.add_argument('--expected-database', required=True)
    parser.add_argument('--expected-role', required=True)
    parser.add_argument('--backup', type=Path, required=True)
    parser.add_argument('--backup-sha256', required=True)
    parser.add_argument('--restore-proof', type=Path, required=True)
    parser.add_argument('--timeout-seconds', type=float, default=7200)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    result = run(
        get_settings(),
        args.fixture,
        Target(args.expected_host, args.expected_database, args.expected_role),
        args.backup,
        args.backup_sha256,
        args.restore_proof,
        execute=args.execute,
        timeout=args.timeout_seconds,
    )
    print('production analysis E2E passed')
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
        print(f'{name}={getattr(value, "value", value)}')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        message = str(exc)
        if message.startswith('production analysis E2E failed:'):
            raise SystemExit(message) from None
        raise SystemExit(
            f'production analysis E2E failed: {type(exc).__name__}'
        ) from None
