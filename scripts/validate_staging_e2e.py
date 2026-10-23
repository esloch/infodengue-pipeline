"""CLI for diagnostic validation of one staging object."""

import argparse

from infodengue_pipeline.staging.e2e import validate_e2e


def main() -> int:
    """Validate the explicitly selected object."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--object-key', required=True)
    parser.add_argument('--timeout-seconds', type=float, default=7200)
    args = parser.parse_args()
    result = validate_e2e(args.object_key, args.timeout_seconds)
    print(
        f'Staging E2E succeeded for object key {result.object_key}', flush=True
    )
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (TimeoutError, RuntimeError, ValueError) as exc:
        raise SystemExit(f'STAGING E2E: FAIL: {exc}') from exc
