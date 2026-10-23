"""Inspect production Compose and run read-only probes in existing workers."""

import json
import subprocess
import sys

from collections.abc import Mapping
from pathlib import Path
from typing import Any

_COMPOSE = (
    'docker',
    'compose',
    '--env-file',
    '.env.prod',
    '-f',
    'containers/compose-prod.yaml',
)
_WORKERS = (
    'worker-ingestion',
    'worker-events',
    'worker-orchestration',
    'worker-analysis',
    'worker-beat',
)


def command(*args: str) -> str:
    """Return Compose output while suppressing secret-bearing diagnostics."""
    try:
        result = subprocess.run(
            (*_COMPOSE, *args), capture_output=True, text=True, check=False
        )
    except OSError as exc:
        raise RuntimeError('Docker Compose: unavailable') from exc
    if result.returncode:
        if args[0] == 'exec':
            for line in result.stdout.splitlines():
                if line.startswith('production validation failed: '):
                    raise RuntimeError(line)
            raise RuntimeError(f'{args[2]}: validation failed')
        raise RuntimeError(
            f'Compose {args[0]} failed (exit {result.returncode})'
        )
    return result.stdout


def validate_config(config: Mapping[str, Any]) -> str:
    """Check worker topology and production settings before any live probes."""
    services = config['services']
    for name in _WORKERS:
        if name not in services:
            raise RuntimeError(f'{name}: missing Compose service')
        environment = services[name]['environment']
        if environment.get('INFODENGUE_PIPELINE_ENVIRONMENT') != 'prod':
            raise RuntimeError(f'{name}: environment must be prod')
        if environment.get('INFODENGUE_PIPELINE_ANALYSIS_LOAD') != 'false':
            raise RuntimeError(f'{name}: analysis_load must be false')
    for name in ('worker-ingestion', 'worker-analysis'):
        if '--concurrency=1' not in services[name]['command']:
            raise RuntimeError(f'{name}: configured concurrency must be 1')
    image = services['worker-analysis']['image']
    if '@sha256:' not in image:
        raise RuntimeError('analysis image: immutable digest required')
    for name in _WORKERS:
        configured = services[name]['environment'].get(
            'INFODENGUE_PIPELINE_ANALYSIS_IMAGE'
        )
        if configured != image:
            raise RuntimeError(f'{name}: analysis image identity mismatch')
    return image


def validate_running(output: str) -> None:
    """Require every expected worker to be running and healthy."""
    try:
        rows = json.loads(output)
    except json.JSONDecodeError:
        rows = [json.loads(line) for line in output.splitlines()]
    if isinstance(rows, dict):
        rows = [rows]
    by_service = {row['Service']: row for row in rows}
    for name in _WORKERS:
        row = by_service.get(name)
        if row is None or row.get('State') != 'running':
            raise RuntimeError(f'{name}: not running')
        if row.get('Health') != 'healthy':
            raise RuntimeError(f'{name}: not healthy')
    migration = by_service.get('migrate')
    if migration is None or migration.get('ExitCode') != 0:
        raise RuntimeError('migrate: not completed successfully')


def main() -> None:
    """Validate the deployed profile without starting or repairing services."""
    if not Path('.env.prod').is_file():
        raise RuntimeError('.env.prod: private production profile is missing')
    config = json.loads(command('config', '--format', 'json'))
    image = validate_config(config)
    validate_running(command('ps', '--all', '--format', 'json'))
    print('worker services: running and healthy')
    print('ingestion and analysis concurrency: 1')
    print(f'analysis image: {image}')
    print('analysis_load: false')
    for service, role in (
        ('worker-ingestion', 'ingestion'),
        ('worker-analysis', 'analysis'),
    ):
        output = command(
            'exec',
            '-T',
            service,
            'python',
            '-m',
            'infodengue_pipeline.production.validate',
            role,
        )
        print(output, end='')
    print('production validation passed')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, RuntimeError) as exc:
        print(f'production validation failed: {exc}', file=sys.stderr)
        raise SystemExit(1) from None
