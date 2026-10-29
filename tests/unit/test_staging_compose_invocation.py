"""The staging E2E entry points join the deployed Compose project."""

import os
import shlex
import subprocess

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PREFIX = [
    'compose',
    '-p',
    'infodengue-pipeline-staging',
    '--env-file',
    '.env.staging',
    '-f',
    'containers/compose-staging.yaml',
]


def test_fixture_launcher_uses_staging_project_and_preserves_arguments(
    tmp_path: Path,
) -> None:
    """Capture Docker arguments without starting a container."""
    docker = tmp_path / 'docker'
    docker.write_text(
        '#!/bin/sh\n'
        'printf "%s\\n" "$@" > "$DOCKER_ARGS_FILE"\n'
        'printf "%s\\n" "$STAGING_E2E_FIXTURE_HOST_PATH" '
        '> "$DOCKER_FIXTURE_FILE"\n'
    )
    docker.chmod(0o755)
    fixture = tmp_path / 'fixture with spaces.DBF'
    fixture.write_bytes(b'fixture')
    arguments = tmp_path / 'arguments'
    exported_fixture = tmp_path / 'exported-fixture'
    env = os.environ.copy()
    env.update(
        PATH=f'{tmp_path}{os.pathsep}{env["PATH"]}',
        DOCKER_ARGS_FILE=str(arguments),
        DOCKER_FIXTURE_FILE=str(exported_fixture),
    )

    subprocess.run(
        ['bash', 'scripts/run_staging_e2e.sh', str(fixture), '123'],
        cwd=ROOT,
        env=env,
        check=True,
    )

    assert arguments.read_text().splitlines() == [
        *COMPOSE_PREFIX,
        'run',
        '--rm',
        '--no-deps',
        'e2e-runner',
        'python',
        'scripts/run_staging_e2e.py',
        '--fixture',
        '/tmp/staging-e2e-fixture.dbf',
        '--timeout-seconds',
        '123',
    ]
    assert exported_fixture.read_text().strip() == str(fixture.resolve())


def test_validator_task_uses_staging_project_and_preserves_arguments() -> None:
    """Check the Makim command with representative rendered arguments."""
    makim = yaml.safe_load((ROOT / '.makim.yaml').read_text())
    command = makim['groups']['staging']['tasks']['validate-e2e']['run']
    command = command.replace(
        '${{ args.object_key }}', 'approved-fixtures/test.dbf'
    ).replace('${{ args.timeout_seconds }}', '123')

    assert shlex.split(command) == [
        'docker',
        *COMPOSE_PREFIX,
        'run',
        '--rm',
        'validator',
        'python',
        'scripts/validate_staging_e2e.py',
        '--object-key',
        'approved-fixtures/test.dbf',
        '--timeout-seconds',
        '123',
    ]
