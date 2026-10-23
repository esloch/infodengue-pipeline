"""Static contract for the production SINAN watcher user service."""

from pathlib import Path

UNIT = Path('deploy/systemd/infodengue-sinan-watcher.service')
ENV_EXAMPLE = Path('deploy/systemd/sinan-watcher.env.example')


def test_foreground_watcher_and_safe_defaults() -> None:
    """Systemd owns the foreground watcher without recovery flags."""
    unit = UNIT.read_text()
    assert 'ExecStart=/usr/bin/env makim ingestion.watch' in unit
    assert '--path ${SINAN_WATCH_PATH}' in unit
    assert '--settle-time ${SINAN_WATCH_SETTLE_TIME}' in unit
    for forbidden in (
        'watch-start',
        'watch-stop',
        'watch-list',
        '--requeue',
        '--allow-outdated',
        '--include-existing',
    ):
        assert forbidden not in unit


def test_systemd_lifecycle_and_logging() -> None:
    """The service has one standard lifecycle and journal output."""
    unit = UNIT.read_text()
    for required in (
        'Type=simple',
        'Restart=on-failure',
        'RestartSec=5s',
        'KillSignal=SIGTERM',
        'KillMode=control-group',
        'TimeoutStopSec=30s',
        'StandardOutput=journal',
        'StandardError=journal',
        'WantedBy=default.target',
    ):
        assert required in unit
    for forbidden in (
        'PIDFile=',
        'pgrep',
        'pkill',
        'nohup',
        'bash -c',
        'sh -c',
        '>>',
        '2>',
    ):
        assert forbidden not in unit


def test_host_environment_boundary() -> None:
    """Required host configuration points at the existing AlertaDengue tree."""
    unit = UNIT.read_text()
    example = ENV_EXAMPLE.read_text()
    assert 'WorkingDirectory=/opt/services/infodengue/AlertaDengue' in unit
    assert 'EnvironmentFile=%h/.config/infodengue/sinan-watcher.env' in unit
    assert 'ExecStartPre=/usr/bin/test -d ${SINAN_WATCH_PATH}' in unit
    assert 'SINAN_WATCH_PATH=' in example
    assert 'SINAN_WATCH_SETTLE_TIME=5.0' in example
    assert 'PATH=' in example
    for forbidden in (
        'PASSWORD',
        'SECRET',
        'DATABASE_URL',
        'MINIO_ACCESS_KEY',
    ):
        assert forbidden not in example


def test_operator_documentation() -> None:
    """Production docs cover service operations and legacy migration."""
    docs = Path('docs/production.md').read_text()
    for required in (
        'systemctl --user',
        'journalctl --user',
        'infodengue-sinan-watcher.service',
        '/opt/data/staging/sinan/incoming',
    ):
        assert required in docs
    assert 'do not use `ingestion.watch-start`' in docs
