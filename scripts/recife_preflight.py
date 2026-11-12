"""Read-only, fail-closed PR 30 rehearsal preflight.

The reviewed evidence file contains metadata and operator attestations only.
It must never contain URLs with credentials or individual SINAN records.
"""

import argparse
import json
import re
import shutil
import subprocess

from pathlib import Path
from typing import Any

from infodengue_pipeline.models import Epiweek
from infodengue_pipeline.production.validate import (
    valid_analysis_image,
    verify_rehearsal_target,
)

ATTESTATIONS = (
    'staging_host_verified',
    'compose_project_unused',
    'volumes_and_ports_exclusive',
    'watcher_unchanged',
    'no_conflicting_consumers',
    'source_read_only_approved',
    'target_dedicated_approved',
    'scientific_dependencies_verified',
    'scientific_input_coverage_verified',
    'normal_week_sql_and_retry_reviewed',
    'scientific_base_provenance_verified',
    'makim_task_verified_in_image',
    'bulletin_package_and_quarto_verified',
    'recife_geography_and_population_approved',
    'sinan_input_and_week_approved',
    'minio_prefix_empty_and_exclusive',
    'minio_permissions_verified',
    'migrations_at_head',
    'authorization_manifest_validated',
    'rollback_and_backup_verified',
)


def _check(
    checks: list[dict[str, Any]],
    identifier: str,
    passed: bool,
    observed: str,
    required: str,
    remediation: str,
) -> None:
    checks.append(
        {
            'id': identifier,
            'status': 'PASS' if passed else 'BLOCKED',
            'description': identifier.replace('_', ' '),
            'observed': observed,
            'required': required,
            'remediation': remediation,
            'stop_execution': not passed,
        }
    )


def evaluate(evidence: dict[str, Any], checkout: Path) -> dict[str, Any]:
    """Evaluate safe metadata plus independently captured role probes."""
    checks: list[dict[str, Any]] = []
    try:
        actual_sha = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            cwd=checkout,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        branch = subprocess.check_output(
            ['git', 'branch', '--show-current'],
            cwd=checkout,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        tracked = subprocess.check_output(
            ['git', 'diff', '--quiet'],
            cwd=checkout,
            stderr=subprocess.DEVNULL,
        )
        staged = subprocess.check_output(
            ['git', 'diff', '--cached', '--quiet'],
            cwd=checkout,
            stderr=subprocess.DEVNULL,
        )
        clean = tracked == b'' and staged == b''
    except (OSError, subprocess.CalledProcessError):
        actual_sha, branch, clean = '', '', False
    expected_sha = evidence.get('commit_sha')
    _check(
        checks,
        'reviewed_commit',
        bool(
            isinstance(expected_sha, str)
            and re.fullmatch(r'[0-9a-f]{40}', expected_sha)
            and actual_sha == expected_sha
            and clean
            and branch == 'feat/intramunicipal-bulletin-recife'
        ),
        actual_sha[:12],
        'reviewed SHA, branch, clean tracked files',
        'Review checkout and commit tracked changes.',
    )
    images = evidence.get('images')
    images = images if isinstance(images, dict) else {}
    for name in ('core', 'analysis', 'bulletin'):
        image = images.get(name)
        image_valid = isinstance(image, str) and valid_analysis_image(image)
        _check(
            checks,
            f'{name}_image',
            image_valid,
            'immutable digest' if image_valid else 'missing',
            'reviewed repository@sha256 digest for this commit',
            'Build and inspect the image, then record its immutable digest.',
        )
    _check(
        checks,
        'project_identity',
        evidence.get('project') == 'infodengue-pipeline-pr30',
        str(evidence.get('project', 'missing')),
        'infodengue-pipeline-pr30',
        'Use an unused dedicated Compose project.',
    )
    prefix = evidence.get('minio_prefix')
    valid_prefix = (
        isinstance(prefix, str)
        and bool(prefix)
        and prefix.endswith('/')
        and not prefix.startswith('/')
        and '..' not in prefix.split('/')
    )
    _check(
        checks,
        'minio_prefix',
        valid_prefix,
        prefix if valid_prefix else 'missing or unsafe',
        'exclusive non-root prefix ending in /',
        'Select an approved empty prefix and inspect all consumers.',
    )
    try:
        Epiweek.parse(evidence['week'])
        week_valid = True
    except (KeyError, ValueError, TypeError):
        week_valid = False
    _check(
        checks,
        'selected_week',
        week_valid,
        str(evidence.get('week', 'missing')),
        'valid YYYYWW',
        'Review the real dengue and chik ingestion weeks.',
    )
    digest = evidence.get('object_sha256')
    key = evidence.get('object_key')
    object_valid = (
        isinstance(digest, str)
        and bool(re.fullmatch(r'[0-9a-f]{64}', digest))
        and isinstance(key, str)
        and valid_prefix
        and key.startswith(prefix)
        and key.endswith(('.csv', '.dbf'))
        and isinstance(evidence.get('object_size'), int)
        and evidence['object_size'] > 0
    )
    _check(
        checks,
        'sinan_object',
        object_valid,
        'metadata complete' if object_valid else 'metadata missing',
        'exact key, SHA-256, positive size under approved prefix',
        'Hash and identify the approved real SINAN object.',
    )
    approved = evidence.get('approved_target', {})
    identity = (
        (
            approved.get('database'),
            approved.get('server_address'),
            approved.get('server_port'),
        )
        if isinstance(approved, dict)
        else (None, None, None)
    )
    writer = evidence.get('writer_probe', {})
    reader = evidence.get('history_probe', {})
    source = evidence.get('source_probe', {})
    try:
        verify_rehearsal_target(writer, reader, identity)
        target_valid = all(identity)
    except ValueError:
        target_valid = False
    _check(
        checks,
        'scientific_target',
        target_valid,
        'matched' if target_valid else 'missing or mismatched',
        'writer and history reader match independently approved target',
        'Run rehearsal-analysis, rehearsal-history and rehearsal-target.',
    )
    source_identity = (
        (
            source.get('database'),
            source.get('server_address'),
            source.get('server_port'),
        )
        if isinstance(source, dict)
        else (None, None, None)
    )
    source_valid = (
        isinstance(source, dict)
        and source.get('role') == 'rehearsal-source'
        and all(source_identity)
        and source_identity != identity
    )
    _check(
        checks,
        'source_isolation',
        source_valid,
        'distinct' if source_valid else 'missing or same target',
        'source probe has a distinct approved identity',
        'Run rehearsal-source and verify the staging source inventory.',
    )
    path = evidence.get('capacity_path')
    required = evidence.get('required_free_bytes')
    try:
        free = shutil.disk_usage(path).free if isinstance(path, str) else 0
        capacity_valid = (
            isinstance(required, int) and required > 0 and free >= required
        )
    except OSError:
        free, capacity_valid = 0, False
    _check(
        checks,
        'disk_and_wal_capacity',
        capacity_valid,
        str(free),
        'free bytes >= reviewed data, WAL and output allowance',
        'Measure target filesystem and record a reviewed allowance.',
    )
    for identifier in ATTESTATIONS:
        passed = evidence.get(identifier) is True
        _check(
            checks,
            identifier,
            passed,
            'reviewed' if passed else 'unverified',
            'operator evidence explicitly reviewed',
            f'Complete and record the {identifier} gate in the runbook.',
        )
    failed = sum(item['status'] != 'PASS' for item in checks)
    return {
        'status': 'PASS' if failed == 0 else 'BLOCKED',
        'passed': len(checks) - failed,
        'blocked': failed,
        'checks': checks,
    }


def main() -> None:
    """Print a concise summary and machine-readable report; never secrets."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--json-output', required=True, type=Path)
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text(encoding='utf-8'))
    if not isinstance(evidence, dict):
        raise SystemExit('BLOCKED: evidence must be a JSON object')
    result = evaluate(evidence, Path(__file__).resolve().parents[1])
    args.json_output.write_text(
        json.dumps(result, indent=2) + '\n', encoding='utf-8'
    )
    print(
        f'{result["status"]}: {result["passed"]} passed, '
        f'{result["blocked"]} blocked; report={args.json_output}'
    )
    if result['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
