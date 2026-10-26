#!/usr/bin/env bash
set -euo pipefail

fixture=$(realpath -e -- "$1")
backup=$(realpath -e -- "$2")
restore_proof=$(realpath -e -- "$3")
test -f "$fixture" && test -f "$backup" && test -f "$restore_proof"
export PROD_E2E_FIXTURE_HOST_PATH="$fixture"
export PROD_E2E_BACKUP_HOST_PATH="$backup"
export PROD_E2E_RESTORE_PROOF_HOST_PATH="$restore_proof"
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  run --rm --no-deps analysis-e2e-runner \
  python scripts/run_prod_analysis_e2e.py \
    --fixture /e2e/fixture --backup /e2e/backup \
    --restore-proof /e2e/restore-proof \
    --backup-sha256 "$4" --expected-host "$5" \
    --expected-database "$6" --expected-role "$7" --execute
