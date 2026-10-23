#!/usr/bin/env bash
set -euo pipefail
if ! fixture=$(realpath -e -- "$1"); then
  echo 'STAGING E2E: FAIL: fixture does not exist' >&2
  exit 1
fi
if [[ ! -f "$fixture" ]]; then
  echo 'STAGING E2E: FAIL: fixture must be a regular file' >&2
  exit 1
fi
case "${fixture,,}" in
  *.csv) suffix=.csv ;;
  *.dbf) suffix=.dbf ;;
  *) echo 'STAGING E2E: FAIL: fixture must have .csv or .dbf suffix' >&2; exit 1 ;;
esac
export STAGING_E2E_FIXTURE_HOST_PATH="$fixture"
export STAGING_E2E_FIXTURE_CONTAINER_PATH="/tmp/staging-e2e-fixture${suffix}"
docker compose --env-file .env.staging -f containers/compose-staging.yaml run --rm --no-deps e2e-runner python scripts/run_staging_e2e.py --fixture "$STAGING_E2E_FIXTURE_CONTAINER_PATH" --timeout-seconds "$2"
