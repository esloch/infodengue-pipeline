# Issue #17: PR #18 production-profile staging acceptance

This document records the development VPS check, the manual staging procedure,
and the observed staging ingress acceptance performed on 2026-10-30. The five
isolated ingress reliability scenarios passed. The complete AlertaDengue
watcher/ingestion integration has not been validated with a fresh approved SINAN
fixture. Do not run its commands on the development VPS or against production.

## Development VPS findings

The checkout was `be9affe49eaeed67073758641c6440fcaf1a3d9f`. The host had the
AlertaDengue development deployment and development MinIO, but not the staging
network `infodengue-staging_infodengue` or `/opt/data/staging/sinan/incoming`.
The private `.env.prod` and `.env.prod.analysis` files were absent. The user
systemd bus could not be reached, so watcher state was not inspectable. No
staging services were started and no objects were uploaded.

`uv run pytest -q` passed 240 tests; 2 PostgreSQL tests were skipped because a
dedicated disposable PostgreSQL URL was not configured. Regression coverage
includes replacement, unchanged-content deduplication, interrupted download,
prepared-file recovery and post-publication recovery. These were development-only results. The local analysis image label identified
AlertaDengueAnalise revision `27a03f26a9bf366597ef135e7da8d2f620ea9a8e`;
the deployed scientific base image's exact upstream revision provenance remains
unverified. Actual staging ingress evidence is recorded below.

## Staging prerequisites

Run the following procedure manually over SSH on the existing staging server.
Stop at any failed gate. Reuse its AlertaDengue MinIO, Docker network, existing
watcher and incoming directory. Do not create services, networks, watchers or
ingestion logic.

Before starting, the staging operator must verify and record:

- Staging SSH target and reviewed PR #18 full Git commit SHA.
- Existing healthy `infodengue-staging_infodengue` integration network,
  AlertaDengue PostgreSQL and MinIO services.
- Verify exactly one running AlertaDengue watcher for
  `/opt/data/staging/sinan/incoming` and its actual process ancestry. The
  observed staging watcher was started via `makim ingestion.watch-start`, not
  an established systemd user unit.
- Staging MinIO endpoint and bucket, exact selected prefix, and every mirror
  or pipeline consumer. Confirm the prefix is empty and exclusive: for another
  consumer on that bucket, neither prefix may contain the other. Exclude this
  prefix from the legacy mirror through its normal operations procedure before
  starting the new scanner.
- Approved, valid chikungunya CSV/DBF fixtures A and B. B has different bytes;
  retain B for the same-content upload. Both SHA-256 values are absent from
  staging `ingestion.run`.
- Read-only staging source and analysis DB roles, with grants checked by the
  staging DBA. Analysis loading stays disabled.
- Reviewed immutable core and analysis image references (`repository@sha256:`
  plus 64 lowercase hex digits), tied to the PR revision and pinned
  AlertaDengueAnalise revision. Use existing images/registry; do not build or
  add registry infrastructure on staging.

Host-specific endpoints, prefix, image digests, commit, DB role names and
fixture paths must be verified on staging. Do not invent them or use production
values. Never print private env files or unredacted Compose configuration.

## Staging commands

### Host, resources and filesystem preflight

```sh
hostname -f
pwd
git rev-parse HEAD
test "$(git rev-parse HEAD)" = 'VERIFIED_PR_18_COMMIT_SHA'
docker network inspect infodengue-staging_infodengue --format '{{.Name}}'
docker ps --format '{{.Names}} {{.Image}} {{.Status}} {{.Networks}}'
docker volume ls --format '{{.Name}}'
test -d /opt/data/staging/sinan/incoming
pgrep -af 'ingestion_watcher.py|ingestion.watch-start'
# Verify exactly one active watcher; do not start an additional watcher.
findmnt -T /opt/data/staging/sinan
findmnt -T /opt/data/staging/sinan/incoming
stat -c '%n %F %a %u:%g' /opt/data/staging/sinan /opt/data/staging/sinan/incoming
namei -l /opt/data/staging/sinan/incoming
ss -ltn '( sport = :5436 or sport = :5675 )'
```

Verify the expected AlertaDengue containers are healthy and attached to the
staging network; inspect routing without printing container environments or
credentials. Confirm PostgreSQL/RabbitMQ ports 5436/5675 are free. The
production Compose profile binds the incoming parent
`/opt/data/staging/sinan` to `/var/lib/pipeline`; its incoming directory is
`/var/lib/pipeline/incoming`. The staging parent and incoming must share a
filesystem and incoming must not be a separate mountpoint. Check worker runtime
UID/GID can create the private staging directory in the parent. Staging files
use mode 0700 and published files mode 0640; confirm the watcher user can
traverse and read them. Do not broaden permissions to bypass a failure.
Verify the dedicated output path `/opt/data/staging/prod-analysis-output` and
analysis image runtime UID/GID permissions; do not use staging's regular
analysis output path.

Confirm the selected MinIO prefix is empty and exclusive using the staging
operator's existing MinIO listing/policy tools. Inspect non-secret endpoint,
bucket and prefix routing for all relevant containers and host consumers.
Docker environment listings alone do not prove ownership. If isolation is
uncertain, stop before deployment or upload.

If `infodengue-pipeline-prod` containers or volumes already exist, inspect and
preserve their state. Do not initialize or replace them when ownership is
unclear. Record the existing containers, volume names and watcher PID for
rollback comparison.

### Private configuration and production profile validation

Use the tracked examples to provision private `.env.prod` and
`.env.prod.analysis` with the site's secret process. Do not overwrite existing
private files. Restrict both to mode 0600. Set and verify these repository
configuration values:

- `PIPELINE_WORKER_ENV_FILE=../.env.prod` and
  `PIPELINE_ANALYSIS_ENV_FILE=../.env.prod.analysis`.
- `PROD_INTEGRATION_NETWORK=infodengue-staging_infodengue`.
- `PROD_SINAN_INCOMING_HOST_PATH=/opt/data/staging/sinan/incoming`.
- `PROD_ANALYSIS_OUTPUT_HOST_PATH=/opt/data/staging/prod-analysis-output`.
- `PIPELINE_POSTGRES_HOST_PORT=5436` and
  `PIPELINE_RABBITMQ_HOST_PORT=5675` (Compose binds both to loopback).
- `PIPELINE_IMAGE` and `PIPELINE_ANALYSIS_IMAGE` are the reviewed immutable
  image digests. Set `INFODENGUE_PIPELINE_ANALYSIS_IMAGE` to the analysis image
  digest in both env files.
- Both files have `INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
  `INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`.
- Pipeline DB and broker URLs use only the profile's own
  `pipeline-postgres:5432/pipeline_prod` and `pipeline-rabbitmq:5672//`.
- MinIO endpoint and bucket are the existing staging service/bucket; the
  configured prefix is the verified exclusive staging prefix. Runtime access
  is read/list only for its authorized bucket/prefix.
- Source and analysis DB URLs resolve only to staging, use separate
  read-only roles, and do not contain production endpoints or credentials.

Do not output either env file or run bare `docker compose config`. Validate:

```sh
stat -c '%a %n' .env.prod .env.prod.analysis
uv run makim containers.prod-config
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --images
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --services
```

Review the displayed image and service lists; verify external endpoints, paths,
environment, analysis load and prefix are staging-only. Then run the existing
read-only production validator:

```sh
uv run makim prod.validate
```

Require success before deploying or uploading. This task checks rendered
configuration, worker health/concurrency/image identity, migrations, pipeline
PostgreSQL, RabbitMQ, MinIO access, source/analysis DB connectivity and worker
filesystem access. It does not prove DB grants, watcher ownership or MinIO
prefix isolation; the operator checks above establish those.

### Deploy and validate the live flow

Start only the existing production profile. Keep legacy sync excluded from the
chosen prefix and the existing watcher running:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
uv run makim prod.validate
```

Require healthy PostgreSQL, RabbitMQ and all five workers; `migrate` must have
exited successfully. Confirm ingestion and analysis concurrency are one and
`analysis_load=false`. Stop on any failure.

Using the approved staging MinIO operator upload credentials (not the
read/list-only runtime account), upload fixture A to a new key under the
exclusive prefix. Record the exact key, ETag/version, size and SHA-256. Wait
for one `pipeline.ingress_object` row in `materialized` status with matching
SHA-256, then one successful `ingestion.run` with that SHA. Verify the complete
file reached incoming and the existing watcher consumed it. Record sanitized
object ID, hash, run ID, coverage and epiweek.

Allow another Beat scan interval or invoke the existing
`pipeline.ingestion.scan` task through its staging worker. Verify unchanged
content creates no new ingress identity, materialization or ingestion run.
Then replace the exact same MinIO key with valid fixture B. Confirm ETag/version
and hash change; verify a new ingress identity and exactly one new successful
ingestion run, with the original ingress/run history retained.

Replace the object once more with identical fixture B bytes and changed object
metadata (therefore changed ETag/version). Verify the new ingress identity is
`skipped`, SHA-256 matches B, no file is published, and no extra ingestion run
occurs. Keep before/after row counts and sanitized IDs as evidence. Do not use
the staging-profile E2E runner; it requires `environment=staging`, while this
rehearsal specifically uses `environment=prod`.

For interrupted-download recovery, use a sufficiently large approved fixture.
Only after logs confirm its download began, stop and restart the production
profile's ingestion worker; do not interrupt an active SINAN watcher action:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  stop worker-ingestion
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  start worker-ingestion
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps worker-ingestion
```

Verify no partial CSV/DBF appeared in incoming. Let the scheduled scanner
recover the object, then verify one complete publication, matching hash, and no
duplicate ingestion. Preserve ingress records and logs while reviewing
`discovered`, `materialized`, `skipped`/`failed` status, SHA-256, and corresponding
`ingestion.run` history. Use existing deterministic regression tests for
prepared-file and post-publication crash windows; do not inject database
failures on staging. Check completion bridge/outbox/workflow/job evidence where
applicable. Do not claim scientific analysis success unless the workflow was
ready and the job actually succeeded. SQL mutation remains disabled.

### Safe rollback

On failure, stop only the new materializer and scheduler. Preserve all state:

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  stop worker-ingestion worker-beat
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
docker compose --env-file .env.prod -f containers/compose-prod.yaml logs \
  --no-color --timestamps
```

Let the existing watcher finish active ingestion. Restore legacy synchronization
for this prefix only after confirming both the new scanner and Beat are
stopped. Leave the watcher running. Preserve PostgreSQL/RabbitMQ volumes,
database records, logs, MinIO objects and prepared ingress files for diagnosis.
Never use `down -v`, remove volumes, or delete prepared ingress files. Do not
stop unrelated staging services. Record final service state and sanitized
evidence below. A successful rehearsal does not transfer production
synchronization ownership.

## Acceptance results: 2026-10-30 staging

### Scope and deployment

The live tests used the already deployed PR #18 pipeline implementation with
`INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`. The staging MinIO bucket was
`sinan-infodengue-issue9`, with prefix `issue17-prod-rehearsal/`.
The tested checkout HEAD was
`5c651a53eda7ba0c40c161b39ea8f240a5e12a6e`.

A dedicated test writer uploaded synthetic CSV payloads. The existing pipeline
runtime account read them. Real `IngressCoordinator`, `MinioObjectStore`
and pipeline PostgreSQL persisted the results. During each controlled test,
`worker-ingestion` and `worker-beat` were stopped and temporary one-off
containers ran scans with incoming overridden to
`/var/lib/pipeline/issue17-acceptance/incoming`, corresponding to host path
`/opt/data/staging/sinan/issue17-acceptance/incoming`. That path is *not*
the real AlertaDengue watcher input. The running AlertaDengue watcher itself
was not changed. The two pipeline workers were subsequently restored.

The final `uv run makim prod.validate` exited successfully. Pipeline
PostgreSQL showed four ingress rows under the acceptance prefix, with empty
`last_error` in the final query.

### Same-key scenario evidence

Object:
`issue17-prod-rehearsal/acceptance-3c8331741d754a84a4d281c946ac8007.csv`

| Version | SHA-256 | Ingress status |
|---|---|---|
| A | `d09db7ab67144c162b284e106cea33fad925c521fcccc7be657a11dbb9d39e15` | `materialized` |
| B (same key, different bytes) | `56a3a8c9684634d294546337ffd2a15170ff98010ea1f0f3714713c6f3b44bb7` | `materialized` |
| B again (same SHA, changed multipart ETag) | `56a3a8c9684634d294546337ffd2a15170ff98010ea1f0f3714713c6f3b44bb7` | `skipped` |

The initial scan materialized one full hash-verified file; a second unchanged
scan materialized zero. Replacing the exact key with B materialized one new
file while retaining A's database row. Reuploading B with a different
multipart ETag created a new `skipped` row and no new publication.

### Injected transfer interruption and recovery

Object:
`issue17-prod-rehearsal/recovery-ed5c017101ca410784c5978ecf9e3d7c.csv`

SHA-256:
`4b86d5757d07bc541d05852ad9930801c518782c4714757f82e7901f389939bf`

An injected `fget_object` exception after creating a partial temporary file
produced `materialized=0`, `status=failed`, and no published CSV.
Retried against the real MinIO object, the scanner produced
`materialized=1`, `status=materialized`, with the matching SHA-256.
Another scan materialized zero. This **was not** a live worker-process kill;
process interruption and crash windows have separate automated regression
coverage.

### Acceptance matrix

| Check | Result | Evidence/limitation |
|---|---|---|
| Production-profile validator | PASS | `prod.validate` exited 0 after workers restored |
| Initial isolated materialization | PASS | New `materialized` row; complete file hash matched |
| Unchanged scan | PASS | Zero new publication |
| Same-key changed-content replacement | PASS | New SHA-256; second `materialized` row |
| Same-content replacement, changed ETag | PASS | Third row `skipped`; no new file |
| Injected interrupted download and retry | PASS | No partial publication; recovered and idempotent |
| Real worker termination during download | NOT TESTED | No live process kill was performed |
| Fresh SINAN through AlertaDengue watcher | PENDING | No fresh, approved ingestion-compatible fixture was available |
| AlertaDengue `ingestion.run` / downstream analysis | PENDING | Synthetic CSV was intentionally isolated; no run/job asserted |
| Scientific base image revision provenance | PENDING VERIFICATION | Exact upstream revision not independently confirmed for deployed base |

### Release boundary and follow-up

These results validate live **ingress reliability**, not complete SINAN
ingestion or scientific processing. Issue #17 requires complete staging
workflow validation before production deployment. A successful run through
the existing AlertaDengue watcher and `ingestion.run` remains an explicit
acceptance gap; do not report the entire issue as satisfied until it is
addressed or the acceptance scope is reviewed and changed.

The running pipeline workers were restored after testing, but the configured
prefix still contains synthetic acceptance objects. Before routine scanning
or cutover, disable the synthetic prefix through a reviewed configuration
change and verify routing so these objects cannot subsequently reach the
real monitored directory. Preserve evidence and pipeline state; do not
perform undocumented destructive cleanup.
