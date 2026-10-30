# Issue #10: production-profile scientific load rehearsal

Status: blocked before mutating execution. This checkout has no private
production-profile configuration or approved fresh SINAN fixture. A verified
staging scientific database backup, staging-only endpoint resolution, and
MinIO/watcher ownership have not been established. No `load=true` run or
scientific database mutation is claimed here.

## Existing execution path

The existing MinIO ingress materializes a file for the AlertaDengue watcher.
The completion bridge reads `ingestion.run`, emits the canonical disease input,
and workflow readiness dispatches one persistent analysis job per workflow and
input fingerprint. The analysis worker runs with concurrency one. A `load=true`
job holds a PostgreSQL advisory lock on the scientific target throughout the
Makim process and artifact checks. Failed jobs require an explicit retry; a
rehearsal failure must be preserved for investigation. The existing staging E2E
validator and production profile validator require `analysis_load=false`, so
their success does not establish SQL application or map generation.

## Gate before submission

Perform these checks on the staging host and record evidence outside the
repository. Stop on any unknown or failed check.

1. Confirm the approved fixture is a valid SINAN CSV or DBF, record its SHA-256,
   and use the read-only source role to verify that the SHA-256 is absent from
   **all** `ingestion.run` statuses. Record its expected UF, epiweek, and
   canonical disease code. The bridge accepts `disease=A90`, `A92.0`, or
   `A928` with a null `canonical_cid10`, and the coarse `disease=A92` only
   when `canonical_cid10` is `A92.0` or `A928`. It also accepts a matching
   canonical CID alongside an exact disease code. Bare `A92` is ambiguous,
   and conflicting disease and canonical CID values are rejected.
2. Verify a recent staging scientific database backup by checking its restore
   result and selected table/row checks. Record backup identity and verification
   time. Do not make the rehearsal the first test of the backup.
3. Inspect the rendered production-profile Compose configuration and both
   private worker environments without printing secrets. Resolve every database
   hostname, MinIO endpoint, bucket/prefix, incoming bind, and analysis output
   bind to staging resources. Confirm the analysis role has the intended staging
   write grants; the ingestion source role remains read-only. Confirm immutable
   analysis image identity, one analysis worker at concurrency one, and the
   existing watcher as the sole owner of the incoming path. Keep the legacy
   MinIO mirror excluded from the pipeline prefix.
4. Capture bounded, read-only before snapshots for the selected UF and epiweek
   in `Historico_alerta`, `Historico_alerta_chik`, and
   `Historico_alerta_zika`, where those tables exist. Inspect the scientific
   schema first and use its actual UF/week columns and disease keys. Record
   counts and stable row identifiers, not broad table dumps. Also record the
   current workflow and analysis job identities for that scope.

## Controlled execution and acceptance

Only after every gate passes, configure the production-profile workers for
`INFODENGUE_PIPELINE_ENVIRONMENT=prod` and
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=true`, retaining staging-only endpoints.
Submit the fixture once through the existing MinIO/watcher path. Do not invoke
an ingestion or analysis retry during this run.

Correlate the exact object identity and SHA-256 to one completed `ingestion.run`
row, its workflow input, workflow fingerprint, and analysis job. Wait for that
job's terminal state. Require `status=succeeded`, `attempts=1`, `load=true`,
and one job for the workflow/fingerprint. On failure, preserve the job, logs,
artifacts, and database state; do not reset or retry it automatically.

Inspect the job's own output directory. Validate RData with the scientific R
loader, not just file size. Check generated dengue and chikungunya SQL is
non-empty and parses against the staging scientific schema. Check Zika SQL
when the pinned AlertaDengueAnalise image and selected disease inputs generate
it; record an explicit exclusion otherwise. Verify the scientific process
reported successful SQL application, then compare bounded after snapshots for
the same UF/week and expected disease tables. Review the actual row changes,
including any expected unchanged disease table. Validate expected map PNGs by
decoding them and checking they correspond to the selected scope.

Finally, from a separate staging scientific database session, verify the
analysis advisory lock is no longer held. Compare job counts and scientific
execution logs for the exact job/fingerprint to rule out duplicate execution.
Record the image digest, fixture SHA-256, coverage, epiweek, disease inclusion
and exclusions, backup identity, before/after results, artifact paths, job ID,
status, and attempts. This establishes staging acceptance only; it is not
production cutover.
