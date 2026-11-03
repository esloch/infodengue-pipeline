# Bulletin integration review and merge order

Verified against local checkouts of `AlertaDengue`, `AlertaDengueAnalise`, and
`AlertaDengue/boletins`. This is a contract and implementation plan, not a
claim that bulletin publication or `analysis_load=true` has been validated.

## Current workflow

The pipeline scans MinIO, atomically materializes supported objects in the
shared incoming directory, and correlates their SHA-256 with the existing
AlertaDengue watcher and `makim ingestion.run`. AlertaDengue inspects content,
moves and merges it, then records `ingestion.run` as `completed` with coverage,
disease, canonical CID-10, delivery week, SHA-256 and finish time. The read-only
completion bridge emits a deterministic event through the pipeline outbox.
Workflow readiness is keyed by coverage and epidemiological week. The pipeline
persists one analysis job per input fingerprint and dispatches its ID to the
existing analysis worker. `AlertaDengueAnalise` runs
`makim pipeline.refresh-alertas-job --week ... --states ... --cores ... --load ...`.
The pipeline records `SUCCEEDED` only after zero exit and current-week RData
and SQL validation. Duplicate deliveries cannot claim a running or terminal
job; failed scientific jobs need explicit retry.

`AlertaDengueAnalise` owns the R computation, generated SQL, SQL application
with `psql -v ON_ERROR_STOP=1` when `load=true`, and maps after loading.
Its Makim script uses `set -euo pipefail`, so subprocess failures propagate.
At the default `load=false`, SQL is not applied and maps are skipped. The
pipeline does not itself prove that `Municipio.Historico_alerta` and
`Municipio.Historico_alerta_chik` contain the requested week. No bulletin
dispatch, publication state, or downstream analysis event exists yet.

## Verified bulletin boundary

The repository is [`AlertaDengue/boletins`](https://github.com/AlertaDengue/boletins),
checked out locally as `/opt/services/infodengue/boletins`. The national entry
point is `Nacional/gerar_boletim_nacional_semanal.sh YYYYWW [arbo|dengue|chik]`;
it renders `Nacional/output/YYYYWW/BR-YYYYWW.pdf` by default. It reads the
historical alert tables through a PostgreSQL connection configured in
`Nacional/.env`, with an SSH tunnel opened separately, plus local reference
data. The state entry point is
`Estaduais/gerar_boletim_estadual.sh YYYYWW UF [den_chik|dengue|arbo]`;
it renders `Estaduais/output/UF/YYYYWW/EUF-YYYYWW.pdf` by default. Its Rmd
loads the matching `ale-UF-YYYYWW.RData` from
`Estaduais/dados/alertas/YYYYWW/` plus local reference data.

Both entry points accept week and coverage but still call `install.packages`
at runtime and set a machine-specific Pandoc path. The state renderer also
hardcodes a home-directory checkout path. National rendering needs a manually
prepared tunnel and file-based credentials. Legacy `uso-ssh.R` contains SCP
copy commands with fixed destinations; the current national and state shell
entry points render PDFs and do not publish them. These are separate workflows
and must not be treated as a verified publication API. The inspected current
entry points do not require interactive Google authentication.

## Independently mergeable issues

1. **Make rendering finite and portable — `AlertaDengue/boletins`.** Keep the
   existing national and state renderers; validate week, UF and disease mode,
   remove runtime package installation and machine-specific paths, package
   pinned R/LaTeX/system dependencies, accept explicit input and output paths,
   and use noninteractive read-only database credentials where needed.
   **Accept:** one invocation per coverage/week succeeds or fails with a
   nonzero exit, writes a predictable nonempty PDF under a supplied directory,
   needs no sibling checkout, manual SSH session, home-directory data copy or
   interactive setup, and performs no publication. Cover success, missing
   inputs and invalid arguments with repository-owned tests.

2. **Define the data-ready contract — `AlertaDengueAnalise` and
   `AlertaDengue/boletins`.** Identify which bulletin modes consume applied
   historical SQL versus RData, and specify how the requested week and
   coverage are verified in the scientific target database or artifact set.
   Keep SQL application and historical table updates in
   `AlertaDengueAnalise`; make the bulletin renderer reject missing or stale
   inputs. **Accept:** a read-only, reproducible check distinguishes generated
   SQL (`load=false`) from successfully applied history, including the
   requested week and coverage; fixture tests show stale/missing data fails.
   Actual `load=true` operational acceptance remains pending.

3. **Schedule bulletin rendering — `esloch/infodengue-pipeline`.** After 1–2,
   use the already persisted terminal analysis job and current workflow
   fingerprint as the readiness source; require the data-ready check before
   dispatch. Add only the durable job state and routing that the concrete
   renderer needs. Key idempotency by bulletin kind, coverage, week, input
   fingerprint and renderer version; claim atomically and record artifact,
   exit status, attempts and errors. Recover queued delivery after broker
   failure, and require explicit retry after execution failure. **Accept:**
   duplicate delivery and Beat polls make one logical job, stale or failed
   analysis cannot render, `load=false` alone cannot render a DB-backed
   bulletin, and a later input fingerprint can produce a new job. Tests use
   fake renderer/data sources and isolated pipeline SQL state.

4. **Publish explicitly — `AlertaDengue/boletins` for the destination adapter,
   `esloch/infodengue-pipeline` for orchestration state.** First select the
   actual publication destination and authentication method with operators;
   the legacy SCP examples are insufficient as a contract. Render and publish
   are distinct operations. Use noninteractive secret injection and an
   idempotent destination key with a receipt or remote verification. **Accept:**
   publication is off by default, never starts before a validated PDF and
   data-ready check, duplicate requests cannot create conflicting outputs,
   and failed publication is observable and safely retryable without
   rerendering or silently declaring success.

Issue 1 can proceed independently. Issue 2 fixes the data gate; issue 3 depends
on 1–2. Issue 4 depends on 1 and 3 plus a chosen publication destination.

## Operational risks and open decisions

- National PDF reads historical tables, while state PDF reads RData; freshness
  and source-to-target database routing must be verified for each mode.
- A scientific process can apply SQL and then fail during maps or artifact
  validation. Its job will be `FAILED` even if database writes occurred;
  operators must inspect the target before an explicit retry.
- The current runner validates file presence and size, not semantic freshness
  of historical rows. The job fingerprint identifies ingestion inputs, not
  necessarily all external bulletin reference data or renderer revisions.
- A worker crash after claiming `RUNNING` leaves an analysis job for manual
  recovery; queued sends are retried, scientific execution is not automatic.
- Shared local artifact paths suit one host. Multi-host execution needs an
  artifact transport contract. Secrets must stay out of argv, logs and PDFs.
- Exact national database access, required historical coverage, renderer
  version policy, publication destination and publication approval policy
  remain operator decisions. No production or staging database was changed.
