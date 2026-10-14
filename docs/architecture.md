# Architecture

## Ownership boundaries

```text
MinIO transient ingress → incoming → watcher → mover → manifest → stage → merge
                                                       AlertaDengue reference

pipeline state + outbox → finite AlertaDengueAnalise OCI job (not implemented)
```

AlertaDengue owns scientific ingestion behavior and its validated stage, merge,
and rollback. AlertaDengueAnalise owns scientific computation. This pipeline
owns workflow identity, readiness, durable events, and future external job
coordination. Communication uses finite runtime contracts; the pipeline does
not import R implementation code or provide a plugin framework.

## SINAN metadata and coverage

`SinanMetadata` is the normalized inspection result: `disease`, `coverage`,
`epiweek`, `file_format`, and `source_name`. It is produced after validating
the file contents. Incoming filenames are not authoritative. Disease,
coverage, and epiweek are derived from validated file contents by the ingestion
inspection layer. This foundation does not implement a SINAN content parser;
the existing AlertaDengue mover remains the migration compatibility
reference. `source_name` is provenance only.

`Coverage` is either `BR` for national delivery or one of the supported state
codes. National workflow identity is `BR + epiweek`; state identity is `UF +
epiweek`. Canonical paths and filenames are derived from normalized metadata,
for example `mg/dbf/dengue/2024/202452/DenInfodengue_MG_202452.dbf`.

National readiness requires dengue (A90) and chikungunya (A92.0). State
readiness requires dengue (A90); chikungunya and Zika (A928) are optional.
Every available supported input participates in a stable fingerprint, so a
later optional input creates a distinct analysis identity.

## Events and state

The `IngestionCompleted` v1 event carries run ID, disease, coverage, epiweek,
fingerprint, and completion time. It is recorded in a PostgreSQL outbox in the
same transaction as pipeline state, then published through RabbitMQ/Celery.
The unique workflow key is `(coverage, epiweek)`; each disease has one current
input per workflow.

## External jobs

`AnalysisRunner` is a small boundary for one finite external analysis job.
`Coverage.BR` maps to analysis scope `ALL`; a state maps to that UF. The
foundation uses `DisabledAnalysisRunner`, so no scientific job is launched.
A future finite Boletim runner can use an explicit contract without changing
this boundary into a generic job registry.

## Compatibility direction

Current AlertaDengue operational behavior is the reference:

```text
MinIO → mc mirror --watch → complete local file → incoming directory
  → watcher → makim ingestion.run → content inspection/mover → canonical file
  → manifest → enqueue → stage → merge
```

The sender may use arbitrary names. The mover discovers disease from fields
such as `ID_AGRAVO`, `CID10`, `CID10_CODIGO`, or `CID10CODIGO`; epiweek from
`SEM_NOT` variants; and geography from UF fields in the content. It checks
format-specific candidate fields in priority order and uses a candidate with
one unambiguous valid UF; if none qualifies, it routes to BR. The legacy tests
confirm, for example, that CSV prioritizes `SG_UF_NOT` while DBF prioritizes
`SG_UF`. It generates the canonical path only after inspection. The pipeline
must preserve these rules during migration without copying the legacy parser
into this foundation.
