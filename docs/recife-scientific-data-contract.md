# PR 30 scientific data contract and bootstrap boundary

Status: **BLOCKED** until the staging DBA inspects the exact scientific base
image, its installed `AlertTools` package, source catalog and real PE coverage.
The development checkout and installed package establish the dependencies
below; they do not establish the content or provenance of the pinned staging
image.

## Observed dependency path

The checked out `main/main_BR.R` calls `getCidades()`, `pipe_infodengue()`,
`tabela_historico()` and `write_alerta()` from `AlertTools`. Inspection of the
installed function bodies shows these transitive reads:

| Call | PostgreSQL relation | Required data |
| --- | --- | --- |
| `getCidades` | `"Dengue_global"."Municipio"` | All enabled PE municipality geocodes, names, region and UF metadata |
| `read.parameters` | `"Dengue_global".parameters` | Disease-specific thresholds, climate variables and model configuration for each PE municipality |
| `getCases` | `"Municipio"."Notificacao"` and `"Dengue_global"."Municipio"` | Genuine dengue/chik notifications from 201001 through selected week, digitization/onset dates and population |
| `getClima` | `"weather"."copernicus_bra"` | Municipality climate observations from 201001 through selected week |
| `write_alerta` | `"Municipio"."Historico_alerta"`, `"Municipio"."Historico_alerta_chik"` | Upsert target with original sequences, keys and unique constraints |
| `maps-br`, `maps-state` | Both histories and `"Dengue_global"."Municipio"` | Current-week incidence and municipality joins; checked-in national/state shapes are also required |

The bulletin additionally reads Recife notifications and both history tables.
`getCases` obtains population from the municipality table. The scientific task
uses `iniSE=201001`, computes **all PE municipalities** for `--states PE`, and
defaults to a 100-week history output window. A Recife-only subset is
scientifically insufficient. Review the exact installed R package definitions
for any further helper queries before exporting.

## Safe preparation procedure

1. **Plan:** Record the staging source and approved dedicated target server
   identities, database names and owning Compose project. Run the existing
   `rehearsal-source`, `rehearsal-analysis` and `rehearsal-history` probes.
   Run `rehearsal-target` with independently approved identity values. Require
   source and target to differ. Refuse a shared or previously owned target.
2. **Discover:** On the source as a SELECT-only role, inspect `pg_catalog`
   for the listed schemas, tables, columns, primary/unique keys, indexes,
   sequences, extensions, functions, views and dependencies. Record counts,
   minimum/maximum epiweeks and distinct PE geocodes for each disease and
   weather. Compare all PE geocodes against `getCidades()` output. Confirm
   notification dates and climate records cover 201001 through the selected
   week; retain enough real history for model inputs. Do not infer completeness
   from a nonzero row count.
3. **Capacity:** Estimate filtered table bytes with a representative sample,
   index growth, WAL and two full output generations. Require measured free
   space to exceed the reviewed total plus reserve. Confirm backup and a
   restore test for the dedicated target.
4. **Provision:** The DBA creates a **new, separately owned** PR 30 database
   and roles. A source role has SELECT only and read-only transactions. The
   scientific writer has SELECT/INSERT/UPDATE only where the inspected SQL
   requires it, plus sequence usage if required. The bulletin reader has
   SELECT only and a read-only default session. Never grant live production
   access to the writer.
5. **Export/import:** After catalog review, export schema with `pg_dump`
   using the reviewed schema list. Export *filtered* real rows through
   `COPY (SELECT ...) TO STDOUT` within a read-only repeatable-read snapshot.
   Include all PE municipalities, their disease parameters, notifications,
   climate observations and the required historical window. Import into the
   empty target with `COPY FROM STDIN`, then restore sequences, constraints,
   indexes and grants. Keep private dumps outside the repository; encrypt and
   restrict their lifetime under the DBA's process. Do not dump the 35 GB
   source by default.
6. **Verify:** Compare row counts, minimum/maximum week, PE geocode sets,
   source/target checksums of aggregate counts, constraints, indexes,
   sequences, extensions and role grants. Run the scientific job first with
   `--load false` against the isolated target and inspect generated SQL for
   its exact week, target, uniqueness and idempotency semantics. Only then
   authorize `--load true`.

The source has nullable `"Localidade_id"`; PostgreSQL's ordinary unique
constraint permits multiple NULL values. Recreate the actual constraint,
including its NULL behavior, rather than replacing it with a key based on
`COALESCE`. Preserve the original `id` primary keys and sequence defaults.

The development scientific checkout now uses `scripts/load_alertas.py` to
validate both normal-week SQL files and send them to one `psql
--single-transaction` invocation. The previously pinned staging base may
instead apply the two files separately. This difference must be resolved by
inspecting the **actual image**, not a Docker build argument or label. If the
image applies separately, a failed second load may leave dengue committed.
Stop automatic retries, preserve SQL/logs, compare target rows for the selected
week against both files, restore the isolated target from its checkpoint if
necessary, and retry only after an operator signs off on reconciliation.
Even a single transaction does not prove the SQL is safe to run twice.

No automated bootstrap is provided until the exact source schema and
scientific package dependencies are verified. Generating a generic SQL dump
now would risk missing transitive data or changing real scientific results.
