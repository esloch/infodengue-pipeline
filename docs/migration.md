# Migration

Migration proceeds in small phases while the existing AlertaDengue behavior
remains authoritative.

1. Establish this standalone foundation for workflow identity, outbox events,
   canonical path construction, and development infrastructure.
2. Before porting ingestion, perform parity tests against the existing
   AlertaDengue mover, stage, merge, and rollback behavior. The tests must use
   content-derived disease, epiweek, and UF cases, including arbitrary source
   filenames, ambiguous/unknown geography, and national fallback.
3. Port the validated mover and ingestion execution boundary only after parity
   is demonstrated. Keep content inspection as the source of normalized
   `SinanMetadata`; do not infer routing from names.
4. Introduce finite external analysis execution after its runtime contract,
   artifacts, and refresh behavior are validated.
5. Consider a separate finite BoletimQuinzenal runner only after the analysis
   boundary is operational and a concrete second workload exists.

The workflow key is `(coverage, epiweek)`. `BR` is national and expects dengue
(A90) plus chikungunya (A92.0); a state UF expects dengue (A90), with chik and
Zika optional. State and national files for the same week never combine.
The canonical storage API returns only the deterministic `base_destination`.
Final placement in the ingestion migration must implement checksum deduplication,
numbered collision versions (`_01`, `_02`, ...), include-existing recovery,
requeue behavior, epiweek-window/outdated policy, `allow-outdated`, manifest
metadata, and host/worker path translation.

A90 dengue and A92.0 chikungunya base routing align with the legacy mover.
The new model intentionally preserves A928 Zika as a separate disease and
routes it to `zika`. The current legacy mover routes A92.8/A928 through its
A92 family and can place it in `chik`. This migration divergence requires an
explicit parity and correction step before ownership moves.

The current AlertaDengue stage/merge and rollback are production-validated
behavior. This repository has not implemented a staging E2E runtime or a
production cutover.
