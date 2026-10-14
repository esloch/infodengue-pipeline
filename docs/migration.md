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
Canonical path compatibility follows the existing mover naming convention.

The current AlertaDengue stage/merge and rollback are production-validated
behavior. This repository has not implemented a staging E2E runtime or a
production cutover.
