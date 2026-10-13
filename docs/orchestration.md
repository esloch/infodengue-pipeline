# Epiweek orchestration

The coordinator records successful dengue (A90), chikungunya (A92.0), and Zika
(A928) inputs by epiweek. Required diseases are configured with
`INFODENGUE_PIPELINE_REQUIRED_DISEASES`, defaulting to `A90,A92.0`; Zika is
currently optional. Readiness is reached when every configured required
disease has an input for the same epiweek. Inputs from separate epiweeks never
combine.

The analysis fingerprint includes every available supported disease input,
including optional Zika, in stable CID-10 order. This records the identity of
all inputs the analysis job can use; receiving or changing optional Zika after
default readiness therefore produces a new input fingerprint. Claiming a
ready fingerprint is persisted before future execution, making repeated
completion events idempotent. Because requirements are configuration rather
than disease-specific workflow columns, Zika can be made required without a
schema redesign.

The existing AlertaDengue historical/runtime ecosystem already contains Zika
data structures, so the pipeline preserves Zika as a distinct disease input.

Phase 0 records readiness and claim state only. It does not launch analysis.
Retries, lease recovery, and full job lifecycle handling will be added with
the real external runner in a later phase.
