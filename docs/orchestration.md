# Epiweek orchestration

The coordinator records successful A90 and A92 inputs by epiweek. One disease
alone leaves the workflow waiting. Both diseases for the same epiweek make it
ready; inputs from separate epiweeks never combine. A stable fingerprint
identifies the exact pair of source runs. Claiming a ready fingerprint is
persisted before future execution, making repeated completion events
idempotent.

Phase 0 records readiness and claim state only. It does not launch analysis.
Retries, lease recovery, and full job lifecycle handling will be added with
the real external runner in a later phase.
