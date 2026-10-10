# ADR 0004: Transactional outbox

Status: Accepted

Persist domain events in the same database transaction as their state change.
Publish asynchronously and mark sent only after broker confirmation. Consumers
must tolerate duplicates because delivery is at least once.
