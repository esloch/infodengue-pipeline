# Events and outbox

`sinan.ingestion.completed` version 1 contains a stable event ID, ingestion run
ID, disease code, epiweek, content fingerprint, and timezone-aware completion
time. Its existing shape accepts dengue (A90), chikungunya (A92.0), and Zika
(A928); adding Zika support does not require changing the event version.
Event identity makes repeated delivery safe.

The outbox row is stored transactionally with application state. The publisher
marks it sent only after RabbitMQ confirms publication. A crash between broker
confirmation and marking can produce a duplicate, so consumers must be
idempotent. Delivery is at least once, not exactly once.
