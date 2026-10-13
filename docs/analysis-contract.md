# Analysis job contract

AlertaDengueAnalise remains independently executable. The pipeline will call
it only through the finite OCI job contract documented in its
`docs/JOB_CONTRACT.md`; it will not import that repository's Python or R code.

The job receives `ALERTA_DB_HOST`, `ALERTA_DB_PORT`, `ALERTA_DB_NAME`,
`ALERTA_DB_USER`, `ALERTA_DB_PASSWORD`, and `ALERTA_OUT_DIR=/outputs`.
Optional correlation values are `ALERTA_JOB_ID` and
`ALERTA_INPUT_FINGERPRINT`. The invocation corresponds to
`uv run makim pipeline.refresh-alertas-job --week YYYYWW --states ALL --cores N
--load true`. The OCI adapter is deferred. No real analysis job is run in
Phase 0.
