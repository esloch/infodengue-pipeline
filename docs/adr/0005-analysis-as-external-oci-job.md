# ADR 0005: Analysis as an external OCI job

Status: Accepted

Keep AlertaDengueAnalise independently executable. The pipeline will invoke
its stable finite OCI job contract through an `AnalysisRunner` port and will not
import analysis code or require either checkout inside the analysis runtime.
