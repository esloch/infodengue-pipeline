# ADR 0001: Standalone pipeline service

Status: Accepted

Run ingestion and workflow coordination in a standalone Python service. Keep
the public application and scientific analysis in their existing projects.
This allows each system to retain its execution model and clear ownership.
