# Infodengue Pipeline

Infodengue Pipeline will coordinate SINAN ingestion and downstream workflow
for InfoDengue. It is experimental and does not run production ingestion.

The service will use MinIO for transient ingress, canonical file storage for
imported sources, and PostgreSQL for processed state.
