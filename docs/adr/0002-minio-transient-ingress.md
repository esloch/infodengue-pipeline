# ADR 0002: MinIO is transient ingress

Status: Accepted

Use MinIO for incoming transient objects, StorageBox for canonical imported
files, and PostgreSQL for processed/queryable data. Development lifecycle
configuration targets only the local development bucket.
