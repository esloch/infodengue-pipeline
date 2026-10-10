# Database

PostgreSQL contains workflow coordination and outbox state in the `pipeline`
schema. SQLAlchemy 2 typed models and repositories own persistence boundaries;
Alembic creates the initial schema. Development uses a separate local database
on loopback. No production schema is changed.

The existing SINAN schema has not yet been migrated. Phase 2 inventories its
exact tables, constraints, and indexes, then proposes a baseline strategy
before any data migration. Avoid destructive schema changes.
