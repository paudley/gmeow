# Go Migration Phase 02 - QUERY Projection

## Goal

Build QUERY as a rebuildable PostgreSQL projection over FILESTORE. PostgreSQL is a secondary index,
not authority.

## Build

- Add goose migrations for the greenfield QUERY schema.
- Project object directories into PostgreSQL:
  - objects;
  - facets;
  - provenance;
  - relationships;
  - compound parts;
  - analysis status;
  - graph edges;
  - keywords;
  - embeddings;
  - overlays;
  - source cursors.
- Implement full rebuild from FILESTORE only.
- Implement incremental projection from changed annotations.
- Implement query interfaces for:
  - lexical search;
  - facet filters;
  - provenance filters;
  - graph/relationship lookup;
  - compound expansion;
  - analyzer status;
  - vector placeholder path or pgvector integration if embeddings exist.
- Ensure all SQL uses parameterized `pgx` calls or generated/query-builder code.

## Retire Python Equivalent

After Go QUERY can rebuild from FILESTORE and serve the planned search APIs, remove old Python
database schema and query code:

- `src/gmeow/db.py`
- `src/gmeow/pg_cache.py`
- `src/gmeow/pg_cache_archive.py`
- `src/gmeow/pg_cache_helpers.py`
- `src/gmeow/pg_cache_imap.py`
- `src/gmeow/pg_cache_jobs.py`
- `src/gmeow/semantic_pg.py` once vector projection is migrated
- `migrations/` Alembic runtime and old Python migration versions after greenfield goose migrations
  are the only supported path

## Functional Proof

- Deleting PostgreSQL and running QUERY rebuild restores projected metadata from FILESTORE.
- QUERY can search by text, facet, provenance, relationship, and compound part.
- QUERY can report missing/stale analysis state without owning analysis output.
- QUERY rebuild does not read `recovery.json`.
- String-formatted SQL is rejected by review or static checks.

## Exit Gate

- QUERY APIs can be tested with a fake implementation for service tests and a real PostgreSQL
  implementation for functional tests.
- No FILESTORE package imports PostgreSQL code.
- Old Python PostgreSQL cache and Alembic schema are removed after rebuild parity is proved.
