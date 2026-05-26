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
- Implement incremental projection from changed authoritative annotations via `gmeow-admin query
  project-changed --since <RFC3339>`.
- Implement query interfaces for:
  - lexical search;
  - facet filters;
  - provenance filters;
  - graph/relationship lookup;
  - compound expansion;
  - analyzer status;
  - pgvector search for projected embeddings;
  - read-only Apache AGE status and Cypher exploration over projected graph data.
- Ensure all SQL uses parameterized `pgx` calls or generated/query-builder code.

## Retired Python Equivalent

Go QUERY can rebuild from FILESTORE and serve the planned search APIs. The old Python database
schema and query code is retired:

- `src/gmeow/db.py`
- `src/gmeow/pg_cache.py`
- `src/gmeow/pg_cache_archive.py`
- `src/gmeow/pg_cache_helpers.py`
- `src/gmeow/pg_cache_imap.py`
- `src/gmeow/pg_cache_jobs.py`
- `src/gmeow/semantic_pg.py`
- `migrations/` Alembic runtime and old Python migration versions; greenfield goose migrations are
  the only supported path

## Functional Proof

- Deleting PostgreSQL and running QUERY rebuild restores projected metadata from FILESTORE.
- QUERY can search by text, facet, provenance, relationship, and compound part.
- QUERY can report missing/stale analysis state without owning analysis output.
- QUERY rebuild does not read `recovery.json`.
- Incremental projection reads manifests and authoritative annotation files, not `recovery.json`.
- String-formatted SQL is rejected by review or static checks.

## Exit Gate

- QUERY APIs are exercised through the real PostgreSQL-backed query implementation. Tests outside
  QUERY's own package-local private behavior cross the QUERY boundary through its gRPC service.
- No FILESTORE package imports PostgreSQL code.
- Old Python PostgreSQL cache and Alembic schema are removed after rebuild parity is proved.
