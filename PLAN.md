# Gmeow Active Plan

## Scope Constraint

Full-archive backfill is not in scope and is not part of the current consideration list. Do not plan, implement, optimize, or prioritize full-archive backfill work. On-demand hydration from live search and ongoing freshness through priority/history sync are allowed.

## Current State

The original implementation plan has been completed and archived in `ARCHIVE.md`. The server now has loopback REST and Streamable HTTP MCP interfaces, Gmail sync/search/read/actions, PostgreSQL/Alembic storage, BLAKE3 CAS payload storage, Tantivy lexical search, pgvector semantic search, asynchronous intelligence work, attachment sidecars, graph extraction, AGE/rustworkx graph views, categorization, contact/people views, and operational status/maintenance endpoints.

## Remaining Plan Items

1. Keep backfill excluded from planning until explicitly allowed later.
