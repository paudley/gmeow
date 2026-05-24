# PostgreSQL Partition Readiness

Backfill is out of scope. This plan exists only to keep future schema choices compatible with very large on-demand cached mail.

## Candidate Partition Keys

- `messages`: range partition by `message_ts` once the active catalog is large enough to need it.
- `message_parts`: partition by parent `messages.message_ts` through a denormalized `message_ts` column only if part scans become large.
- `graph_triples`: partition by `source_message_id` date only after graph refresh jobs need it; keep global node/profile summaries outside partitions.
- `embedding_chunks`: partition by `source_kind`, then optionally by message date for message chunks.
- `content_object_refs`: partition by `ref_table` only if CAS-reference audits become a bottleneck.

## Current Readiness Choices

- Message bodies and large payloads stay in CAS, not heap rows.
- Parsed `message_ts`, sender/recipient fields, `search_tsv`, graph profiles, edge stats, summaries, and timelines are materialized separately from full message bodies.
- New indexes are scoped so future partitions can keep small local indexes.
- Summary and timeline materialized views provide agent-facing aggregate access without requiring wide message scans.

## Deferred Until Needed

- Creating partitions.
- Moving existing cached rows between partitions.
