# Gmeow Archive

Completed and verified items moved out of `TODO.md` and `PLAN.md`.

## Verification Snapshot

Verified on 2026-05-23 against the local server at `http://127.0.0.1:8765`.

- Live status: 80 messages, 14 attachments, 10,645 graph triples, 1,380 embedding chunks.
- Intelligence queue: 93 done, 0 pending.
- Maintenance: 0 missing object files, 0 orphan content objects, 0 ingest issues.
- AGE: available, graph `gmeow_graph`.
- Migrations: Alembic applied through `20260523_0007`.
- Compile check: `uv run python -m compileall src/gmeow migrations`.
- Route/tool verification used `rg` over `src/gmeow/app.py`, `src/gmeow/mcp_server.py`, `src/gmeow/pg_cache.py`, `src/gmeow/sync.py`, migrations, and package dependencies.

## Archived TODO Items

1. Gmail-style query syntax in `text_search`

   Verified: `PgCache.text_search` parses `from:`, `to:`, `subject:`, `label:`, `after:`, and `before:` and `gmail_text_search` exposes the path.

2. Compact thread mode

   Verified: `gmail_get_thread(compact=True)` is the MCP default and `PgCache.compact_message` / `compact_messages` return bounded message summaries.

3. Date-range filtering

   Verified: REST/MCP text search and MCP semantic search accept `after` and `before`; cache text search now avoids limiting before date filtering when date bounds are present.

4. Address-based search/filter

   Verified: `gmail_address_messages` and `PgCache.address_messages` are present, and `from:` / `to:` are supported by text search.

5. Better failure/help behavior for query syntax

   Verified: `gmail_help` documents search syntax and behavior.

6. Date fields in message results

   Verified: message rows include `message_date_iso` and `date`, and thread messages are sorted chronologically.

7. Sender/recipient fields in thread messages

   Verified: compact message output includes `from`, `to`, `sender`, and `recipients`.

8. Label names instead of opaque IDs

   Verified: message results include resolved `labels` alongside `label_ids`; graph labels resolve through cache helpers.

9. Entity denoising

   Verified: default graph reads filter known HTML/CSS, numeric, cardinal, and structural ontology/type noise, while `include_noise` keeps raw access available.

10. Contact/people resolution

   Verified: MCP tools `gmail_contacts`, `gmail_people`, `gmail_set_people_alias` and REST endpoints `/api/v1/contacts`, `/api/v1/people`, `/api/v1/people/aliases` exist.

11. rustworkx projection/path/ranking exposure

   Verified: MCP tools and REST endpoints expose graph projection, shortest path, ranking, and DOAP project views.

12. Ontology interoperability profile

   Verified: graph extraction emits RDF/RDFS, FOAF, SIOC, schema.org, SKOS, PROV-O, DOAP, and `gmeow:` triples; DOAP project views are exposed.

13. Attachment content access

   Verified: `gmail_get_attachments` and `GET /api/v1/messages/{message_id}/attachments` return attachment metadata sidecars and extracted text fields. Attachment blobs stay separate through `GET /api/v1/attachments/{sha1}`.

14. TOON default for MCP read/search tools

   Verified: MCP tools default `format="toon"` and allow `format="json"`.

15. Automated categorization

   Verified: deterministic categories, manual rules, manual overrides, TF-IDF learned clusters, category stats, recategorization, and default-hidden category filtering are implemented.

16. New-message async intelligence

   Verified: sync enqueues message and attachment intelligence jobs; `run-intelligence-worker` drains pending jobs and applies graph/category/semantic work.

17. Sync status / corpus stats

   Verified: `gmail_status`, `GET /api/v1/sync/status`, and maintenance endpoints report corpus counts, date range, intelligence jobs, semantic dimensions, graph/AGE health, and object-store health.

18. `sync_priority` documentation

   Verified: `gmail_help` describes configured rules, `limit_per_rule`, archive/cache behavior, category defaults, and thread compact behavior.

19. Gmail History API incremental sync

   Verified: `gmail_sync_history`, `POST /api/v1/sync/history`, and `uv run gmeow sync-history` exist; truncated history responses do not advance the stored cursor.

20. Header intelligence / critical facts

   Verified: markdown generation includes header intelligence and deterministic critical facts for dates, money, emails, phone numbers, URLs, and action-oriented sentences.

21. Attachment CAS duplicate-reference correctness

   Verified: migration `20260523_0007` gives attachments a row id and unique `(message_id, part_id, sha1)` reference identity so duplicate payload bytes can appear on multiple messages without losing per-message attachment rows.

22. Ontology-aware graph discovery

   Verified: graph discovery results now include node profiles with `kind`, `namespace`, `visibility`, `role`, and `noise_reason`; top/rank/search/projection/message-node APIs can filter by kind, namespace, and visibility. Default discovery shows user-facing semantic nodes while structural ontology classes, mailbox identifiers, metadata-key artifacts, Gmail label artifacts, and extraction noise are available only when explicitly requested.

## Archived Plan Items

1. Python 3.13 `uv` package layout

   Verified: package modules exist under `src/gmeow/` for config, Gmail client, sync, cache/storage, REST, MCP, CAS object store, semantic search, graph extraction, markdown extraction, categories, intelligence worker, and CLI.

2. FastAPI REST server and Streamable HTTP MCP endpoint

   Verified: `create_app` builds FastAPI routes under `/api/v1` and mounts the MCP app routes.

3. Loopback-only unauthenticated local access

   Verified: server binds to `127.0.0.1` by default and `loopback_guard` rejects non-loopback clients/host headers.

4. Gmail authentication and limited mutation surface

   Verified: service-account and user-OAuth Gmail clients exist; exposed actions are label, unlabel, archive, star/unstar, mark read/unread, sync, search, and reads. No send, draft, trash, delete, or filter mutation endpoints are present.

5. Repo-local storage layout

   Verified: PostgreSQL is the primary catalog; Alembic migrations exist; BLAKE3 CAS object storage, sidecar metadata paths, Tantivy lexical search paths, and secrets paths are configured.

6. REST interface surface

   Verified: health, status, sync, labels, threads, messages, raw, raw RFC822, markdown, attachments, search, graph, actions, contacts, people, ingest, and maintenance endpoints are implemented in `src/gmeow/app.py`.

7. MCP interface surface

   Verified: labels/threads/messages/attachment metadata resources and Gmail search/read/graph/category/status/sync/action tools are implemented in `src/gmeow/mcp_server.py`.

8. Local cache architecture

   Verified: PostgreSQL stores normalized metadata, MIME parts, attachment references, sync state, categories, graph triples, async jobs, and pgvector chunks; CAS stores raw JSON/RFC822, body text, markdown, MIME text, and attachment bytes; Tantivy indexes lexical message fields.

9. Priority and history sync

   Verified: priority rules live in config, `sync_priority` hydrates matching messages, `sync_history` hydrates changed messages from Gmail history, and both enqueue intelligence work.

10. Semantic index

   Verified: `PgSemanticIndex` chunks with `semchunk`, embeds through the configured Nomic-compatible endpoint, and stores vectors in PostgreSQL through pgvector.

11. Graph storage and traversal

   Verified: relational graph triples are authoritative, AGE mirroring exists, read-only Cypher is exposed, and rustworkx powers projection/path/rank/project views.

12. Ontology profile

   Verified: RDF/RDFS, FOAF, SIOC, schema.org, SKOS, PROV-O, DOAP, and `gmeow:` predicates are emitted and documented.

13. Markdown extraction

   Verified: deterministic markdown includes message headers, body text, attachments, critical facts, tasks, dates, links, and evidence-oriented sections.

14. Header intelligence

   Verified: routing/authentication/list/message-id/reference parsing is implemented and surfaced in markdown.

15. Scale-oriented PostgreSQL storage plan

   Verified: large bodies are separated from headers/metadata into CAS, compressible payloads are compressed, SQL objects are versioned and commented, and operational maintenance views exist.

16. Apache AGE and SQL object comments

   Verified: AGE status/Cypher endpoints exist and current added SQL columns/indexes/constraints have comments.

23. Scoped HNSW vector indexes

   Verified: message and attachment partial HNSW indexes exist for 768-dimensional embeddings, semantic search accepts `source_kind`, and embedding calls are batched against the llama.cpp-compatible endpoint.

24. Normalized message columns and SQL full-text

   Verified: `messages` has parsed timestamp, sender/recipient address/domain fields, recipient arrays, and `search_tsv`; text search composes lexical, date, address, label, and category filters in SQL.

25. Hybrid ranking

   Verified: REST `/api/v1/search/hybrid` and MCP `gmail_hybrid_search` combine lexical, semantic, graph, recency, and category visibility signals into ranked message results.

26. Materialized graph node profiles

   Verified: `graph_node_profiles` stores node label, kind, namespace, visibility, role, noise reason, degree, and message count; refresh tooling is exposed through CLI and REST.

27. Object-store reference accounting

   Verified: `content_object_refs` tracks row-to-CAS digest references for messages, MIME parts, attachments, and ingest artifacts; diagnostics report reference counts by kind and orphan checks use the reference table.

28. Summary and centroid tables

   Verified: `summary_items` stores thread, contact, category, and project summaries with message counts, latest timestamps, metadata, and centroid embeddings when source vectors exist.

29. Graph edge weights and path scoring

   Verified: `graph_edge_stats` stores evidence counts, message counts, timestamps, and traversal weights; path results include weights and scores, and weighted path APIs are exposed.

30. Timeline and emergence views

   Verified: materialized views `message_timeline_daily` and `graph_entity_emergence` are refreshable and exposed through REST/MCP timeline and emerging-entity APIs.

31. Operational diagnostics

   Verified: storage diagnostics report installed extensions, relation sizes, index sizes, HNSW indexes, vector dimensions, materialized-view population, maintenance state, and CAS health.

32. Materialized summary views

   Verified: refreshable materialized views exist for message search summaries, thread summaries, contact summaries, category summaries, project summaries, and graph node summaries.

33. Partition-readiness plan

   Verified: `docs/POSTGRES_PARTITION_READINESS.md` records future partition-compatible schema choices while explicitly excluding full-archive backfill.

34. Timed server maintenance tasks

   Verified: `gmeow serve` starts a configurable scheduler for History API sync, priority sync, intelligence job draining, derived cache/materialized-view refresh, PostgreSQL analyze, and optional attachment sidecar refresh. REST exposes `/api/v1/maintenance/timed` and `/api/v1/maintenance/timed/{task_name}/run`.

35. Attachment analysis graph enrichment

   Verified: sidecar analysis now emits first-class document nodes, calendar event nodes, document author nodes, attachment evidence nodes, source-specific document/OCR/vision/calendar entity nodes, message rollup edges, document text edges, calendar participant edges, archive-contained-file edges, graph profile kinds, and weighted edge support. Attachment intelligence was rebuilt and graph profiles/edge stats/materialized summaries were refreshed.

36. Rustworkx graph algorithms

   Verified: weighted path traversal now uses rustworkx Dijkstra over `graph_edge_stats`; centrality, bridge/broker, weak/strong component, related-node, cycle diagnostic, and message-recommendation APIs are exposed through REST and MCP. HTTP checks returned successful results for centrality, components, bridges, related nodes, cycles, and recommendations.

37. Resilience and self-recovery

   Verified: intelligence jobs have durable leases, backoff, stale-run reclaim, and dead-letter handling; sync runs are checkpointed; operational events are persisted; startup self-checks report degraded features; liveness, readiness, degraded, resilience, job, dead-letter, event, and repair-cache APIs are exposed through REST/MCP/CLI; repair checks validate object-store presence and decompression. Live checks returned ready=true and resilience ok=true.

38. Long-term archive and read-only IMAP foundations

   Verified: archive completeness requires Gmail JSON plus canonical RFC822; CAS writes are atomic and verified; object verification records manifest status; attachment sidecar metadata is versioned in CAS; retention policies default to tombstone with SPAM/TRASH purge policies; manifested archive export/verify/restore workflows exist; read-only IMAP exposes Gmail labels as folders with stable UIDs and serves RFC822 from CAS. Live checks completed one bounded archive completion, verified object integrity, exported and verified a manifested CAS bundle, and smoke-tested IMAP LOGIN/SELECT/UID SEARCH/UID FETCH.

## Not Archived

- Full-archive backfill remains out of scope and must not be planned.
