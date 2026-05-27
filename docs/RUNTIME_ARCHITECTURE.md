# Gmeow Go Runtime Architecture

This document describes the current Go runtime that replaces the old Python
application. It is the detailed architecture guide for the branch being prepared
to become the main Gmeow implementation.

The only retained Python code is `python/`, published as `gmeow-intel`, an
external ANALYSIS adapter package. It is not the application runtime, does not
load `gmeow.toml`, does not own scheduling, and does not talk to PostgreSQL,
RabbitMQ, Gmail, or FILESTORE except through worker-managed job contracts.

## Runtime Topology

Gmeow is a set of typed Go services connected by gRPC:

- FILESTORE owns object bytes, manifests, facets, compound structure,
  provenance, overlays, source ingest claims, source cursors, and annotations.
- QUERY owns PostgreSQL, pgvector rows, Apache AGE graph projection, migrations,
  rebuilds, and projection refreshes.
- SCHEDULER owns RabbitMQ topology, priorities, retries, dead letters,
  deterministic job keys, and projection scheduling.
- ANALYSIS workers consume scheduler-owned work, read objects from FILESTORE
  through gRPC, and write FILESTORE annotations through gRPC.
- SOURCE/BACKEND services hydrate external data into FILESTORE and expose live
  source operations to INTERFACE through gRPC.
- INTERFACE exposes MCP, REST, IMAP, CLI, and admin workflows over shared
  application services.

Every service boundary is typed. Whole-request JSON is not the internal service
protocol; JSON appears only in intentionally dynamic metadata leaf fields.

## Ownership Rules

- QUERY is the only runtime component that opens PostgreSQL connections.
- SCHEDULER is the only runtime component that talks to RabbitMQ.
- ANALYSIS only talks to FILESTORE for object reads and annotation writes.
- BACKENDS talk to their backend APIs and FILESTORE-facing ingest contracts.
- INTERFACE talks to QUERY, SCHEDULER, FILESTORE retrieval services, and
  BACKENDS through application service contracts and gRPC clients.
- FILESTORE notifies SCHEDULER over gRPC after object, annotation, overlay, and
  source-state changes; SCHEDULER decides whether to schedule analysis work,
  QUERY projection refresh, or both.

The package-level architecture tests enforce these boundaries so import drift
cannot silently bypass the deployed service model.

## FILESTORE

FILESTORE is authoritative. Objects are addressed by BLAKE3 identity and stored
under the configured FILESTORE root with zstd-compressed bytes, immutable
recovery sidecars, manifests, annotations, overlays, and source-state metadata.

FILESTORE creates both byte-bearing objects and stable compound objects.
Compound objects carry their own digest, manifest, facets, and role-mapped
parts. Gmail messages are stored as `mail_message` compound objects with
headers, body, Gmail metadata, MIME structure, and attachment subobjects.

SOURCE adapters never compute dedupe locally. They look up source identity,
acquire FILESTORE ingest claims for misses, stream payloads through
`FilestoreService`, and let FILESTORE own identity and reuse decisions.
Source identity lookups use FILESTORE source-index records and validate the
target manifest. Runtime SOURCE paths must not walk the object tree to recover
missing source indexes; a missing index is a lookup miss and normal ingest will
write the index.

FILESTORE also maintains a reverse parent index for compound parts. Annotation
refresh uses that index to update affected compound parents directly instead of
walking all compound objects.

## QUERY

QUERY is a rebuildable PostgreSQL projection over FILESTORE authority. It
projects manifests, facets, provenance, relationships, compound parts, graph
facts, keywords, embeddings, overlays, analysis status, and source cursors.

PostgreSQL can be wiped and rebuilt from FILESTORE. QUERY rebuild and changed
projection read authoritative manifests and annotations; recovery sidecars are
for emergency operator triage and are not projection authority. These are
explicit operator workflows. Normal runtime projection refresh is digest-scoped
and does not walk the filestore.

QUERY exposes typed gRPC search, object projection, structure, graph, analysis
status, source cursor, and operational surfaces. SQL is parameterized through
the Go implementation and remains inside QUERY ownership.

## SCHEDULER

SCHEDULER owns RabbitMQ coordination. Production RabbitMQ vhost and queue prefix
are deployment configuration. The production example uses the `gmeow-prod`
vhost and `gmeow.` queue prefix; integration tests use the `gmeow-test` vhost
and `gmeow.test.` queue prefix.

SCHEDULER derives deterministic analysis and projection jobs from FILESTORE
state, analyzer specs, forced interface requests, and repair workflows. It owns
priorities, retries, exponential backoff, dead-letter inspection, requeue, and
projection refresh scheduling.

Runtime projection refresh queue messages carry object digests. SCHEDULER loads
those objects from FILESTORE and calls QUERY projection per object. The
background scheduler loop processes failures and digest-scoped projection
refreshes; broad scans remain explicit admin commands.

Analyzer jobs are granular by object, analyzer name, and analyzer version. If a
summary job fails, SCHEDULER retries that summary job; completed NER,
categorization, embedding, or header annotations are not rerun just because
another analyzer failed.

## ANALYSIS

ANALYSIS workers receive scheduler jobs, check FILESTORE for an existing
matching analyzer name/version annotation, and skip already-complete work. New
results are durable only after FILESTORE annotation writes succeed; workers ack
only after that durability point.

The production email analyzer set uses `phase04-email-v2` for Go analyzers and
`python-email-v1` for explicit external Python adapters. Older `phase04` and
`python-current` annotations are stale and must be rescheduled before cutover
validation passes.

Go analyzers cover text extraction, RFC822/header parsing, metadata extraction,
graph facts, endpoint embeddings, and model summaries. Python-backed NER and
categorization remain explicit external analyzer adapters through `gmeow-intel`
when Go parity is not proven.

Model endpoint analyzers serialize requests per worker instance, use a 60s
timeout, and fail closed on unavailable endpoints, malformed output, empty
responses, echoed prompts, or missing dependencies. Failed endpoint calls return
through SCHEDULER failure handling so only the failed analyzer job is retried.

## SOURCE And BACKENDS

Sources are first-class services. Gmail runs as a source gRPC server through
`gmeow source-serve <source-name>` and exposes live search, hydrate-backed
search, retrieve, action, backfill, and inbox refresh operations to INTERFACE
and admin clients. INTERFACE must not instantiate Gmail adapters in-process or
call Gmail APIs directly.

Gmail backfill is a SOURCE workflow. It pages Gmail, hydrates messages, writes
compound objects and source cursors to FILESTORE, and stops there. FILESTORE
notification then lets SCHEDULER derive analysis and projection through the
normal scheduler-owned RabbitMQ path.

Inbox refresh is also source-owned. When enabled, it refreshes the configured
rolling inbox query, defaulting to recent inbox mail, and stores a separate
namespaced cursor under the Gmail source state. When both backfill and inbox
refresh are enabled, the source service runs both concurrently; inbox refresh
does not wait for historical backfill.

Drive is intentionally design-only on this email-focused branch. Unsupported
Drive operations fail at capability boundaries rather than degrading silently.

## INTERFACE

INTERFACE exposes shared application services over multiple protocols:

- MCP stdio through `gmeow mcp-serve`.
- MCP Streamable HTTP through `gmeow mcp-http-serve` and `gmeow-mcp.service`.
- REST through `gmeow rest-serve`.
- Read-only IMAP through `gmeow imap-serve`.
- CLI and admin commands through `gmeow` and `gmeow-admin`.

MCP Streamable HTTP is the production MCP server. It supports stateful sessions,
server-sent event streams, `Mcp-Session-Id`, `Last-Event-ID` replay with an
in-process event store, client-driven session termination, health, and metrics.
The stdio MCP command remains for local MCP hosts that own stdin and stdout.
MCP tool result content is TOON-only for agent consumption; tools do not expose
JSON text or `structuredContent`.

Protocol packages parse requests and shape responses. They do not import
concrete FILESTORE, QUERY, SOURCE, SCHEDULER, or ANALYSIS implementations.

## Main Workflows

### Gmail Backfill

1. The Gmail source service pages Gmail according to configured backfill or
   inbox-refresh settings.
2. For each Gmail message identity, the source asks FILESTORE whether the
   object already exists.
3. On a miss, the source acquires a FILESTORE ingest claim, hydrates the message,
   and submits the compound object and subobjects through `FilestoreService`.
4. FILESTORE writes authoritative objects and source cursors, then notifies
   SCHEDULER.
5. SCHEDULER schedules missing/stale analysis and QUERY projection refresh.
6. QUERY projects the resulting manifests and annotations into PostgreSQL.

Backfill and inbox refresh use separate cursor keys. Operators can run rolling
inbox refresh while a long historical backfill is still in progress.

### Search

1. INTERFACE receives search through MCP, REST, IMAP, CLI, or admin workflow.
2. Application services select QUERY and any applicable live BACKENDS.
3. QUERY returns projected results from PostgreSQL.
4. Gmail live search may return existing FILESTORE digests or hydrate missing
   results according to request shape and source capabilities.
5. Application services fuse results by FILESTORE identity and fetch structure
   from FILESTORE only when the response requires expansion.

### Forced Analysis

1. INTERFACE asks SCHEDULER to force analysis for selected objects/analyzers.
2. SCHEDULER creates deterministic high-priority jobs for the requested analyzer
   specs and versions.
3. ANALYSIS workers skip already-complete annotations, run missing work, and
   write FILESTORE annotations.
4. FILESTORE notification lets SCHEDULER schedule QUERY projection refresh.

### Rebuild

1. QUERY can be stopped and PostgreSQL wiped.
2. QUERY rebuild walks FILESTORE manifests and annotations.
3. PostgreSQL tables, vector rows, graph facts, overlays, source cursors, and
   analysis status are reconstructed from FILESTORE.

## Operations

Production services are controlled with systemd units under `deploy/systemd/`.
`gmeow.target` groups all services; role targets group core services, backends,
workers, and interfaces; `gmeow.slice` provides shared resource control.

The canonical local gate is `make check`. Production readiness also requires
systemd verification, sanitized public release checks, Gmail credential sanity,
live backfill/inbox-refresh validation when enabled, and confirmation that stale
analyzer versions have been rescheduled.

## Documentation Map

- `docs/ARCHITECTURE.md` is the concise boundary contract.
- `docs/architecture/` contains subsystem architecture docs for runtime
  foundation, FILESTORE, QUERY, SCHEDULER, ANALYSIS, SOURCES, INTERFACES, and
  operations.
- `docs/systemd.md` describes grouped service operation.
- `docs/TESTING.md` describes testing boundaries and anti-mock rules.
- `docs/FILESTORE_BACKUP_RESTORE.md` describes FILESTORE backup and restore.
