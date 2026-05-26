# Gmeow Go Rewrite - FILESTORE-First Architecture & Work Plan

## Context

This is no longer a compatibility port of the current Python application. Treat it as a greenfield
Go rewrite that keeps the lessons from the working system but does not preserve the current
PostgreSQL schema, Alembic history, Gmail-shaped data model, or API compatibility.

The primary goals are:

1. **Rich interfaces over all stored knowledge.** INTERFACE exposes MCP, REST, IMAP, CLI, and later
   filesystem-style interaction planes over the whole FILESTORE, using QUERY for search, graph,
   semantic retrieval, summaries, analytics, and operational inspection.
2. **Extensible analysis.** ANALYSIS supports new analyzers without rewriting the core system. It
   runs idempotently against FILESTORE, tracks analyzer versions, and can prioritize interactive or
   forced work above background indexing.
3. **Many input methods.** SOURCE adapters may backfill, hydrate, live-search, live-retrieve, export,
   delete/action, or only push normalized objects into FILESTORE. ANALYSIS and QUERY then pick up the
   resulting objects automatically.
4. **Robust, self-healing operation.** The system is designed around replayable state, idempotent
   jobs, durable manifests, integrity checks, rebuildable indexes, retries, dead letters, and repair
   scans.
5. **SOLID boundaries.** Components depend on narrow interfaces, not concrete databases, brokers,
   source APIs, analyzers, or interaction planes.

The architecture is built around six discrete components:

- **FILESTORE:** authoritative content, metadata, generated analysis, graph facts, embeddings, and
  human overlays.
- **QUERY:** rebuildable secondary index over FILESTORE, initially PostgreSQL with extensions.
- **SCHEDULER:** derives missing/stale work and publishes prioritized jobs.
- **ANALYSIS:** analyzer workers that enrich FILESTORE idempotently.
- **SOURCE:** adapters and push ingestors that add or hydrate content.
- **INTERFACE:** MCP, REST, IMAP, CLI, and later FUSE-style access through shared services.

## Architectural Principles

### FILESTORE Is Authority

FILESTORE is the definitive datastore. PostgreSQL is not a source of truth; it is a projection that
can be deleted and rebuilt from FILESTORE alone.

The canonical storage key is a **BLAKE3-style object digest**, but the digest is derived differently
by object class. Byte-bearing file objects use the BLAKE3 hash of canonical uncompressed bytes.
Compound objects use a stable semantic object ID chosen at creation time, then derive the storage
digest as `BLAKE3("compound:{object_id}")`. Source identity is provenance, not the domain model. A
Gmail message, Drive file export, local note, ringme artifact, PDF attachment, OCR text output,
generated summary, and embedding payload are all content objects with manifests.

Each content object has:

- immutable bytes in CAS;
- an immutable emergency JSON sidecar stored beside the CAS blob;
- an atomic mutable manifest;
- at least one domain facet describing how it can be interpreted;
- provenance records describing where it came from;
- relationships to other objects;
- generated analysis outputs;
- graph facts and keywords;
- embeddings or references to embedding objects;
- human overlays such as notes, categories, aliases, and visibility decisions.

This keeps the system able to expose a FUSE-like view later: even if a user browses the store as
files, the source provenance may not matter.

Every FILESTORE object key maps to an object directory. At minimum, each object directory contains
the immutable blob and emergency sidecar. Compound objects without natural source bytes use their
canonical JSON envelope as the blob payload, so they get the same resilience treatment:

- `blob.zstd`: the compressed object bytes;
- `recovery.json`: a small emergency sidecar.

The emergency sidecar is **not** the manifest. It is write-once, never updated, and not read by
normal gmeow processes. It exists for later human recovery if the database, manifests, or indexes are
damaged. It must contain enough context to identify and minimally use the blob by itself: source
hint, observed filename/title if known, media type, uncompressed size, compressed size,
uncompressed BLAKE3/SHA-256, compressed BLAKE3/SHA-256, compression codec, creation timestamp, and
the FILESTORE/schema version that wrote it. No blob in the system should ever be a mystery file.

Authoritative object state is stored as compressed annotation files in the same object directory,
for example:

- `manifest.json.zst`: core object manifest, facets, provenance, relationships, and compound parts;
- `analysis.json.zst`: analyzer outputs and analyzer status;
- `overlays.json.zst`: human-authored overlays;
- additional `{name}.json.zst` annotation files for future first-class state.

QUERY reconstruction is a directory walk: enumerate FILESTORE object directories by object key,
read authoritative annotation files for each object, and project them into QUERY. The emergency
`recovery.json` sidecar is ignored during reconstruction unless an operator is doing manual disaster
recovery.

FILESTORE also owns all deduplication. SOURCE adapters, ANALYSIS workers, QUERY, and INTERFACE code
do not decide whether content is "the same object." They submit bytes or a declared compound object
ID to FILESTORE; FILESTORE computes the digest, reuses the existing object when one exists, and
merges facets, provenance, relationships, overlays, and analysis state through manifest rules. This
prevents source-specific duplicate logic from leaking into the rest of the system.

High-duplicate sources must use FILESTORE source identity lookups before streaming payload bytes.
The source identity tuple is `source_kind`, `source_name`, `external_id`, and optional
`external_version`. If FILESTORE already maps that exact source version to a digest, SOURCE reuses
the digest and does not transfer or write payload bytes. On a miss, SOURCE must acquire a
FILESTORE-owned source ingest claim for that exact tuple before opening the network payload stream.
This prevents two workers from hydrating the same Gmail message, Drive export, or realtime segment
at the same time. SOURCE still never computes content identity; source identity is only a
zero-transfer fast path and concurrency guard.

### Facets And Compound Objects Are First Class

Every object must have at least one **facet**, and may have several. Facets describe the domain
interfaces and interpretation rules that apply to the object. They are different from provenance:
provenance says where the object came from; facets say what the object is usable as.

Examples:

- `mail_message`: compound email/message semantics, thread membership, labels, sender/recipient
  semantics, and message-level actions;
- `email_part`: RFC822 header/body/part semantics when a subobject is best understood as email
  structure rather than a generic file;
- `file`: ordinary file/document semantics, filename, path-like display, MIME-oriented retrieval;
- `phone`: call/SMS/contact semantics for ringme-style data;
- `calendar`: event/time-window semantics;
- `webpage`: captured web page semantics, canonical URL, fetch metadata, and rendered/exported
  variants;
- `song`: track-level music semantics with one or more audio encodings;
- `album`: collection-level music semantics containing ordered songs and album-level metadata;
- `analysis_output`: generated text, OCR, summary, or embedding payload;
- `container`: object that exists primarily to group subobjects.

Rules:

- every object has at least one facet;
- facets can be added by SOURCE ingestion, FILESTORE policy, application services, or human overlay;
- subobjects can have their own facets independent of the compound parent;
- subobjects are content objects and dedupe globally through FILESTORE;
- interface endpoints may be facet-filtered;
- source adapters may emit objects with multiple facets at ingest time;
- ANALYSIS does not use facets to decide semantics; analyzers work against objects, media types,
  content roles, and compound part roles.

Compound objects are mandatory real FILESTORE objects, not virtual views. A Gmail message is not a
single flattened blob and not just a QUERY projection: it is a compound FILESTORE object with a
`mail_message` facet and likely a `container` facet. In normal ingest it contains Gmail metadata,
RFC822 headers, body objects, a MIME-structure object without embedded payload bytes, and attachment
objects. It must not also retain the full raw RFC822 container when decomposed body or attachment
objects are stored, because that would duplicate payload bytes inside the MIME container. Each
attachment is a discrete subobject and may also get a `file` facet. The compound object owns
relationships among the parts, but the parts remain independently addressable, analyzable, and
searchable.

The same rule applies outside mail. A webpage compound can contain raw HTML, MHTML, PDF render, and
screenshot objects. A song compound can contain MP3, FLAC, cover art, and metadata objects. An album
compound can contain ordered song compounds plus album-level art and metadata. Compound is a general
FILESTORE modeling tool for "one conceptual thing with multiple concrete representations or parts."

If two Gmail messages carry the same attachment bytes, FILESTORE stores two compound email objects
but one shared attachment file object. The two compounds each have their own Gmail metadata object,
RFC822/header object, body object, MIME-structure object, and compound manifest. Both compounds
point at the same attachment digest through their `compound.parts` and `contains`/`part_of`
relationships.

ANALYSIS sees those subobjects as ordinary objects. RFC822/header objects can yield identities,
message IDs, temporal data, location hints, and graph nodes. Attachment objects are analyzed like
any other file. Message body objects are text objects. FILESTORE merges those subobject outputs into
the compound object's manifest and structure view; analyzers do not need to know that the compound
parent has a `mail_message` facet.

### QUERY Is Replaceable

QUERY is scoped behind a small interface. The first implementation is PostgreSQL because it is
already available and good enough for structured metadata, JSONB, full-text search, pgvector, graph
tables, summaries, and analytics. Nothing outside QUERY should know or care that PostgreSQL is the
backend.

If specialized search becomes necessary later, the implementation can add or replace projection
backends without changing FILESTORE, ANALYSIS, SOURCE, or INTERFACE contracts.

### SCHEDULER Is Explicit

The analysis pipeline needs a dedicated SCHEDULER component. It is not just a queue table. It scans
FILESTORE object directories, source event streams, authoritative annotations, and analyzer specs to
derive work:

- new content needs extraction, embedding, graph, categorization, and summaries;
- analyzer versions changed and outputs are stale;
- annotations reference missing derived objects;
- failed jobs are eligible for retry or repair;
- an interactive request needs a digest indexed now;
- an operator forced a rebuild or reanalysis.

SCHEDULER publishes jobs to RabbitMQ, which is available in the core-data stack. RabbitMQ is the
primary broker because durable acknowledgements, worker pools, priority queues, retry routing, and
dead-letter queues fit the workload well.
Production connections use the RabbitMQ `gmeow` vhost. Integration tests use the separate
`gmeow-test` vhost.

### Service Interface Protocol

Go services communicate over gRPC with typed protobuf contracts in `proto/gmeow/v1/`. The default
transport is Unix-domain sockets under `/run/gmeow/`; loopback TCP is allowed only when explicitly
configured.

The stable service interfaces are:

- `FilestoreService`: source-object lookup/claim/release, object streaming write/read, compound
  creation, manifest/structure reads, annotation writes, source cursor writes, and verification.
- `QueryService`: projection, rebuild/project-changed, search, structure, relationship, graph,
  analysis-status, vector-search, and source-cursor queries.
- `SchedulerService`: scan, enqueue, force, requeue, dead-letter inspection, status, and
  object-change notification.

The protocol must not use a whole-request JSON envelope. Stable domain shapes are first-class
protobuf messages. JSON is permitted only as `bytes *_json` leaf fields for dynamic maps such as
facet metadata, analyzer output data, cursor payloads, overlays, graph metadata, and search result
attributes.

Large content crosses the FILESTORE boundary through client-streamed `PutObjectFrame` messages and
server-streamed `ObjectChunk` reads. The FILESTORE server is the first durable writer for payload
bytes; sources do not pre-spool just to calculate hashes.

### INTERFACE Has No Primacy

MCP may be implemented first, but MCP does not own the domain model. REST, IMAP, CLI, and future
filesystem views all bind to the same application services. The shared services define the domain
operations: search, retrieve, analyze, inspect provenance, list graph neighborhoods, summarize,
hydrate, and perform source-specific actions where supported.

Some interface endpoints are intentionally facet-filtered. IMAP operates only over objects with the
`mail_message` facet. Filesystem-style browsing operates over `file` and `container` facets.
Phone/call views operate over `phone` facets. MCP and REST can expose broad cross-facet search by
default while still allowing explicit facet filters.

### SOLID Component Rules

- FILESTORE stores and validates content/manifests; it does not search or run analysis.
- FILESTORE owns the existence, digest, manifest, facets, and structure of compound objects.
- QUERY projects manifests and answers queries; it does not own durable data.
- SCHEDULER decides what work should happen; it does not implement analyzers.
- ANALYSIS runs analyzers and writes results; it does not own source collection or query behavior.
- SOURCE adapters produce content/provenance and optional live capabilities; they do not expose user
  interfaces directly.
- INTERFACE formats and authorizes user-facing operations; it does not know storage internals.

## Target Architecture

```text
                             +-----------------------------+
                             |          INTERFACE          |
                             | MCP | REST | IMAP | CLI     |
                             +---------------+-------------+
                                             |
                                             v
                             +-----------------------------+
                             |     Application Services    |
                             | search | retrieve | analyze |
                             | hydrate | graph | actions   |
                             +------+-----------+----------+
                                    |           |
                         query/read |           | content/read/write
                                    v           v
+-------------+           +----------------+    +-----------------------------+
|   SOURCE    |  ingest   |     QUERY      |    |          FILESTORE          |
| Gmail       +---------->+ PostgreSQL v1  |<---+ CAS bytes + manifests       |
| Drive       | events    | rebuildable    |    | overlays + analysis outputs |
| ringme push |           | projection     |    +-------------+---------------+
+------+------+           +----------------+                  ^
       |                                                        |
       | hydrate/live/search/action                            |
       v                                                        |
+----------------+       derive/enqueue        +----------------+-------------+
| source APIs    |<----------------------------+          SCHEDULER           |
| Gmail/Drive    |                             | RabbitMQ priority jobs      |
+----------------+                             | repair scans | stale checks  |
                                               +---------------+-------------+
                                                               |
                                                               v
                                               +-----------------------------+
                                               |           ANALYSIS          |
                                               | Go workers + Python workers |
                                               | text | OCR | NER | graph    |
                                               | embeddings | categories     |
                                               +-----------------------------+
```

## Core Data Contracts

The exact Go package layout can change, but the interfaces and data shapes should be implemented
early because they drive every component.

### Content Manifest

Each digest has one core manifest annotation, stored atomically inside the object directory.
Suggested path: `objects/blake3/aa/bb/<digest>/manifest.json.zst`.

The manifest is authoritative and mutable. The blob emergency sidecar is non-authoritative and
immutable. They intentionally have different purposes:

- the manifest drives FILESTORE, QUERY, SCHEDULER, ANALYSIS, SOURCE, and INTERFACE behavior;
- the emergency sidecar is for human disaster recovery only and must not be used for normal reads,
  indexing, scheduling, or analysis.

The manifest may be split from other authoritative annotations, such as `analysis.json.zst` and
`overlays.json.zst`, to keep updates narrow. These annotations are still FILESTORE authority and are
part of normal reconstruction.

Minimum manifest fields:

```json
{
  "schema_version": 1,
  "digest": "blake3-hex",
  "object_id": "blake3-hex",
  "identity_strategy": "file_blake3",
  "media_type": "application/pdf",
  "size": 12345,
  "compression": "zstd|identity",
  "created_at": "timestamp",
  "updated_at": "timestamp",
  "content_roles": ["source", "attachment", "analysis_output"],
  "facets": [{"kind": "file", "version": 1, "metadata": {"display_name": "Quarterly Statement.pdf"}}],
  "titles": [{"value": "Quarterly Statement", "source": "gmail", "confidence": 1.0}],
  "timestamps": {"created": "timestamp", "modified": "timestamp", "observed": "timestamp"},
  "provenance": [],
  "relationships": [],
  "compound": {"is_compound": false, "parts": []},
  "analysis": {},
  "graph": [],
  "keywords": [],
  "embeddings": [],
  "overlays": {}
}
```

Rules:

- bytes are immutable; manifests are mutable but must be written atomically;
- blob emergency sidecars are immutable and must be written atomically with blob creation;
- writes to an existing object are no-ops for `blob.zstd` and `recovery.json`; they only merge
  mutable authoritative annotations such as `manifest.json.zst`, `analysis.json.zst`, and
  `overlays.json.zst`;
- normal processes must ignore emergency sidecars and read authoritative manifests instead;
- authoritative annotations live in the object directory and are the only normal reconstruction
  source;
- every object has an immutable `object_id`, `identity_strategy`, and derived `digest`;
- dedupe happens before manifest merge and is based on FILESTORE's canonical identity/digest rules;
- byte-bearing file objects default to `identity_strategy = file_blake3`, where `object_id` is the
  uncompressed BLAKE3 payload hash and `digest` is the same value;
- compound objects use `identity_strategy = compound_stable_id`, where `object_id` is selected by
  the compound facet/source rule and `digest = BLAKE3("compound:{object_id}")`;
- manifest updates are merge operations with deterministic conflict handling;
- generated large outputs are CAS objects referenced from `analysis`;
- human-authored overlays live in the manifest, not in QUERY;
- every object must have at least one facet;
- source IDs are provenance fields, not primary keys or facets;
- compound objects list contained part digests and typed part roles;
- subobjects may have facets that the parent does not have;
- every analyzer output includes analyzer name, version, input digests, output digests, status,
  timestamps, and error metadata when applicable.

### Facet

Facets describe the domain semantics available for an object. They are used by SOURCE, FILESTORE,
QUERY, INTERFACE, and application services. ANALYSIS does not branch on facets; it branches on object
bytes, media type, content role, part role, and declared analyzer input requirements.

Required fields:

- `kind`: stable facet identifier such as `mail_message`, `email_part`, `file`, `phone`,
  `container`, or
  `analysis_output`;
- `version`: facet schema version;
- `metadata`: facet-specific metadata that survives QUERY rebuilds.

Facet rules:

- `mail_message` facet metadata can include subject, sender, recipients, thread keys, labels,
  mailbox, message dates, and message-level source action hints;
- `email_part` facet metadata can include part kind, header/body role, MIME structure hints, and
  RFC822-derived identifiers;
- `file` facet metadata can include display name, extension, MIME expectations, source path, and
  file timestamps;
- `phone` facet metadata can include participants, direction, number/contact identifiers, call/SMS
  timestamps, and duration;
- `webpage` facet metadata can include canonical URL, fetch URL, HTTP status, capture timestamp,
  title, site, and content fingerprints;
- `song` facet metadata can include title, artist, ISRC/MBID, track number, duration, and preferred
  playback variant;
- `album` facet metadata can include title, artist, release identifiers, release date, label, and
  ordered disc/track layout;
- `container` facet metadata can include display ordering and grouping semantics;
- a single object can expose multiple facets, for example an email attachment can be both `file` and
  `analysis_output` after FILESTORE records generated OCR text.

### Provenance Record

Provenance records answer "where did this object come from?" without forcing the object into a
source-specific schema.

Required fields:

- `source_kind`: `gmail`, `drive`, `ringme`, `filesystem`, `manual`, etc.;
- `source_name`: configured adapter or collector name;
- `external_id`: source-native ID when available;
- `external_version`: revision/history/version marker when available;
- `observed_at`;
- `capabilities_seen`: optional source capability snapshot;
- `metadata`: source-native metadata that should survive rebuilds.

### Compound Object

Compound objects are manifests whose purpose includes grouping related content objects into a
domain-level unit. They are actual FILESTORE objects. They are never virtual-only records and never
only QUERY rows; they have their own digest, manifest, facets, relationships, overlays, and analysis
state.

Compound objects do not use a payload hash as identity. They must be assigned a stable immutable
`object_id` at creation time, and that `object_id` must be specified by the facet/source strategy
before ingestion. FILESTORE derives the compound storage digest as `BLAKE3("compound:{object_id}")`.
The object ID must be unique for the conceptual thing, deterministic enough for idempotent reingest,
and strong enough to support dedupe.

Initial identity strategies:

- `mail_message`: `object_id = mail_message:{normalized Message-ID}` when RFC822 Message-ID exists;
  fall back only to a documented source-stable message identity when Message-ID is genuinely absent;
- `file`: `object_id = {uncompressed BLAKE3}` and `digest = {uncompressed BLAKE3}`;
- `webpage`: reserve a strategy such as canonical URL plus capture timestamp or content fingerprint;
- `song`: reserve a future strategy such as songprint/acoustic fingerprint plus release metadata;
- `album`: reserve a future strategy based on release identifiers or ordered song object IDs.

If a compound object has no natural source bytes, its emergency blob payload is a canonical JSON
envelope that records the `object_id`, identity strategy, source provenance, and ordered part
digests. That keeps compound objects addressable through the same FILESTORE APIs as byte-bearing
objects while preserving stable dedupe semantics.

Required fields inside `compound.parts`:

- `digest`: contained object digest;
- `role`: `metadata`, `body`, `attachment`, `raw`, `export`, `thumbnail`, `analysis_output`, etc.;
- `order`: stable display/order hint when relevant;
- `required`: whether the parent is incomplete without the part;
- `metadata`: role-specific metadata.

Common compound part roles:

- `rfc822_headers`;
- `email_body`;
- `attachment`;
- `gmail_data`;
- `mime_structure`;
- `derived_text`;
- `raw_html`;
- `mhtml_archive`;
- `pdf_render`;
- `screenshot`;
- `audio_mp3`;
- `audio_flac`;
- `cover_art`;
- `track`;
- `disc`;

FILESTORE owns compound merge behavior. When a subobject receives analysis output, FILESTORE can
materialize a parent-level structure and metadata view that references the subobject output without
copying or flattening away the subobject identity. That supports queries such as:

- `get metadata from object X21`;
- `get structure from object X21`;
- `X21 { rfc822_headers: X33, email_body: X34, attachments: [X35, X36], gmail_data: X37 }`.

QUERY projects those structure views for fast lookup, but FILESTORE remains the authority.

Example Gmail shape:

- parent compound object: `mail_message` + `container` facets;
- Gmail API metadata object: `email_metadata` or `analysis_output` role;
- RFC822 headers object: `email_part` role with header bytes or normalized header JSON;
- stripped message text object: `email_part` and/or `file` facet depending on representation;
- MIME structure object: `email_part` or `analysis_output` role with MIME tree metadata only, no
  embedded body or attachment payload bytes;
- each attachment object: `file` facet, possibly with source-specific provenance and later
  `analysis_output` relationships.

Example shared-attachment shape:

- message A compound object: unique digest, `mail_message` + `container` facets;
- message B compound object: unique digest, `mail_message` + `container` facets;
- two Gmail metadata subobjects: unique per message because the source metadata differs;
- two RFC822/header subobjects: unique per message when headers differ;
- two stripped body subobjects: unique per message when body text differs;
- one attachment file object: shared digest when attachment bytes are identical;
- two compound part entries and relationships point from message A and message B to the same
  attachment digest.

Example webpage shape:

- parent compound object: `webpage` + `container` facets;
- raw HTML object: `file` facet, `raw_html` role, captured by curl or a browser fetcher;
- MHTML archive object: `file` facet, `mhtml_archive` role, preserving page resources;
- PDF render object: `file` facet, `pdf_render` role, preserving a printable/rendered view;
- screenshot object: `file` facet, `screenshot` role, optional visual capture;
- extracted text object: `analysis_output` role, produced by ANALYSIS from one or more variants.

Example song shape:

- parent compound object: `song` + `container` facets;
- MP3 object: `file` facet, `audio_mp3` role;
- hi-res FLAC object: `file` facet, `audio_flac` role;
- cover-art object: `file` facet, `cover_art` role when track-specific art exists;
- metadata object: `analysis_output` or `file` role depending on whether it is extracted tags,
  sidecar JSON, or source-native metadata.

Example album shape:

- parent compound object: `album` + `container` facets;
- song compounds: `track` roles, ordered by disc/track number;
- album cover object: `file` facet, `cover_art` role;
- booklet/PDF object: `file` facet, optional `attachment` or `pdf_render` role;
- album-level metadata object: release identifiers, artist, label, dates, and source provenance.

Compound objects can contain other compound objects. An album contains song objects; each song
contains one or more file variants. FILESTORE owns that nesting and dedupe: if two albums reference
the same song digest, or two songs share the same cover-art bytes, the shared object remains one
FILESTORE object with multiple relationships.

### Relationship

Relationships are typed edges between content objects or between content objects and logical nodes.
Examples:

- `contains`: Gmail message contains attachment digest;
- `part_of`: attachment/body/header object belongs to a compound parent;
- `export_of`: Drive PDF export is derived from Drive document object;
- `revision_of`: object is a newer version of another object;
- `analysis_of`: generated text is analysis output for a binary object;
- `thread_member`: message belongs to a logical conversation node;
- `mentions`: object mentions entity node.

Relationships are also projected into QUERY graph tables.

### Analyzer Spec

Each analyzer declares enough metadata for SCHEDULER to derive missing or stale work:

- `name`;
- `version`;
- accepted media types and content roles;
- required inputs;
- output manifest sections;
- dependency analyzers;
- priority hint;
- idempotency key formula;
- deterministic/non-deterministic flag;
- runtime: Go process, Python worker, or external command wrapper.

### Analysis Job

RabbitMQ jobs should be small pointers, not payload dumps:

- `job_id`;
- `idempotency_key`;
- `target_digest`;
- `analyzer`;
- `analyzer_version`;
- `priority_class`;
- `requested_by`: scheduler, interface, source, operator;
- `reason`;
- `attempt`;
- `trace_id`;
- optional `deadline`.

Priority classes:

1. `interactive`: user asked for data and indexing/hydration is blocking usefulness;
2. `forced`: operator requested immediate reindex/reanalysis;
3. `fresh_ingest`: newly ingested content;
4. `repair`: missing/corrupt/stale outputs;
5. `background`: broad rebuilds and low-urgency enrichment.

### Source Event

SOURCE adapters and push collectors emit normalized source events:

- `source_kind`;
- `source_name`;
- `event_kind`: ingest, update, delete, tombstone, hydrate, action_result;
- `external_id`;
- `external_version`: source-native version, revision, history marker, segment generation, or
  equivalent stable change token when available;
- `cursor`;
- `content`: one or more byte streams, paths, or already-known digests;
- `metadata`;
- `relationships`;
- `requested_analysis`: optional analyzer hints.

## Component Responsibilities

### FILESTORE

Build first. Everything depends on it.

Required behavior:

- put/get CAS objects by BLAKE3 digest;
- lookup an exact source identity/version to return an existing digest before payload streaming;
- acquire and release source ingest claims so concurrent SOURCE workers do not stream the same
  source object at the same time;
- attach provenance to an existing digest without transferring payload bytes when a trusted
  source/version lookup proves it is already known;
- assign immutable object IDs and identity strategies before object creation;
- create one object directory per digest;
- write every object with `blob.zstd` plus immutable emergency `recovery.json` sidecar;
- treat existing-object writes as metadata merges only, never as blob or recovery sidecar repairs;
- own dedupe for byte-bearing objects and canonical compound envelopes;
- optional zstd compression by media type;
- annotation read, atomic write, and merge for `manifest.json.zst`, `analysis.json.zst`,
  `overlays.json.zst`, and future annotation files;
- manifest schema validation;
- compound structure view generation from part roles and subobject manifests;
- parent manifest refresh when subobject analysis changes;
- integrity verification for bytes and referenced output digests;
- integrity verification for emergency sidecar hashes without depending on sidecars for normal
  operation;
- list manifests by updated time and by dirty flags;
- walk object directories for reconstruction and verification;
- record human overlays durably;
- provide a repair report for missing bytes, corrupt bytes, missing manifests, and dangling refs.

Implementation notes:

- keep writes crash-safe: temp file, fsync, atomic rename, directory fsync;
- for network byte ingest, SOURCE streams directly to FILESTORE and FILESTORE writes payload bytes
  exactly once while hashing; hidden in-progress paths inside the FILESTORE root are the eventual
  object write, not source-side staging or a second payload copy;
- create blob and emergency sidecar as an atomic object-write unit; if either file is missing, the
  object is incomplete and must be reported by verification;
- write annotation files atomically and independently so analysis/overlay updates do not rewrite
  blob data;
- never load emergency sidecars in normal FILESTORE reads, QUERY projection, SCHEDULER scans, or
  ANALYSIS workers;
- keep dedupe centralized: callers never predeclare object uniqueness or create alternate IDs for
  identical bytes;
- require SOURCE adapters to perform lookup -> source-ingest-claim -> stream -> commit/release for
  any source object with a stable external identity/version;
- reject compound creation when the caller cannot provide a valid stable object ID for the selected
  facet/source strategy;
- avoid source-specific fields in core structs;
- expose read-only streaming APIs for large content;
- reserve FUSE-friendly path organization, but do not implement FUSE in the first slice.

### QUERY

QUERY v1 is PostgreSQL, fully scoped behind interfaces.

Projection tables should be greenfield and object-centric:

- objects;
- object_facets;
- object_provenance;
- object_relationships;
- object_compound_parts;
- object_analysis;
- object_graph_edges;
- object_keywords;
- object_embeddings;
- object_overlays;
- source_cursors;
- summaries;
- query_projection_state.

Capabilities:

- full rebuild by walking FILESTORE object directories and reading authoritative annotations;
- incremental upsert from changed authoritative annotations, exposed by
  `gmeow-admin query project-changed --since <RFC3339>`;
- text search over extracted/source text;
- vector search via pgvector;
- graph traversal and analytics over object relationships and extracted graph facts;
- metadata and structure queries for compound objects;
- source/provenance filters;
- facet filters for domain-specific views such as mail_message, email_part, webpage, song, album,
  file, phone, container, and
  analysis_output;
- category, visibility, analyzer status, media type, time, relationship, and compound-part filters;
- operational queries for stale, failed, dirty, and unindexed objects.

Design constraint: PostgreSQL-specific SQL must not leak outside QUERY packages. All QUERY SQL must
use parameterized `pgx` calls or generated/query-builder code that preserves parameter binding;
string-formatted SQL is not allowed.

### SCHEDULER

SCHEDULER owns work discovery and queue policy.

Required behavior:

- scan FILESTORE object directories for new/changed annotations;
- compare annotations against analyzer specs;
- identify missing, stale, failed, or invalid outputs;
- enqueue RabbitMQ jobs with deterministic idempotency keys;
- raise priority for interactive and forced work;
- route retryable failures with backoff;
- route exhausted failures to dead-letter queues;
- periodically requeue self-healing repair work;
- publish QUERY refresh requests after FILESTORE changes.

RabbitMQ topology:

- one exchange for analysis work;
- queues by priority or a priority queue with max priority configured;
- production RabbitMQ queue names are prefixed with `gmeow.`;
- integration-test RabbitMQ queue names are prefixed with `gmeow.test.`;
- production broker URLs target the RabbitMQ `gmeow` vhost;
- integration tests target the RabbitMQ `gmeow-test` vhost;
- retry queues with TTL/dead-letter routing;
- dead-letter queue for inspection;
- separate lightweight queue for QUERY projection refresh events if needed.

Valkey can remain useful for transient rate-limit state, locks, or metrics, but RabbitMQ is the
authoritative analysis work broker.

### ANALYSIS

ANALYSIS workers enrich FILESTORE only. QUERY observes the result through projection.

ANALYSIS works with objects, not facets. An analyzer receives a target digest plus enough object
context to read bytes, media type, content role, part role, prior analyzer outputs, and declared
dependencies. It does not need to know whether the object participates in an email, phone record,
Drive export, or future facet. FILESTORE is responsible for merging object-level outputs back into
compound parents and facet-specific metadata views.

Initial analyzer families:

- content identification and metadata extraction;
- text extraction for HTML, plain text, PDF, office documents, archives, and OCR-capable images;
- RFC822/header extraction for identities, graph nodes, temporal data, and location hints;
- attachment/document analysis wrappers around existing command-line tools;
- embedding generation through the existing OpenAI-compatible endpoint;
- graph fact extraction;
- NER through Python/spaCy;
- categorization through Python/sklearn;
- summaries and centroid metadata.

Rules:

- analyzers are idempotent;
- analyzers do not branch on facets;
- analyzer output is keyed by analyzer name + version + target digest + input digest set;
- reruns update the manifest deterministically;
- non-deterministic analyzers must record model/tool versions and timestamps clearly;
- workers ack jobs only after FILESTORE writes are durable;
- QUERY refresh can lag without losing data.

### SOURCE

SOURCE adapters are producers and optional live capability providers.

Adapter interface:

- `Capabilities()`;
- `SupportedFacets()` for search/hydration/action registration;
- `Ingest()` or event stream handling;
- `Hydrate()` when source can retrieve missing bytes;
- `LiveSearch()` when source can search outside FILESTORE;
- `LiveRetrieve()` when source can fetch source-native records on demand;
- `Export()` for Drive-like systems;
- `Action()` for Gmail archive/delete/label-style operations;
- `Cursor()` for change tracking/backfill.

Initial source strategy:

- Gmail adapter: emits real compound FILESTORE objects with `mail_message` + `container` facets and
  metadata/header/body/MIME-structure/attachment subobjects; supports rich live search/retrieve,
  history cursor, backfill, actions, and hydration;
- Drive adapter: emits `file` and optionally `container` objects for files, folders, revisions, and
  exports; ingest/export/change tracking can land as a later slice;
- ringme/push adapter: emits `phone`, `file`, or other declared facets through a file spool or API
  endpoint that writes normalized source events;
- local filesystem importer: emits `file` and `container` facets for arbitrary FILESTORE population.

Adapters must not call QUERY directly except through application services. Their durable output is
FILESTORE content and provenance.

### INTERFACE

INTERFACE exposes shared services; it does not define the domain.

Facet-specific tools can be pre-facetted search orchestrators. They do not own storage or query
logic, but they can look up all registered search backends that support a requested facet, fan out
queries in parallel, fuse results, apply endpoint-specific response shaping, and fetch missing
details from FILESTORE.

First implementation order:

1. MCP tools for object search, retrieval, provenance, analysis status, graph exploration, semantic
   search, forced analysis, facet inspection, compound-object expansion, and source actions.
2. REST endpoints for the same core operations and operational dashboards.
3. CLI for admin operations: rebuild QUERY, verify FILESTORE, rescan scheduler, requeue dead letters,
   force analysis, inspect manifests.
4. IMAP projection over objects with the `mail_message` facet after Gmail ingestion is stable.
5. FUSE-style filesystem view over `file` and `container` facets later; reserve the data model now
   but do not build it first.

MCP is first in sequence only. It has no architectural primacy.

## Recommended Go Libraries And Services

| Concern | Recommendation |
|---|---|
| HTTP router | `go-chi/chi/v5` |
| MCP | official `modelcontextprotocol/go-sdk` if server APIs remain suitable; otherwise `mark3labs/mcp-go` |
| CLI | `spf13/cobra` |
| Config | `knadh/koanf` or `spf13/viper`; prefer one config loader across all binaries |
| Logging | `log/slog` |
| PostgreSQL | `jackc/pgx/v5` + `pgxpool` |
| Migrations | `pressly/goose/v3` for new greenfield QUERY schema |
| Vector search | pgvector |
| Broker | RabbitMQ |
| RabbitMQ client | `rabbitmq/amqp091-go` |
| Service RPC | `google.golang.org/grpc` + generated typed protobuf |
| BLAKE3 | `lukechampine/blake3` |
| Zstd | `klauspost/compress/zstd` |
| Retry/backoff | `cenkalti/backoff/v4` |
| Gmail | `google.golang.org/api/gmail/v1` |
| Drive | `google.golang.org/api/drive/v3` |
| OAuth2 | `golang.org/x/oauth2/google` |
| Graph algorithms | `gonum.org/v1/gonum/graph` where in-process analytics are useful |
| Python worker IPC | RabbitMQ jobs + FILESTORE annotations, not synchronous RPC |

## Module And Filesystem Layout

Keep two layouts distinct:

- source repository layout: code, contracts, migrations, tests, and documentation;
- runtime FILESTORE layout: object directories, blobs, recovery files, annotations, reports, and
  operational scratch space.

### Source Repository Layout

```text
gmeow/
  cmd/
    gmeow/              # main service: INTERFACE + SOURCE loops + SCHEDULER/projector wiring
    gmeow-worker/       # Go ANALYSIS workers
    gmeow-admin/        # CLI/admin: verify, rebuild, requeue, inspect
  internal/
    appsvc/             # use-case orchestration: search, retrieve, hydrate, analyze
    filestore/          # object dirs, blob IO, annotations, identity, dedupe
    query/              # QUERY interfaces
      postgres/         # PostgreSQL implementation hidden behind query interfaces
    scheduler/          # work derivation + RabbitMQ publishing
    analysis/           # analyzer contracts + Go analyzer runtime
      analyzers/
    source/             # source adapter contracts and implementations
      gmail/
      drive/
      spool/
      filesystem/
    interface/
      mcp/
      rest/
      imap/
    contracts/          # manifest, annotations, jobs, source events, search DTOs
    config/
    observability/
  migrations/
    query/              # goose migrations for QUERY only
  testdata/
    filestore/
    source_events/
    annotations/
  python/
    gmeow_intel/
      worker.py
      analyzers/
        ner.py
        categories.py
      contracts/
  docs/
    architecture/
```

Ownership rules:

- `filestore` owns object IDs, digest derivation, dedupe, object directory layout, annotation IO,
  recovery sidecars, and compound structure expansion.
- `query` owns projection rebuild and search APIs. `internal/query/postgres` must not leak upward.
- `scheduler` owns work derivation, priority, retries, dead letters, and RabbitMQ publishing.
- `analysis` owns analyzer specs, worker execution, and Go analyzer implementations.
- `source/*` owns external systems, source events, live search, hydration, and source actions.
- `appsvc` owns workflows such as `mail_search`, generic search, forced analysis, and result fusion.
- `interface/*` adapts protocol shape to app services only.
- `contracts` should stay boring and stable: manifests, annotations, jobs, source events, search
  requests/results, structure responses, and shared JSON schemas.

Python package shape:

- `gmeow_intel.worker`: RabbitMQ worker entrypoint;
- `gmeow_intel.analyzers.ner`;
- `gmeow_intel.analyzers.categories`;
- optional `gmeow_intel.analyzers.chunking` if semchunk stays Python-side;
- `gmeow_intel.contracts` generated from or validated against the same schemas used by Go.

The Python workers should share the same annotation and job contracts as Go workers.

## Distribution And PyPI Scope

The existing PyPI publishing should be rescoped. In the greenfield architecture, PyPI is only for
the Python ANALYSIS package, not the whole gmeow system.

### PyPI Package

Publish a package such as `gmeow-intel` from `python/`.

Contents:

- Python RabbitMQ worker entrypoint;
- spaCy NER analyzer;
- sklearn categorization analyzer;
- optional semchunk/chunking helper;
- generated/shared contracts for jobs and FILESTORE annotations;
- test fixtures for Python analyzers.

Not included:

- Go core service;
- MCP/REST/IMAP interface servers;
- SOURCE adapters implemented in Go;
- QUERY migrations and PostgreSQL projector runtime;
- FILESTORE implementation;
- RabbitMQ/PostgreSQL orchestration;
- old Python FastAPI/Gmail server package.

The Python package exists so Python-native analysis dependencies can be installed and upgraded with
normal Python tooling. It should communicate with the rest of gmeow through RabbitMQ jobs and
FILESTORE annotations only.

### Go Distribution

Distribute Go components outside PyPI:

- `gmeow`: main service binary;
- `gmeow-worker`: Go ANALYSIS worker binary;
- `gmeow-admin`: admin CLI;
- container images for service deployments;
- GitHub release artifacts for OS/architecture binaries;
- optional install scripts or systemd unit templates.

Do not make PyPI the authoritative distribution path for the local data platform. A future
`pip install gmeow` convenience wrapper may exist only if it downloads or locates Go binaries, but
that should not be v1.

### Release Workflow Implication

The current Python package/release workflow should be replaced or narrowed:

- stop publishing the old `gmeow` Python application package;
- add a `python/pyproject.toml` for `gmeow-intel`;
- make PyPI release jobs build only `python/`;
- move Go binary release to Go build/release workflows;
- keep shared JSON schemas/contracts versioned so Go and Python workers can reject incompatible job
  or annotation formats.

## Configuration Model

The greenfield system should replace the current flat `config.toml` with a component-shaped
`gmeow.toml`. Config declares which components exist and how they connect. It should not hold
runtime cursors, source checkpoints, priority state, user category policy, or analysis results; those
belong in FILESTORE annotations, QUERY projection state, or SCHEDULER state.

Config rules:

- one shared Go parser owns 100% of config parsing, defaults, validation, and environment/file
  secret resolution;
- all Go binaries use the same parser and typed config model;
- Python ANALYSIS workers do not read `gmeow.toml`;
- ANALYSIS receives every needed setting through RabbitMQ job payloads, analyzer specs, environment
  variables for process-level concerns, or resolved worker startup payloads;
- SOPS is a first-class startup dependency, not an optional integration;
- startup requires a SOPS age identity from `GMEOW_SOPS_UNLOCK_KEY`, falling back to
  `~/.config/gmeow/key.txt`; if neither exists, every binary fails before starting components;
- avoid the current secondary-config indirection: `gmeow.toml` must not contain a
  `secrets.file`/`config.file` pointer to another config file;
- config file selection belongs to process startup through CLI flags, environment variables, or
  fixed defaults, not to the config data model itself;
- encrypt only minimal leaf secrets as SOPS JSON envelopes inside `[secrets]`, not whole
  operational structures: for example, PostgreSQL host/port/database/user remain visible while
  only the password is encrypted;
- password leaves have fixed names under `[secrets]`; connection URLs, hosts, users, ports, and
  vhosts stay visible operational config and must not be encrypted as opaque DSNs;
- the shared Go parser decrypts referenced SOPS leaf envelopes, resolves declared secret fields into
  the typed config model, and dispatches resolved values to components;
- repeated components use arrays, such as `[[sources]]`, `[[search.backends]]`, and
  `[[analysis.analyzers]]`;
- source configs declare facets and capabilities explicitly;
- search backends are registered declaratively, then resolved by application services;
- config schema/version is explicit so old binaries can reject incompatible config.

Draft:

```toml
[system]
config_version = 1
instance_id = "local"
data_dir = "data"
log_level = "info"

[filestore]
root = "data/filestore"
compression = "zstd"
zstd_level = 6

[postgres]
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
sslmode = "disable"
migrations = "migrations/query"
max_conns = 10

[rabbitmq]
host = "127.0.0.1"
port = 5672
vhost = "gmeow"
user = "gmeow"

[scheduler]
scan_interval = "30s"
stale_after = "15m"

[scheduler.priorities]
interactive = 100
forced = 90
fresh_ingest = 70
repair = 50
background = 10

[analysis]
worker_concurrency = 4

[analysis.embeddings]
endpoint = "http://127.0.0.1:8090/v1/embeddings"
model = "nomic-embed-text-v1.5"
batch_size = 32

[[analysis.analyzers]]
name = "text.extract"
runtime = "go"

[[analysis.analyzers]]
name = "ner.spacy"
runtime = "python"

[[analysis.analyzers]]
name = "categories.sklearn"
runtime = "python"

[interface.mcp]
listen = "127.0.0.1:8765"

[interface.rest]
listen = "127.0.0.1:8766"

[interface.imap]
listen = "127.0.0.1:1143"
username = "gmeow"
facet = "mail_message"

[[sources]]
name = "primary-gmail"
kind = "gmail"
facets = ["mail_message"]
capabilities = ["ingest", "hydrate", "live_search", "live_retrieve", "action", "cursor", "backfill"]

[sources.gmail.auth]
mode = "service_account"
subject = "user@example.com"
service_account_json_secret = "gmail.primary.service_account_json"

[[sources]]
name = "local-spool"
kind = "spool"
path = "data/spool"
facets = ["file", "phone", "webpage"]
capabilities = ["ingest"]

[[search.backends]]
name = "query"
kind = "query"
facets = ["*"]

[[search.backends]]
name = "gmail-live"
kind = "source"
source = "primary-gmail"
facets = ["mail_message"]
```

Startup config resolution should be deterministic:

1. select the config source from CLI flag, `GMEOW_CONFIG`, or the default path;
2. require the SOPS age identity from `GMEOW_SOPS_UNLOCK_KEY` or `~/.config/gmeow/key.txt`;
3. read TOML-shaped `gmeow.toml` and reject unknown schema versions or opaque whole-file SOPS blobs;
4. decrypt referenced `[secrets]` leaf envelopes through SOPS;
5. resolve named secret references inside the shared parser;
6. validate the complete typed config before any component starts;
7. dispatch resolved config to Go components and resolved worker settings/job specs to ANALYSIS.

Startup validation is mandatory and cannot be disabled. Deep FILESTORE verification belongs to an
explicit CLI command, not startup config. Recovery sidecars are mandatory FILESTORE output, not a
config toggle.

Secret management should be part of `gmeow-admin` so operators do not hand-edit encrypted values:

- `gmeow-admin config secret set <name>` reads a value from stdin or an interactive prompt and
  writes the encrypted leaf value back through the shared SOPS config writer;
- `gmeow-admin config secret unset <name>` removes an encrypted leaf after validating no configured
  component still references it;
- `gmeow-admin config secret list` prints secret names, reference counts, and last update metadata,
  but never decrypted values;
- `gmeow-admin config validate` decrypts, resolves, and validates the effective config using the
  same parser as every service binary;
- secret writes must be atomic and preserve comments/formatting where the selected config backend
  supports it.

Destructive or broad admin commands, including QUERY rebuilds, repair rewrites, test resets, and
secret deletion, must validate the active instance before proceeding and refuse production-like
targets unless an explicit, audited operator workflow permits them.

Parser output should include resolved, typed structures for Go components and a smaller resolved
worker view for Python ANALYSIS. Python workers should validate the received job/spec payloads
against shared contracts but must not open or interpret config files, SOPS files, or unlock-key
files.

Avoid placing long-lived user policy in config. Gmail priority searches, category rules, aliases,
visibility choices, saved searches, and similar user policy should be FILESTORE overlays or
operator-managed records so they survive config replacement and can be changed through interfaces.

### Runtime FILESTORE Layout

```text
data/
  filestore/
    objects/
      blake3/
        ab/
          cd/
            <digest>/
              blob.zstd
              recovery.json
              manifest.json.zst
              analysis.json.zst
              overlays.json.zst
              graph.json.zst
              embeddings.json.zst
              summary.json.zst
              ...
    tmp/
    locks/
    verify-reports/
```

Runtime layout rules:

- every object key has exactly one object directory;
- `blob.zstd` and `recovery.json` are immutable after creation;
- once `blob.zstd` exists, normal writes must not create, replace, repair, or refresh
  `recovery.json`;
- authoritative annotations are compressed `*.json.zst` files in the object directory;
- annotation files are independently and atomically written;
- QUERY rebuilds by walking object directories and reading authoritative annotations;
- normal processes ignore `recovery.json`;
- `tmp/` is only for atomic writes and crash recovery;
- `locks/` is optional and only for local coordination that cannot live in RabbitMQ/PostgreSQL;
- `verify-reports/` stores operator-facing integrity and repair reports, not authoritative state.

## Implementation Plan

### Phase 0 - Repository Split And Skeleton

Goal: create the greenfield Go and Python worker skeletons with interfaces before behavior.

Deliverables:

- Go module layout and binaries;
- `python/pyproject.toml` for `gmeow-intel` with PyPI scope limited to Python ANALYSIS;
- shared config parser/model for FILESTORE root, structured PostgreSQL/RabbitMQ connection settings,
  worker settings, interfaces, sources, analyzers, secret references, and search backend registry;
- first-class SOPS unlock/decrypt support with required `GMEOW_SOPS_UNLOCK_KEY` or
  `~/.config/gmeow/key.txt` startup validation and no secondary config-file pointer inside
  `gmeow.toml`;
- minimal encrypted-leaf secret model and `gmeow-admin config secret` commands for add/update/list
  operations;
- resolved ANALYSIS worker config payloads so Python workers never read config files directly;
- interface definitions for FILESTORE, QUERY, SCHEDULER, ANALYSIS, SOURCE, and application services;
- structured logging and trace IDs;
- development docker/core-data assumptions documented;
- release workflow split: Go binaries/containers outside PyPI, Python ANALYSIS package on PyPI;
- smoke test that starts real in-repo FILESTORE, QUERY, and analyzer implementations with local
  test storage.

Exit criteria:

- packages compile;
- canonical local quality gate is documented for Go and Python work and includes formatting, lint,
  type/static analysis, and tests;
- dependency directions are enforced by package boundaries;
- no component imports a concrete implementation it should not know.

### Phase 1 - FILESTORE

Goal: implement authoritative CAS and manifest storage.

Deliverables:

- BLAKE3 CAS put/get/stream;
- object directory layout under `objects/blake3/aa/bb/<digest>/`;
- object ID and identity-strategy enforcement for file and compound objects;
- immutable emergency sidecar writer for each blob;
- media-type-aware zstd compression;
- manifest schema structs and validation;
- atomic annotation write and merge;
- compound structure query APIs such as `GetStructure(digest)`;
- parent metadata refresh from subobject analysis outputs;
- content/facet/provenance/relationship/compound/overlay update APIs;
- integrity checker and repair report;
- test fixtures for text, JSON, PDF-like binary, and generated analysis outputs.

Exit criteria:

- repeated writes are idempotent;
- repeated compound creation with the same object ID resolves to the same digest;
- compound creation without a valid object ID is rejected;
- corrupt bytes are detected;
- missing or mismatched emergency sidecars are reported without blocking normal manifest-based use;
- manifest updates survive process interruption simulations where practical;
- human overlays are durable without QUERY.

### Phase 2 - QUERY Projection

Goal: build a clean PostgreSQL projection that can be wiped and rebuilt.

Deliverables:

- goose migrations for object-centric QUERY schema;
- projector that walks FILESTORE object directories, reads annotation files, and upserts projection
  rows;
- full rebuild command;
- incremental projection from changed annotation files;
- text search, vector placeholder paths, graph edge projection, facet filters, compound expansion,
  metadata/structure queries, and analyzer status queries;
- package-local in-memory QUERY implementation for tests that do not need PostgreSQL, plus
  PostgreSQL-backed tests for QUERY behavior and app-service orchestration.

Exit criteria:

- deleting PostgreSQL and rebuilding from FILESTORE restores search/retrieval metadata;
- no FILESTORE component imports PostgreSQL code;
- QUERY API can be tested without RabbitMQ or source adapters.

### Phase 3 - RabbitMQ SCHEDULER

Goal: make analysis work derivable, prioritized, retryable, and self-healing.

Deliverables:

- analyzer spec registry;
- stale/missing work detector;
- idempotency key generation;
- RabbitMQ exchange/queue setup;
- production RabbitMQ vhost `gmeow` and test vhost `gmeow-test`;
- production queue prefix `gmeow.` and test queue prefix `gmeow.test.`;
- priority classes;
- retry and dead-letter routing;
- forced reanalysis and interactive reprioritization;
- scheduler scan command and background loop.

Exit criteria:

- interactive jobs run before background jobs;
- duplicate scheduler scans do not create duplicate effective work;
- failed jobs retry with backoff and land in dead letter after exhaustion;
- analyzer version bump schedules only affected outputs.

### Phase 4 - ANALYSIS Workers

Goal: implement extensible analyzers that write results to FILESTORE.

Deliverables:

- Go worker runtime;
- Go-owned analyzer registry, RabbitMQ consume/ack, failure routing, spec-version checks, and
  durable FILESTORE annotation writes before ack through `FilestoreService` gRPC;
- external adapter support for Python/model/tool analyzers that cannot yet be replaced in Go without
  quality regression, configured explicitly with command, arguments, timeout, analyzer name, and
  analyzer version;
- `gmeow-intel` packaging and entrypoint only for quality-preserving external analyzer adapters;
- text extraction analyzer;
- RFC822/header analyzer;
- metadata extraction analyzer;
- embedding analyzer using configured endpoint and model;
- graph fact analyzer;
- NER analyzer, either equal-or-better Go-native or Python/model-backed through the external adapter;
- categorization analyzer, either equal-or-better Go-native or Python/model-backed through the
  external adapter;
- summary/centroid analyzer placeholder or first pass.

Exit criteria:

- every analyzer is idempotent by digest/spec version;
- workers ack only after durable FILESTORE writes;
- QUERY can lag and later catch up from FILESTORE annotations;
- Go owns scheduling, queue consumption, gRPC FILESTORE writes, and ack/nack decisions;
- ANALYSIS workers do not open FILESTORE directories directly outside tests;
- Python/model adapters share job and annotation contracts and fail closed when unregistered;
- Python/model analyzers without an explicit external adapter command fail at worker startup;
- Go-native replacements for Python-backed analyzers pass equal-or-better fixture gates before the
  Python implementation is retired.
- the configured analyzer set covers text, RFC822/header, metadata, graph facts, embeddings,
  summary, and explicit NER/category adapters unless Go parity has been proven.

### Phase 5 - SOURCE Adapters

Goal: feed FILESTORE from live and push-based sources.

Deliverables:

- normalized source event contract;
- local filesystem importer for easy test data;
- push/spool ingestor for ringme-style producers;
- Gmail adapter with ingest, hydrate, live search, live retrieve, cursor/backfill, and actions;
- Drive adapter design plus initial ingest/export/change tracking if included in this slice;
- source cursor storage in FILESTORE source-state annotations, projected into QUERY.

Exit criteria:

- push-only data appears in FILESTORE, schedules analysis, and becomes searchable after projection;
- Gmail live search can hydrate useful content and raise analysis priority;
- source capability checks prevent unsupported operations from leaking into INTERFACE.
- facet filters prevent endpoint-specific views from exposing inapplicable objects.

### Phase 6 - INTERFACE

Goal: expose rich interaction planes through shared services.

Deliverables:

- application services for search, retrieve, analyze, provenance, facets, compound expansion, graph,
  source actions, search backend registry, and ops;
- MCP tools over the service layer;
- REST endpoints over the same service layer;
- CLI/admin commands;
- IMAP projection for `mail_message` facet objects;
- operational status surfaces for FILESTORE, QUERY, SCHEDULER, ANALYSIS, SOURCE health.

Exit criteria:

- MCP and REST return equivalent results for shared operations;
- forcing analysis from an interface raises priority and eventually updates FILESTORE/QUERY;
- source-specific actions are gated by source capabilities;
- IMAP does not require Gmail to be the core data model and only sees the `mail_message` facet projection.

### Phase 7 - Hardening, Rebuilds, And Operations

Goal: prove robustness and self-healing.

Deliverables:

- full FILESTORE verify command;
- full QUERY rebuild command;
- scheduler repair scan;
- dead-letter inspection and requeue tools;
- backup/restore documentation for FILESTORE authority;
- metrics for queue depth, analysis latency, projection lag, corrupt objects, failed analyzers, and
  source errors;
- load tests for ingest, projection, and search;
- chaos tests for worker crash, broker restart, DB wipe, and corrupt object.

Exit criteria:

- QUERY wipe/rebuild is routine;
- worker crashes do not lose acknowledged work;
- corrupt/missing objects are reported and do not poison unrelated indexing;
- operators can inspect and repair failed analysis without editing database rows by hand.

## Use Case - `mail_search("bob@dob.com")`

`mail_search` is a pre-facetted MCP tool for the `mail_message` facet. It is not a direct PostgreSQL
query and not a direct Gmail call. It is a search orchestration path over registered backends.

Flow:

1. MCP receives `mail_search` with query text and response-shaping options such as `full_msg`,
   `summaries_only`, attachment inclusion, result limit, and freshness policy.
2. INTERFACE calls the shared search service with facet `mail_message`.
3. The search service consults the search backend registry and selects backends that support
   `mail_message`.
4. For this query, the selected backends are at least QUERY and Gmail.
5. The service issues backend searches in parallel and waits according to the tool deadline.
6. QUERY searches its PostgreSQL projection and returns whatever it currently knows from FILESTORE
   annotations: message compounds, header/body/entity matches, graph hits, snippets, categories,
   summaries, and analyzer status.
7. Gmail performs live Gmail search. For each returned Gmail message ID, Gmail asks FILESTORE whether
   the corresponding `mail_message` compound already exists.
8. On FILESTORE hit, Gmail returns the existing object digest and any available structure/metadata.
9. On FILESTORE miss, Gmail queues or performs hydration according to the search options:
   - if summaries are enough, it may return a live lightweight result and enqueue full hydration;
   - if `full_msg` or the API contract requires full content, it downloads the full message, submits
     the complete compound object and subobjects to FILESTORE, then returns the new digest when
     available;
   - new FILESTORE writes trigger SCHEDULER/ANALYSIS events and QUERY projection refresh.
10. The search service fuses QUERY and Gmail results by FILESTORE digest/object ID, de-duplicates
    live and indexed hits, ranks them, and applies endpoint response shaping.
11. If the response needs fields not supplied by the backends, the service asks FILESTORE for
    structure and annotations, such as headers, body, attachments, Gmail metadata, and analysis
    status.
12. MCP returns immediately with the best available fused result set. Some results may be complete,
    while freshly hydrated Gmail hits may include `analysis_pending` or `projection_pending` state.

Convergence behavior:

- FILESTORE is the write point for new Gmail content.
- ANALYSIS receives events from FILESTORE for new or changed objects and writes annotation updates
  back to FILESTORE.
- QUERY catches up by projecting changed annotations.
- Repeating the same `mail_search` later should converge from mixed live/indexed partial results to
  richer QUERY-backed results as hydration, analysis, and projection complete.

Ownership boundaries:

- `mail_search` owns orchestration, backend fan-out, result fusion, and response shaping.
- QUERY owns PostgreSQL projection search only.
- Gmail owns live Gmail search and hydration.
- FILESTORE owns object lookup, dedupe, writes, structure, and annotations.
- ANALYSIS owns enrichment after FILESTORE writes.
- SCHEDULER owns priority and retry policy for hydration-triggered analysis/projection work.

## Additional Use Cases

### Push-Only Ingest

1. A ringme-style collector or local producer submits a normalized source event with content,
   metadata, relationships, and a facet such as `phone` or `file`.
2. SOURCE validates the event and submits bytes or compound IDs to FILESTORE.
3. FILESTORE writes object directories, dedupes existing objects, updates authoritative
   annotations, and emits change events.
4. SCHEDULER derives analysis/projection work from changed annotations.
5. ANALYSIS enriches objects and writes `analysis.json.zst` updates.
6. QUERY projects changed annotations. The object becomes searchable without any live backend.

### Generic Object Search

1. User calls a broad tool such as `object_search("project apollo")`.
2. INTERFACE does not pre-filter to one facet unless requested.
3. The search service selects broad backends, usually QUERY first.
4. QUERY searches projected text, graph, vector, facets, provenance, summaries, and analyzer outputs.
5. INTERFACE fetches FILESTORE structure only for results that need expansion.
6. Results may include mail messages, files, webpages, phone records, songs, albums, and analysis
   outputs in one fused response.

### Facet-Filtered Retrieval

1. IMAP asks for folders/messages.
2. INTERFACE maps the request to the `mail_message` facet.
3. QUERY returns only projected `mail_message` compound objects.
4. FILESTORE supplies message structure and content parts as needed.
5. Objects without `mail_message` are invisible to IMAP even if they contain text, files, or matching
   entities.

### Compound Expansion

1. User asks `get_structure(X21)`.
2. FILESTORE reads `manifest.json.zst` and relevant annotations from object `X21`.
3. If `X21` is compound, FILESTORE returns role-mapped parts such as:
   `X21 { rfc822_headers: X33, email_body: X34, attachments: [X35, X36], gmail_data: X37 }`.
4. The caller can then request any part directly by digest.
5. QUERY may cache/project this structure, but FILESTORE remains authority.

### Forced Analysis

1. User requests reanalysis of object `X35`, either from MCP, REST, or CLI.
2. INTERFACE calls SCHEDULER with priority `forced`.
3. SCHEDULER creates deterministic jobs for the relevant analyzer specs and versions.
4. ANALYSIS reads object bytes and prior annotations, writes new `analysis.json.zst`, and acks only
   after FILESTORE writes are durable.
5. QUERY projection refresh follows asynchronously.

### QUERY Rebuild

1. PostgreSQL is empty, corrupt, or intentionally wiped.
2. Operator runs QUERY rebuild.
3. Rebuilder walks FILESTORE object directories under `objects/blake3/aa/bb/<digest>/`.
4. For each object, it reads authoritative annotations such as `manifest.json.zst`,
   `analysis.json.zst`, and `overlays.json.zst`.
5. It ignores `recovery.json`.
6. It repopulates QUERY tables, indexes, graph projections, semantic rows, summaries, and operational
   state.

### Live Gmail Full Hydration

1. User calls `mail_search(..., full_msg=true)` or retrieves a Gmail live hit requiring full content.
2. Gmail live search finds a Gmail message ID not present in FILESTORE.
3. Gmail downloads metadata, RFC822 headers, body parts, MIME structure, and required attachments
   without retaining a raw RFC822 duplicate when decomposed parts are stored.
4. Gmail submits the complete `mail_message` compound and subobjects to FILESTORE.
5. FILESTORE dedupes attachments/files, writes object directories, and emits analysis/projection
   events.
6. INTERFACE can return the hydrated result immediately if available, or return a pending state until
   hydration completes.

### Webpage Capture

1. SOURCE captures a webpage as raw HTML, optional MHTML archive, optional PDF render, optional
   screenshot, and source metadata.
2. FILESTORE creates one `webpage` + `container` compound object with parts for each representation.
3. ANALYSIS treats each part as an object: text extraction from HTML/PDF, OCR from screenshots, link
   extraction, entity extraction, and summaries.
4. FILESTORE merges subobject analysis into the webpage compound structure.
5. QUERY makes the page searchable by URL, title, extracted text, entities, graph links, and capture
   metadata.

### Song And Album Import

1. SOURCE imports audio files and album metadata.
2. FILESTORE creates `song` compounds for tracks, with MP3/FLAC/cover-art file parts as available.
3. FILESTORE creates an `album` compound containing ordered `song` compounds plus album art,
   booklet/PDF, and release metadata.
4. Duplicate audio or art bytes dedupe as shared file objects.
5. QUERY supports searches by song, album, artist, metadata, file format, and nested compound
   structure.

### Recovery From Blobs

1. Operator finds object directories but manifests or QUERY are damaged.
2. Normal system processes still ignore `recovery.json`.
3. Human reads `recovery.json` beside `blob.zstd` to identify media type, source hint, observed
   filename/title, sizes, hashes, compression, and writer version.
4. Operator can manually triage or rebuild enough context to reimport/recover data.

## Verification Matrix

| Scenario | Expected result |
|---|---|
| Same content ingested from two sources | One CAS object, multiple provenance records, no duplicate analysis work |
| Same file bytes ingested twice | Both ingests use the uncompressed BLAKE3 as `object_id` and `digest`, producing one file object |
| Two email messages share attachment bytes | Two compound email objects reference one shared `file` facet attachment object |
| Object ingested without a facet | Ingest is rejected or SOURCE/FILESTORE policy must assign at least one valid facet before commit |
| Compound object created | FILESTORE creates an actual object with its own digest, manifest, and facets; QUERY does not invent it |
| Compound created twice with same object ID | Both ingests resolve to `BLAKE3("compound:{object_id}")` and merge into one object |
| Compound created without object ID | FILESTORE rejects it before writing blob, sidecar, or manifest |
| Gmail message ingested | A real compound FILESTORE object with `mail_message` + `container` facets is created with metadata/header/body/MIME-structure/attachment subobjects and no raw RFC822 duplicate |
| Gmail attachment ingested | Attachment is a discrete object with a `file` facet and `part_of`/`contains` relationships |
| RFC822 header object analyzed | Analyzer outputs identities, graph nodes, temporal data, and location hints without reading facets |
| Message body object analyzed | Analyzer treats it as text content, not as an email-domain object |
| Compound structure requested | FILESTORE returns part-role mapping such as headers, body, attachments, and Gmail metadata digests |
| Webpage captured in multiple forms | One `webpage` compound object references raw HTML, MHTML, PDF render, screenshot, and extracted text objects |
| Song has multiple encodings | One `song` compound object references MP3 and FLAC file objects without duplicating track metadata |
| Album ingested | One `album` compound object contains ordered `song` compound objects plus album-level art and metadata |
| Push-only ringme object arrives | FILESTORE manifest appears, SCHEDULER enqueues analysis, QUERY projection makes it searchable |
| Generic object search runs | QUERY returns cross-facet results and INTERFACE expands only requested result structures from FILESTORE |
| IMAP lists objects | Only objects with the `mail_message` facet are visible |
| Forced analysis requested | SCHEDULER enqueues forced-priority analyzer jobs and QUERY catches up after `analysis.json.zst` changes |
| `mail_search("bob@dob.com")` runs | INTERFACE fans out to QUERY and Gmail, fuses results, fetches missing structure from FILESTORE, and returns pending states for fresh live hits |
| Gmail live search finds existing message | Gmail returns existing FILESTORE digest without rewriting the object |
| Gmail live search finds missing message | Gmail hydrates or queues hydration, submits full content to FILESTORE when needed, and later QUERY converges after analysis/projection |
| Interactive search hits unindexed object | SCHEDULER raises priority for needed analysis/projection |
| Analyzer version changes | Only stale outputs for that analyzer/version are rescheduled |
| PostgreSQL is wiped | Full QUERY rebuild from FILESTORE restores search, graph, semantic metadata, and overlays |
| QUERY rebuild runs | Rebuilder walks object directories and projects `manifest.json.zst`, `analysis.json.zst`, overlays, and other annotations |
| Recovery-only blob triage | Operator can identify a blob from `recovery.json` without relying on QUERY or authoritative annotations |
| PyPI release runs | Only `gmeow-intel` Python ANALYSIS package is built and published, not the Go core or old Python app |
| Python ANALYSIS worker starts | Worker receives resolved settings/job specs from Go/RabbitMQ and never parses `gmeow.toml` |
| Invalid config is provided | Shared Go parser rejects it before component startup and reports schema/field errors |
| SOPS age identity is missing | Startup fails before any component starts and reports the required env/file sources |
| Config includes a secondary config-file pointer | Shared Go parser rejects it; config source selection is only CLI/env/default startup policy |
| SOPS-protected secret leaves are provided | Shared Go parser decrypts referenced leaf envelopes and dispatches resolved values; components and Python workers do not read SOPS directly |
| PostgreSQL config is inspected | Host, port, database, user, and SSL mode are visible; only the password is encrypted as a named leaf secret |
| Encrypted config secret is updated | `gmeow-admin config secret set` atomically updates one encrypted leaf without rewriting whole operational config blocks |
| QUERY SQL is reviewed | SQL is parameterized through `pgx`/query-builder APIs; string-formatted SQL is rejected by review or static checks |
| Destructive admin command targets production | Command refuses to run unless the documented audited production workflow is satisfied |
| RabbitMQ restarts | Unacked jobs are redelivered; idempotency prevents duplicate outputs |
| Analyzer repeatedly fails | Job reaches dead-letter queue with manifest/job error context |
| Human category override is added | Overlay persists in FILESTORE and is mirrored into QUERY |
| Blob is found without manifests/indexes | Human can inspect the immutable sidecar to identify media type, source hint, sizes, and compressed/uncompressed hashes |
| Emergency sidecar is corrupt or missing | FILESTORE verification reports it, but normal processes do not rely on it |
| Gmail source action requested for Drive object | INTERFACE rejects it through capability checks |
| FILESTORE object corrupts | Integrity checker reports it and SCHEDULER can enqueue repair/hydration if a capable source exists |

## Testing Strategy

Testing should define the architecture contract, not just implementation details.

- **Functional workflow tests:** run real `gmeow`, `gmeow-admin`, and worker commands against
  temporary FILESTORE roots plus real PostgreSQL/RabbitMQ test services where practical; inspect the
  resulting object directories, annotations, projected rows, queue state, stdout/stderr, and exit
  codes. These tests are the proof for core operator workflows.
- **Unit tests:** config parser/defaults/validation, FILESTORE atomicity, object ID strategy
  enforcement, emergency sidecar contents, manifest merge semantics, analyzer idempotency keys,
  source capability checks, dedupe ownership, compound structure assembly, QUERY SQL builders,
  scheduler work derivation.
- **Contract tests:** every SOURCE adapter emits valid source events; every ANALYZER obeys spec and
  writes expected manifest sections; every INTERFACE plane calls shared services.
- **Integration tests:** FILESTORE object-directory walk + QUERY rebuild; SCHEDULER + RabbitMQ +
  test worker; source ingest through projection; forced analysis from MCP/REST through to annotation
  update; `mail_search` fan-out through real app-service, QUERY, SOURCE, and Gmail-adapter code
  paths, with mocks only below external APIs such as Google Gmail.
- **Failure tests:** duplicate ingest, worker crash before ack, worker crash after FILESTORE write,
  stale analyzer output, parent refresh after subobject analysis, dead-letter requeue, corrupt CAS
  object, missing emergency sidecar, missing manifest, PostgreSQL wipe.
- **Performance tests:** ingest throughput, projection lag, queue latency by priority, search p95/p99,
  semantic search latency, rebuild time by object count.

Mocks are allowed only at external-system boundaries such as live Google Gmail/Drive APIs,
unavailable model endpoints, and network services that cannot be exercised safely in a given test.
They are not sufficient proof for core workflows. Any feature that changes an operator command,
config bootstrap, FILESTORE write path, QUERY rebuild, scheduler enqueue, or worker acknowledgement
path needs at least one representative functional test using the real in-repo code path.

## Risks And Mitigations

1. **Manifest design becomes too broad.** Keep the manifest schema explicit and versioned. Store large
   outputs as CAS objects and reference them instead of bloating manifests.
2. **Emergency sidecars become a shadow metadata system.** Make them write-once, minimal, and
   ignored by all normal processes; they are for human recovery only.
3. **QUERY leaks into the domain.** Enforce dependency direction with package boundaries and tests
   that exercise real QUERY interfaces without importing concrete implementations into domain or
   interface packages.
4. **Facets become vague tags.** Keep facet schemas versioned and enforce "at least one facet" at
   FILESTORE ingest time.
5. **Compound objects collapse back into source-specific blobs.** Require subobjects and typed
   relationships for multipart content such as email messages and Drive exports.
6. **Analyzers learn facet semantics.** Keep analyzer inputs object-oriented: bytes, media type,
   content role, part role, dependency outputs, and analyzer version.
7. **Compound parent metadata goes stale.** Make FILESTORE parent refresh part of subobject analysis
   commit and test structure queries after analysis.
8. **Compound IDs are unstable or ad hoc.** Require each compound facet/source strategy to specify
   object ID derivation before ingestion; reject compounds without a stable ID.
9. **Dedupe leaks into adapters.** Keep digest computation and object reuse inside FILESTORE APIs;
   adapters only submit content and desired metadata.
10. **Scheduler duplicates work.** Make idempotency keys first-class and test repeated scans.
11. **Analyzer versioning gets vague.** Require every analyzer to declare version and output sections.
12. **Priority queues starve background work.** Reserve worker concurrency or scheduling windows for
   background classes.
13. **Source-specific assumptions creep into FILESTORE.** Keep source-native fields inside provenance
   metadata and use capability checks for source actions.
14. **Python/Go contract drift.** Generate or share JSON schemas for manifests, analyzer specs, and
   jobs; test both runtimes against the same fixtures.
15. **Operational complexity grows.** RabbitMQ and PostgreSQL are acceptable because they are already
   part of the available core-data stack; keep Valkey optional.

## Revised Effort Estimate

This is larger than the earlier compatibility rewrite because the data model is intentionally being
redesigned. The old Python code remains useful as an executable reference for Gmail behavior,
attachment extraction, graph extraction, semantic indexing, categorization, and MCP tool coverage,
but it is not a porting target.

| Phase | Agent time | Wall time |
|---|---:|---:|
| 0. Skeleton and contracts | 2-4 hrs | 3-6 hrs |
| 1. FILESTORE | 4-7 hrs | 6-12 hrs |
| 2. QUERY projection | 5-9 hrs | 8-18 hrs |
| 3. SCHEDULER/RabbitMQ | 4-7 hrs | 6-14 hrs |
| 4. ANALYSIS workers | 6-12 hrs | 10-24 hrs |
| 5. SOURCE adapters | 6-12 hrs | 12-30 hrs |
| 6. INTERFACE | 5-10 hrs | 8-20 hrs |
| 7. Hardening/ops | 4-8 hrs | 8-24 hrs |

**Total:** roughly **36-69 hours of agent time** and **61-148 hours wall time**, depending on how much
Drive, IMAP, summarization, and operational hardening are included in the first production slice.

A practical v1 can be shorter if it ships in this order:

1. FILESTORE + QUERY rebuild;
2. local/push ingest + basic analysis;
3. MCP search/retrieve/force-analysis;
4. Gmail adapter;
5. REST/CLI;
6. IMAP and Drive.

## Bottom Line

The right architecture is not "Go port of the Python Gmail cache." It is a FILESTORE-first knowledge
system with PostgreSQL as a rebuildable QUERY projection, RabbitMQ-backed SCHEDULER, idempotent
ANALYSIS workers, capability-declared SOURCE adapters, and multiple INTERFACE planes over shared
services.

This gives the system the properties the current Python application points toward but cannot cleanly
achieve without a hard reset:

- source-agnostic ingestion;
- durable analysis and human metadata outside the database;
- rebuildable search and analytics;
- priority-aware enrichment;
- self-healing repair loops;
- rich MCP/REST/IMAP/CLI access without interface-specific domain models;
- a clean path to future FUSE browsing and stronger search backends.
