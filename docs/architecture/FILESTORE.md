# FILESTORE Architecture

FILESTORE is the authoritative datastore for Gmeow. All object bytes,
manifests, compound structure, annotations, overlays, provenance, source ingest
claims, and source cursors are owned here.

## Object Model

Byte-bearing objects use canonical BLAKE3 content identity and zstd-compressed
payload storage. Each object directory contains the compressed blob,
authoritative manifest, and optional annotation and overlay files. Recovery
records are immutable FILESTORE metadata and are stored in packed recovery
shards so ordinary object writes do not create one extra tiny recovery file per
object.

Compound objects are first-class FILESTORE objects. They have stable object IDs,
their own digest, facets, manifest, and role-mapped parts. Gmail messages are
stored as `mail_message` compound objects containing headers, body, Gmail
metadata, MIME structure, and attachment subobjects.

Every object must have at least one facet. Facets guide interfaces and
projection but do not transfer authority away from FILESTORE.

## Dedupe And Source Identity

FILESTORE owns dedupe. SOURCE adapters and BACKENDS look up source identity,
acquire ingest claims for misses, and stream payload bytes through the
`FilestoreService` API. They do not prehash, spool, or decide object reuse.

Source identity lookup uses stable source kind/name/external ID/version fields.
Ingest claims serialize concurrent hydration attempts for the same source
object.

Source identity lookups are served by packed FILESTORE source-index records
under the configured filestore root. Runtime hydrate and search paths read that
index and verify the pointed manifest; they do not recover by walking object
directories. Missing historical index records are treated as misses and are
recreated by normal ingest.

## Annotations And Notifications

Analyzer outputs, overlays, and source cursors are written atomically as
FILESTORE annotations. After object or annotation state changes, FILESTORE
notifies SCHEDULER over gRPC. SCHEDULER then derives analysis work, QUERY
projection refresh, or both.

Normal reads and projection use manifests and annotations. Recovery records are
for operator triage and verification, not runtime authority.

Compound child-to-parent relationships are also indexed in packed FILESTORE
shards. Analysis annotation writes use the reverse parent index to refresh
affected compound parents without scanning the object tree.

Packed metadata uses append-only JSONL shard files:

- `source-index-v2/<hh>/<hh>/records.jsonl`
- `compound-parent-index-v2/blake3/<hh>/<hh>/records.jsonl`
- `recovery-v2/blake3/<hh>/<hh>/records.jsonl`

Readers check packed v2 shards first and fall back to the legacy v1 layout
(`source-index/`, `compound-parent-index/`, and per-object `recovery.json`) so
existing FILESTORE roots remain readable during migration. New writes use v2.

## Operations

`gmeow-admin filestore verify` checks object integrity and reports corrupt or
incomplete state without poisoning unrelated reads. Backup and restore
procedures are documented in `docs/FILESTORE_BACKUP_RESTORE.md`.

`gmeow-admin filestore cleanup-locks` removes expired source ingest lock files
and empty lock shard directories. `gmeow-admin filestore compact` migrates
legacy v1 metadata files into packed v2 shards; use `--dry-run` first to count
eligible files without mutating the FILESTORE root. `gmeow-admin filestore
export-recovery --digest <digest>` prints one object's recovery record from v2
or legacy recovery data.

`gmeow-admin filestore storage` reports the logical and filesystem-allocated
bytes used by one object and lists the FILESTORE files that contribute to that
total. It accepts `--digest`, a source object ref, or `--message-id`. Message-ID
resolution goes through QUERY gRPC exact mail identity lookup; the command does
not talk directly to PostgreSQL. Compound objects include referenced part
objects by default; use `--no-recursive-parts` to inspect only the target
object's own files.

`gmeow-admin filestore path <path>` resolves an absolute or FILESTORE-relative
path back to the data it belongs to. It classifies object files, packed metadata
shards, legacy indexes, source cursors, source locks, and staging paths without
walking the FILESTORE tree. Packed shard reports decode a bounded record sample
and include the total record count for the target shard; use `--records-limit`
to adjust the sample size and `--json` for structured output. Symlink escapes
and paths outside the configured FILESTORE root are rejected.
