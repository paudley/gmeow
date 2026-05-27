# FILESTORE Architecture

FILESTORE is the authoritative datastore for Gmeow. All object bytes,
manifests, compound structure, annotations, overlays, provenance, source ingest
claims, and source cursors are owned here.

## Object Model

Byte-bearing objects use canonical BLAKE3 content identity and zstd-compressed
payload storage. Each object directory contains the compressed blob,
authoritative manifest, immutable recovery sidecar, and optional annotation and
overlay files.

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

Source identity lookups are served by FILESTORE source-index records under the
configured filestore root. Runtime hydrate and search paths read that index and
verify the pointed manifest; they do not recover by walking object directories.
Missing historical index records are treated as misses and are recreated by
normal ingest.

## Annotations And Notifications

Analyzer outputs, overlays, and source cursors are written atomically as
FILESTORE annotations. After object or annotation state changes, FILESTORE
notifies SCHEDULER over gRPC. SCHEDULER then derives analysis work, QUERY
projection refresh, or both.

Normal reads and projection use manifests and annotations. Recovery sidecars are
for operator triage and verification, not runtime authority.

Compound child-to-parent relationships are also indexed in FILESTORE. Analysis
annotation writes use the reverse parent index to refresh affected compound
parents without scanning the object tree.

## Operations

`gmeow-admin filestore verify` checks object integrity and reports corrupt or
incomplete state without poisoning unrelated reads. Backup and restore
procedures are documented in `docs/FILESTORE_BACKUP_RESTORE.md`.
