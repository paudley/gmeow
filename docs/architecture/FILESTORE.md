# FILESTORE Architecture

FILESTORE is the authoritative datastore for Gmeow. All object bytes,
manifests, compound structure, annotations, overlays, provenance, source ingest
claims, and source cursors are owned here.

## Storage Engine

FILESTORE separates the data tier from the metadata tier so that a workload of
many small, similar records (email) plus large immutable media stays dense on
disk and rebuildable.

- **Content tier — content-addressed chunk packs.** Object bytes are split with
  FastCDC content-defined chunking, each chunk is addressed by its BLAKE3 hash
  and zstd-compressed (with a trained dictionary for small similar records), and
  appended into large sealed, immutable pack segments under `chunk-packs/`.
  Identical chunks are stored once, so quoted email threads, repeated
  attachments, and document revisions share storage. An object's content is an
  ordered recipe of chunk hashes.
- **Metadata tier — embedded Pebble LSM (`metadata/`).** Manifests, content
  recipes, annotations, recovery sidecars, ingest claims, and the
  source/alias/compound-parent indexes are keyed records in one log-structured
  key-value store (block-compressed, block-checksummed, bloom-filtered,
  WAL-backed). Key namespaces: `m/` manifest, `r/` content recipe, `c/` chunk
  index, `a/` annotation, `si/` source index, `sa/` source alias, `cp/` compound
  parent, `rec/` recovery, `lk/` source ingest claim.

Object content is read back as a lazy stream that fetches and verifies one chunk
at a time, and written by streaming the input through the chunker in a single
pass, so multi-gigabyte media and Drive files never need to be held whole in
memory. Read pack file descriptors are cached (LRU) so reassembling a many-chunk
object does not reopen a pack per chunk.

There is **no per-object directory and no per-object file**. This removes the
filesystem small-file amplification (a ~600-byte manifest no longer rounds up to
a 4K block plus a directory inode) while keeping the store fully rebuildable:
every chunk is self-verifying by hash, and the metadata LSM is reconstructable
by scanning pack contents. FILESTORE owns Pebble exclusively for the process;
QUERY and admin tooling read the projection over gRPC rather than opening the
store.

## Object Model

Byte-bearing objects use canonical BLAKE3 content identity. The content is
stored as deduplicated chunks (above) and the authoritative manifest, optional
annotations and overlays, and the recovery sidecar are keyed records in the
metadata LSM. Recovery records are immutable FILESTORE metadata; ordinary object
writes never create per-object tiny files.

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

Source identity lookups are served by source-index records (`si/`/`sa/`) in the
metadata LSM. Runtime hydrate and search paths read that index and verify the
pointed manifest; they never recover by scanning the store. Missing index
records are treated as misses and are recreated by normal ingest.

## Annotations And Notifications

Analyzer outputs, overlays, and source cursors are written atomically as
FILESTORE annotations. After object or annotation state changes, FILESTORE
notifies SCHEDULER over gRPC. SCHEDULER then derives analysis work, QUERY
projection refresh, or both.

Normal reads and projection use manifests and annotations. Recovery records are
for operator triage and verification, not runtime authority.

Compound child-to-parent relationships are indexed in the metadata LSM (`cp/`).
Analysis annotation writes use the reverse parent index to refresh affected
compound parents without scanning the object set.

All metadata records (manifests, recipes, annotations, recovery, and the
source/alias/compound-parent indexes) are keyed entries in the Pebble store;
writes overwrite a key in place, so repeated refreshes cannot grow the store
without bound. Large annotation payloads externalize into the content-addressed
chunk store rather than a side file, referenced by content digest.

Object enumeration (verify, projection rebuild) is an ordered Pebble `m/`
prefix scan over the manifest key space. Legacy on-disk layouts — per-object
directories with `manifest.json.zst`/`blob.zstd`/`recovery.json`, the
`source-index/` and `compound-parent-index/` v1 trees, and the
`*-index-v2`/`recovery-v2` packed JSONL shards — are still readable by the
maintenance, verify, and path-resolve code so a pre-migration root can be
inspected and compacted, but new writes only go to the chunk packs and the
metadata LSM.

## Operations

`gmeow-admin filestore verify` enumerates objects by scanning the manifest key
space in the metadata LSM and checks content (recipe reassembly + per-chunk hash
verification), recovery records, annotation resolution, and source-index
reachability. It also verifies any legacy on-disk packed shards still present.
`verify --repair` rebuilds missing source-index entries from manifest
provenance, reaps stale staging directories, truncates torn legacy-shard tails,
and cleans up expired source locks. This is the post-restore normalization step.
Backup and restore procedures are documented in
`docs/FILESTORE_BACKUP_RESTORE.md`.

`gmeow-admin filestore cleanup-locks` removes expired source ingest lock files
and empty lock shard directories. `gmeow-admin filestore compact` migrates
legacy on-disk metadata (v1 index/recovery files and `*-v2` packed shards) into
the metadata LSM and deduplicates legacy shards; use `--dry-run` first to count
eligible files without mutating the FILESTORE root. `gmeow-admin filestore
export-recovery --digest <digest>` prints one object's recovery record from the
metadata LSM or legacy recovery data.

### Lifecycle and reclamation

`gmeow-admin filestore delete --digest <digest>` removes an object's metadata
(manifest, recipe, recovery, annotations, and its source/alias/compound-parent
index entries) atomically; its content chunks are reclaimed by a later `gc`.

`gmeow-admin filestore gc` reclaims chunk-index entries (and orphaned recipes) no
longer reachable from any object manifest or externalized annotation. It runs
online against a metadata snapshot with a grace window, so concurrent writes are
safe. `gmeow-admin filestore repack` then rewrites sealed packs to reclaim the
on-disk bytes `gc` left behind, copying each pack's still-live chunks into the
active pack and deleting the old pack (the chunk index is updated before deletion
and reads retry on a superseded pack, so repack is also online-safe). Run `gc`
before `repack`.

FILESTORE selects zstd dictionaries by dictionary family, not by one global
active dictionary. The classifier uses MIME type plus known content roles to
separate coherent small-object families such as mail headers, mail bodies,
Gmeow mail JSON, patches, structured JSON/XML, source-like text, logs, CSV/TSV,
RDF/Turtle, and small binary serialization formats. Unknown content and
already-compressed or high-entropy formats (archives, images, audio/video, PDFs,
packaged office files, and `application/octet-stream`) default to no dictionary.
Each chunk still records the concrete `dict_id` used, and new chunks also record
the dictionary family; old chunks with a legacy global `dict_id` remain readable.

`gmeow-admin filestore train-dictionary --family <family>` samples only small
objects classified into that family, trains a zstd dictionary via the `zstd`
binary, installs it for that family, and refreshes the in-process cache so new
chunks in that family compress against it immediately. `--all-families` trains
eligible families independently. Existing chunks adopt a newer family dictionary
only when rewritten by a future compaction/recompression path; ordinary `repack`
preserves existing compressed bytes. Training fails closed if `zstd` is absent
or too few representative samples exist.

### Online vs offline

`storage`, `path`, read-only `verify`, `delete`, `gc`, `repack`, and
`train-dictionary` run **online**: they dial a running `filestore-serve` over
gRPC (so the metadata LSM's exclusive lock is respected) and fall back to opening
the store locally when the server is stopped. `verify --repair`, `compact`,
`cleanup-locks`, and `export-recovery` mutate or scan legacy on-disk state
directly and are **offline** — run them with `filestore-serve` stopped.

`gmeow-admin filestore storage` reports an object's logical footprint: the
manifest bytes (in the metadata LSM) and the content bytes (in the chunk packs),
plus any legacy on-disk files still present. Because metadata is shared in the
LSM and content is shared/deduped across packs, these are logical bytes rather
than exclusive per-object files. It accepts `--digest`, a source object ref, or
`--message-id`. Message-ID resolution goes through QUERY gRPC exact mail identity
lookup; the command does not talk directly to PostgreSQL. Compound objects
include referenced part objects by default; use `--no-recursive-parts` to
inspect only the target object. Content rows include dictionary ids and
dictionary families so operators can distinguish no dictionary, current
family dictionaries, mixed-family references, and legacy global dictionaries.

`gmeow-admin filestore path <path>` resolves an absolute or FILESTORE-relative
path back to the data it belongs to. It classifies object files, packed metadata
shards, legacy indexes, source cursors, source locks, and staging paths without
walking the FILESTORE tree. Packed shard reports decode a bounded record sample
and include the total record count for the target shard; use `--records-limit`
to adjust the sample size and `--json` for structured output. Symlink escapes
and paths outside the configured FILESTORE root are rejected.
