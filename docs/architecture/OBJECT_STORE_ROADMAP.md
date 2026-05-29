<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Object Store — Phased Implementation Roadmap

This roadmap evolves the current FILESTORE toward the target in
`docs/architecture/OBJECT_STORE_SOTA.md`. It is deliberately **evolutionary, not a big-bang
rewrite**: each phase is independently shippable, reuses existing code where possible, and
states a concrete measurement so the value is proven before the next phase starts.

Ordering principle: do the **highest-ROI, lowest-risk** work first (the measured per-object
amplification), and defer the largest architectural change (DAG unification) until the storage
substrate underneath it is in place.

## Implementation status

- **Phase 1 — done, then superseded by the full metadata migration below.** Annotations stopped
  being one file per item. The manifest was the last per-object file, and it has since moved into
  the metadata LSM too (see Phase 3), so objects now have **no per-object directory at all**.
- **Phase 2 — done.** Content-addressed chunk store: FastCDC chunker, BLAKE3 chunks, append-only
  packs, recipes; `Put`/`Open` reroute through chunks; mixed-mode read fallback.
- **Phase 3 — done (decision recorded below), and extended to all metadata.** Keyed metadata —
  chunk-map, recipes, annotations, **manifests**, and the **source/alias/compound-parent/recovery
  indexes** — all live in an embedded **Pebble** LSM (`metadata/`; key namespaces `m/ r/ c/ a/ si/
  sa/ cp/ rec/`). Pebble takes an **exclusive directory lock**, incompatible with the prior
  multi-process access (filestore-serve writer + query-serve reader). **Resolution: FILESTORE owns
  its metadata store as the sole writer.** QUERY and admin no longer open the FILESTORE root; they
  stream the projection over FILESTORE gRPC RPCs (`GetProjectionObject`, `WalkProjection`,
  `WalkChangedProjection`, `WalkSourceCursors`). Object enumeration is a Pebble `m/` prefix scan;
  large annotation payloads externalize into the chunk store rather than side files. Offline
  maintenance (`filestore verify`/`compact`) legitimately requires `filestore-serve` stopped — the
  lock enforces it. The legacy packed-shard and per-object-directory layouts remain readable for
  pre-migration roots; new writes go only to packs + Pebble. **Measured result:** per-object
  on-disk amplification dropped from ~15.6× (manifest rounded to a 4K block + a directory inode) to
  ~1.0× (manifest stored at its actual byte size in a shared, block-compressed LSM).
- **Phase 4 — done (mechanism).** Chunk compression is dictionary-aware: each chunk records the
  dictionary id it used (empty = none), dictionaries are raw zstd dictionaries under
  `<root>/dictionaries/` selected by a `current` marker, and old/new chunks coexist. Training a
  real dictionary needs a representative corpus and is an operator/recompaction step; the running
  mechanism is in place so the full backfill can adopt one.
- **Lifecycle + reclamation — done.** Streaming chunk I/O (no whole-object
  buffering on put/open/verify); a read-side pack-descriptor cache; `DeleteObject`;
  online snapshot-based `Gc` (mark-and-sweep with a grace window) and `Repack`
  (online pack rewrite, reads retry on superseded packs); a trained zstd
  dictionary workflow (`train-dictionary`, adopted by new chunks immediately and
  existing chunks on the next repack). Inspection/reclamation admin commands run
  online over the FILESTORE gRPC service. Source ingest claims moved into the
  metadata LSM (`lk/`).
  *Validated:* a greenfield re-backfill produces only `chunk-packs/` +
  `metadata/` + `source-cursors/` (no per-object directories, no `source-locks/`
  files); `delete`, `gc`, and `repack` run live over gRPC against a serving
  store with no lock conflict; a live `gc` scanned 138k chunks with zero false
  sweeps; the `delete → gc → repack` reclamation loop and the streaming
  chunk/dedup/verify and dictionary-adoption paths are covered by unit tests.
  Byte-level `repack` reclamation is observable only past `packTargetBytes`
  (sealed packs), i.e. at production scale.
- **Phase 5 (DAG/permanode) — not yet started.**
- **Phase 6 (recovery hardening) — partially shipped:** content is self-verifying
  per chunk and the metadata LSM is rebuildable from packs; per-record
  checksums and Reed-Solomon parity remain future work.

## Phase 1 — Kill per-object small-file amplification (highest ROI, lowest risk)

The per-object cost of ~10 tiny files each rounding to a 4K block is the only amplification
that does **not** self-heal at scale.

- Pack each object's metadata (`manifest`, `scheduler`, and every `analysis-*` annotation) into
  digest-sharded append-only `records.jsonl` shards instead of one file per item per object —
  reusing the existing, recovery-grade primitives `appendPackedRecord` / `scanPackedShard` /
  torn-tail repair in `internal/filestore/packed_store.go`.
- Right-size the shard fan-out for the actual corpus (the current 2-byte / 65,536-bucket key is
  sized for millions; it wastes blocks at thousands). Consider a size-triggered split instead of
  a fixed prefix width.
- Reclaim the ~1,000 empty lock-shard directories now via `gmeow-admin filestore cleanup-locks`,
  and review the lock-directory lifecycle so they do not re-accumulate.
- Keep large blobs (`blob.zstd`) as their own files — block waste is negligible for large
  content, and per-blob files keep them independently verifiable and rsync-friendly.

**Measure:** on-disk vs logical bytes (via `gmeow-admin filestore storage --json` plus `du
--apparent-size`) before and after, on a live backfill sample. Target: per-object allocated
bytes approach logical bytes for small messages.

## Phase 2 — Content-addressed chunk store (CAS)

- Add a FastCDC content-defined chunker over BLAKE3 (the existing repo hash).
- Add a pack writer producing immutable sealed segments (256MB–1GB) with a trailing index +
  checksum, and a reader that resolves a recipe (ordered chunk hashes) back to bytes.
- Route large blobs through packs; pull tiny blobs (e.g. `rfc822_headers`, short bodies) into
  packs above a size threshold so they stop costing a block each.
- Keep the rebuild-from-pack-trailers guarantee (each pack self-describes its chunks).

**Measure:** dedup ratio and total space vs the current store on a real email + Drive sample
(threads, forwarded mail, repeated attachments, document revisions).

## Phase 3 — LSM metadata store (Pebble)

- Introduce Pebble (pure-Go, no cgo) as the metadata/index engine: manifests, annotations,
  source/alias indexes, recovery records, and the chunk map (`chunk-hash → pack-id, offset,
  length, refcount`).
- Migrate the `*-v2` packed sidecars into the LSM; keep their append-only/rebuildable semantics.
- Preserve the invariant that the LSM is fully reconstructable by scanning packs (LSM is a cache
  over authoritative packs + root log).

**Measure:** ingest throughput, point-lookup latency, and total metadata footprint vs today.

## Phase 4 — Content-aware compression

- Train a zstd dictionary on an email-corpus sample; compress small email records/chunks against
  it.
- Detect media (FLAC/MP3) and store raw; use adaptive zstd (keep-if-it-helps) for Drive.
- Record codec + dictionary id per object; version dictionaries as the corpus drifts.

**Measure:** compression ratio per workload class, especially small-email ratio with vs without
the dictionary.

## Phase 5 — DAG / permanode unification

Collapse the four existing subsystems (see the ADR's mapping table) into one content-addressed
Merkle DAG plus a claims layer.

- Model immutable content/versions as DAG nodes with typed edges (parts, thread membership,
  relationships, version lineage), reusing `CompoundPart`, `Relationship`, and the version-set
  semantics in `internal/contracts/`.
- Make the existing `compound_stable_id` the explicit **permanode** (stable logical identity);
  hang version nodes off it.
- Move mutable state (annotations, JMAP mailbox/keyword state, overlays) into a **claims log
  keyed by permanode** in the LSM — never as new versions. Map Gmail `HistoryId` changes to
  claims; map Drive revisions and mail canonicalization-variants to DAG version nodes +
  promotion.
- Implement GC as mark-and-sweep reachability from permanode roots.

**Verify:** existing version-set, compound-part, relationship, and JMAP behaviors are
reproduced; a QUERY rebuild produced by walking the DAG matches the projection produced today
(diff the projected tables).

## Phase 6 — Recovery hardening

- Add per-record checksums (CRC32C/BLAKE3) to packed records and DAG nodes.
- Add optional per-pack Reed-Solomon parity (par2-style) for bit-rot repair beyond the
  filesystem.
- Extend `verify` / `verify --repair` (`internal/filestore/verify.go`) to cover packs and the
  DAG/claim logs, not just object directories and `*-v2` shards.
- Document the ZFS/Btrfs substrate guidance (block checksums, snapshots, send/receive backup).

**Verify:** inject torn-tail, mid-file, and bit-flip corruption into packs and logs; confirm
detection and, where recoverable, repair; confirm a full store rebuild from packs + root log
alone.

## Existing code this roadmap reuses

- `internal/filestore/packed_store.go` — append-only framed shard format + torn-tail repair (the
  recovery primitive generalized in Phases 1, 5, 6).
- `internal/contracts/versioning.go`, `internal/contracts/contracts.go` — version sets,
  `CompoundPart`, `Relationship`, `Provenance` (unified in Phase 5).
- `internal/filestore/store.go` — `file_blake3` vs `compound_stable_id` identity (content node
  vs permanode).
- `internal/query/postgres/jmap.go`, `internal/query/postgres/source_cursor.go` — mutable
  state/claims and cursors (move to the claims layer in Phase 5).
- `internal/filestore/maintenance.go`, `verify.go`, `storage_report.go` — compaction, verify,
  and the `filestore storage` diagnostic to extend.

## Sequencing notes

- Phases 1–4 are pure storage-substrate work and ship value independently of the DAG.
- Phase 5 depends on the CAS (Phase 2) and LSM (Phase 3) being in place.
- A throwaway prototype of Phase 2 (chunker + pack writer + Pebble chunk index + email-trained
  dictionary) is the recommended way to empirically validate the dedup/compression/space
  assumptions on a real backfill sample before committing to the full build.
