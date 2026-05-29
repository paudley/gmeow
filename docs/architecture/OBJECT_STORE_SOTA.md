<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Object Store — State-of-the-Art Target Architecture

Status: design / ADR. The storage substrate of this target is now **implemented** — the
content tier (FastCDC content-addressed chunk packs) and the metadata tier (embedded Pebble
LSM holding manifests, recipes, annotations, recovery, and indexes, with no per-object
directories) are live; see the implementation status in
`docs/architecture/OBJECT_STORE_ROADMAP.md` and the current layout in
`docs/architecture/FILESTORE.md`. The DAG/permanode unification and recovery-hardening phases
remain future work. The "current FILESTORE" described below is the **pre-migration** starting
point (object directories + packed `*-v2` sidecars) that motivated this design, retained here
for context — not the live layout.

## 1. Problem and goals

The first live Gmail backfill exposed heavy space amplification. Measured on the running
store at ~1,000 objects:

- ~8.6M logical bytes occupied ~50M on disk (**~5.8×**).
- Per object: ~10 separate files (`blob.zstd`, `manifest.json.zst`, `scheduler.json.zst`,
  N× `analysis-*.json.zst`), most 300–700 bytes, **each rounding up to a 4K Btrfs block**.
- Index sidecars (`source-index-v2`, `source-alias-index-v2`, `compound-parent-index-v2`,
  `recovery-v2`) shard into a 65,536-bucket space and held ~1 record per shard at this
  scale (12–16× amplification), plus ~1,000 empty lock-shard directories (~4M).

The cause is **filesystem small-file and directory amplification, not compression** — every
blob and sidecar is already zstd-compressed. The over-wide index sharding self-amortizes at
production scale (15M ÷ 65,536 ≈ 229 records/shard); the **per-object multi-file layout does
not** — it is a fixed cost per object and would reach ~150M files / ~600GB of pure block
waste at target scale.

### Target workload

| Class | Volume | Character |
|---|---|---|
| Email messages | ~15M | tiny, highly self-similar, multi-part (headers/body/mime/attachments) |
| FLAC / MP3 | ~0.5TB | large, already-compressed, immutable |
| Drive files | 1–2TB | mixed compressibility, **true content revisions** |

### Non-negotiable properties

- FILESTORE is the **authoritative, rebuildable** store. PostgreSQL/QUERY is a derived
  projection that can be rebuilt entirely from FILESTORE.
- Local, single-user, trusted host. Loopback only.
- Crash-safe, bit-rot-resistant, backup-friendly.

## 2. The cardinal split: metadata ≠ data

The single most important decision is to stop storing tiny metadata as filesystem files. The
two concerns have opposite physics:

- **Metadata** — manifests, annotations, indexes, recovery records, the chunk map. Tiny,
  numerous (15M × several), point-lookup + ordered-scan, accretive. Belongs in a
  **write-optimized LSM key-value store**. Recommended engine: **Pebble** (pure-Go, no cgo;
  block-compressed and block-checksummed SSTs, bloom filters, ordered iteration for rebuild,
  WAL for crash recovery). Per-record overhead ≈ 0.
- **Data** — blob bytes. Large aggregate, immutable, write-once-read-many, content-addressed.
  Belongs in an **append-only content-addressable store (CAS) of packed segments**.

Everything below follows from this split.

## 3. Data tier — content-defined chunking + content-addressed packs

The model that restic, Borg, casync/desync, Perkeep, git packfiles, and Facebook Haystack all
converge on:

1. **Content-defined chunking (CDC).** Split every blob with a rolling-hash chunker (FastCDC,
   ~4–64KB variable chunks) rather than fixed blocks. Address each chunk by **BLAKE3** (already
   the repo hash). An object becomes a *recipe*: an ordered list of chunk hashes.
2. **Dedup falls out for free** and is the dominant win for this mix:
   - Email: quoted replies, forwarded threads, list boilerplate, signatures, and **identical
     attachments mailed to many recipients** dedup massively.
   - Drive: successive document revisions share nearly all chunks (CDC shifts gracefully
     around edits where fixed blocks would not).
   - FLAC/MP3: already at entropy, so sub-file dedup is ~0, but **exact duplicate tracks**
     dedup at whole-chunk granularity, and no CPU is wasted trying to compress them.
3. **Pack files.** Append chunks into large immutable sealed **segments** (256MB–1GB), each
   with a trailing index and checksum. A 400-byte body costs ~400 bytes in a pack, never a 4K
   block. Sequential writes give high throughput; sealed packs make backup incremental
   (rsync/ZFS-send transfers only new segments). This erases the projected ~600GB of block
   amplification.

## 4. Compression — content-aware

- **Email: zstd with a trained dictionary.** Dictionaries are the SOTA lever for many small,
  similar records — a 600-byte body that barely compresses alone shrinks dramatically against
  a dictionary trained on common headers/HTML/boilerplate.
- **Media (FLAC/MP3): store raw.** Already at entropy; compression is wasted CPU.
- **Drive: adaptive** — try zstd, keep only if it beats a threshold (PDF/JPEG/zip are already
  compressed).
- Record the codec and dictionary id per object so reads are self-describing; version
  dictionaries as the corpus drifts.

## 5. Versioning — content-addressed Merkle DAG + permanode + claims

**Decision: unify versioning into a content-addressed Merkle DAG.** This is not an add-on —
it collapses four subsystems that already exist in gmeow into one structure.

### gmeow already implements ~80% of this, as disconnected parts

| Existing mechanism | Code | DAG role |
|---|---|---|
| `CompoundPart{Digest,Role,Order}` in the manifest | `internal/contracts/contracts.go` | forward Merkle edges (parent → child by hash) |
| `compoundParentIndexRecord.Parents []compoundParentEdge` | `internal/filestore/source_ingest.go` | inverse, **many-to-many** edges |
| `Relationship{Type,From,To,Role}` | `internal/contracts/contracts.go` | typed directed edges (`thread_member`, `mail_identity_links`) |
| Version sets: `VersionSetMetadata`, `VersionRecordMetadata`, `PreviousCanonicalID`, `BaseVersionID`/`BaseDigest`, scale trivial/minor/major | `internal/contracts/versioning.go` | version history + deltas |
| `compound_stable_id` identity (`hash("compound:"+objectID)`, stable across content change) | `internal/filestore/store.go` | a **permanode** — stable logical identity decoupled from content hash |
| Annotations/overlays keyed by digest; JMAP mailbox/keyword state in Postgres | `internal/filestore/store.go`, `internal/query/postgres/jmap.go` | **claims** — mutable state that never changes identity |

Those are nodes, edges, history, permanodes, and a claims layer — implemented as four
parallel mechanisms with four indexes. A DAG merges them.

### Advantages of making the DAG explicit

1. **Unification.** Parts, versions, threads, and relationships all become typed edges between
   content-addressed nodes; the version-set facets, compound-parent index, and relationships
   list stop being separate systems.
2. **Native delta versions via structural sharing.** A new version is a new root node that
   re-references the unchanged child nodes by hash; only the changed leaf is new. The explicit
   `delta`/`BaseDigest` encoding becomes automatic for everything except the actually-changed
   bytes — the central win for **Drive revisions** and **minor mail canonicalization-variants**.
3. **Verifiable immutable history.** Each version is a Merkle root; the whole lineage is
   tamper-evident and hash-checked — consistent with "authoritative, rebuildable store."
4. **Multi-parent / convergence.** A DAG (not a chain) is the correct shape for gmeow's genuine
   many-to-many: the same content arriving from multiple sources converging via dedup, a
   message in multiple threads/mailboxes, a shared attachment.
5. **GC by reachability** from permanode roots (mark-and-sweep); **rebuild by DAG walk** into
   the QUERY projection.

### The discipline that keeps it correct (the Perkeep model)

- **Immutable Merkle DAG nodes for content/versions only.**
- **Mutable state = claims keyed by permanode in the LSM, never new versions.** Otherwise every
  "mark read" / label toggle mints a version and churns history. This maps Gmail `HistoryId`
  (which tracks label/flag changes, not content) to **claims**, while Drive revisions and mail
  canonicalization-variants become **DAG version nodes + promotion**.
- **Permanode ≠ content hash.** The stable logical identity must survive content edits; only
  the version nodes are content-addressed. gmeow already has this duality
  (`compound_stable_id` vs `file_blake3`).

The result is essentially **Perkeep** (permanodes + immutable Merkle DAG + mutable claims),
which was designed for exactly this personal-email-plus-files-plus-versions domain, sitting on
the CAS/LSM substrate above.

## 6. Recovery model

Recovery is a property of append-only log discipline, made denser. The existing
`internal/filestore/packed_store.go` already implements the core primitive well and should be
generalized rather than replaced: length-prefixed framing (`<8-hex-len>\t<json>\n`), advisory
`flock` with **no on-disk lock state** (kernel releases on exit — backup-safe), `fsync` of file
and directory, torn-tail detection with `verify --repair` truncation, and mid-file corruption
surfaced as a hard error.

- **Self-verifying data.** Every chunk/node name is its BLAKE3 hash → every read validates
  integrity; corruption is detected, not propagated.
- **Rebuildable index.** Each pack carries its own trailer index; the LSM chunk map is fully
  reconstructable by scanning pack trailers. Lose the LSM entirely → replay packs and rebuild
  (the restic guarantee).
- **Append-only root / claim log** is the entry point for full rebuild and for GC; torn-tail
  safe via the existing framing.
- **Crash consistency.** LSM WAL replay for hot metadata; sealed packs are immutable; the open
  pack tail is truncated to the last intact chunk on restart.
- **Bit-rot beyond the filesystem.** Per-pack Reed-Solomon parity (par2-style) repairs bad
  sectors. Btrfs/ZFS already checksum every data block, so app-level parity is portable
  belt-and-suspenders rather than the primary defence.
- **Per-record checksum.** When many records share one packed file, add a CRC32C/BLAKE3 per
  record: today's framing + `json.Valid` catch torn tails and gross garbage but not a silent
  in-file bit flip that keeps JSON valid (on Btrfs/ZFS the FS layer covers this).
- **Substrate.** ZFS or Btrfs underneath provides block checksums, transparent compression as a
  backstop, and snapshot + send/receive backup of an immutable-pack store. Prefer app-level CDC
  dedup over filesystem dedup (FS dedup is RAM-heavy and coarse).

## 7. Tiering by workload class

One chunk store, three policies selected by facet / media-type:

| Class | Chunking | Compression | Dedup | Layout |
|---|---|---|---|---|
| Email (15M, tiny, similar) | CDC | zstd + **trained dictionary** | high (threads, attachments) | packs |
| FLAC/MP3 (0.5TB) | whole-file / coarse CDC | **none** (raw) | whole-file only | packs, or standalone if very large |
| Drive (1–2TB, mixed) | CDC | adaptive zstd | high (revisions) | packs |

## 8. Trade-offs

- **GC / refcounting complexity.** Shared chunks/nodes require mark-and-sweep GC or
  refcounting; deletes become "unreference, then sweep." This is the main cost of going SOTA.
- **Ingest CPU.** Rolling-hash chunking + hashing + compression is CPU-heavy; pipeline it
  across the existing worker pool.
- **Random-read locality.** A heavily-deduped object's chunks scatter across packs → more seeks
  to reassemble. Mitigate by packing a single object's fresh chunks contiguously and keeping
  the chunk index hot on SSD/NVMe.
- **Dictionary lifecycle.** Trained dictionaries drift; version them and record which one each
  record used.
- **Index footprint.** ~3–4TB at ~64KB chunks ≈ ~50M chunks → a few GB of chunk index; keep the
  LSM on SSD even if packs live on slower disk (hot-metadata / cold-data tiering).

## 9. Synthesized stack

> CDC (FastCDC) → BLAKE3-addressed chunks → content-aware zstd(+dictionary) → append-only sealed
> packs (CAS), with a Pebble LSM holding manifests/annotations/indexes/chunk-map, an index that
> is rebuildable from pack trailers, an append-only root/claim log for rebuild + GC, per-pack
> Reed-Solomon parity, all on a checksummed/snapshotting volume (ZFS/Btrfs).

This borrows Venti/Perkeep content-addressing + permanodes, restic/Borg CDC dedup + packs,
Haystack small-object packing, RocksDB/Pebble LSM metadata, and ZFS durability — each from the
system that does that one thing best.

See `docs/architecture/OBJECT_STORE_ROADMAP.md` for the phased, evolutionary path from today's
FILESTORE to this target.
