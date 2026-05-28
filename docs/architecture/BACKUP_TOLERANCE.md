# FILESTORE Gap Analysis — `mail_import` Branch

## Context

The `mail_import` branch (3 commits ahead of `main`, ~8.1k LOC added) introduces
versioned archive import and a substantial set of filestore changes intended to
move Gmeow toward a "resilient, robust, backup-friendly, fully deduped"
FILESTORE. The new files are:

- `internal/filestore/packed_store.go` (287 LOC) — append-only JSONL shard layer
  for source-index, compound-parent-index, and recovery records (v2 layout).
- `internal/filestore/maintenance.go` (297) — `CompactV1Layout`,
  `CleanupSourceLocks`, `ExportRecoveryJSON`.
- `internal/filestore/process_lock.go` (60) — directory-based mutex used by the
  v2 append path.
- `internal/filestore/path_report.go` (468) — operator path-classifier for
  `gmeow-admin filestore path`.
- `internal/filestore/storage_report.go` (414) — per-object disk-footprint
  reporter.
- `internal/cli/filestore.go` (606) — admin CLI: `verify`, `cleanup-locks`,
  `compact`, `export-recovery`, `storage`, `path`, `serve`.
- `internal/source/archive_import.go` (1426) + supporting files — versioned
  archive ingest with low-noise mode.

The product property this work must deliver:

> The FILESTORE must be backed up by an arbitrary `rsync`/`cp -a`/filesystem
> snapshot taken at any instant while the system is actively writing, with no
> signal/quiesce/pause coordination, and the restored copy must be repairable
> to a fully consistent state by running `filestore verify` plus its self-repair
> output.

That is the same property as *crash-consistency at every instant*: a backup is
indistinguishable from a hard power-loss snapshot of the live process. If the
store survives a hard kill, it survives a backup. This document enumerates the
gaps that violate that property and the dedup, resilience, and operator
ergonomics goals, then sketches the smallest set of changes that closes them.

---

## Architecture as built (one paragraph)

A loose-object content-addressable store keyed on BLAKE3 (`objects/blake3/xx/yy/DIGEST/`)
with three packed JSONL sidecar layers (`source-index-v2/`,
`compound-parent-index-v2/blake3/`, `recovery-v2/blake3/`). Loose-object writes
stage into a hidden directory, fsync, and rename — that part is solid. Packed
writes are O_APPEND newline-JSON, serialized by an in-memory per-key mutex
*and* a single global `.locks/packed-metadata.lock` directory. Reads scan packed
shards linearly. Source-identity dedup is enforced at ingest time via the
packed source-index plus a TTL lock file in `source-locks/`. Verify walks the
loose objects only.

---

## The crash-consistent-backup invariants we must hold

For "rsync at any moment → restorable copy" to be true, every mutating
operation must preserve all of:

1. **No state lives in directories that survive into the backup but whose
   meaning depends on a live process.** Lock *directories* (G2) violate this
   because the backup carries a "locked" marker into the restored copy with no
   owning process to release it.
2. **Every append must be torn-write-tolerant.** A reader of the backup must
   be able to skip a partial record at the tail of a shard without erroring.
3. **Every "newly committed" record must point only at things that are already
   on-disk-durable.** I.e. fsync the dependency *before* fsyncing the
   reference. (Current code mostly does this — see store.go:744 vs 752.)
4. **Crash-orphan state must be either invisible at backup-copy time or
   identifiable and reaped at restore time.** Staging dirs and stale claims
   fall here.
5. **Verify must double as a self-repair tool**, because that is the
   post-restore step the operator runs.

These five invariants are what most of the fixes below are designed to
establish.

---

## Gap Inventory (ranked by severity)

### G1 — Torn-write fragility of packed JSONL shards (CRITICAL for backup)

**Where:** `packed_store.go:206-228` (writer), `packed_store.go:231-255` (reader).

`appendPackedRecord` does `O_APPEND | O_WRONLY` + `file.Write(append(encoded, '\n'))`
+ `file.Sync()` + `fsyncDir`. POSIX `O_APPEND` is not torn-write-atomic across
page boundaries, and any moment-in-time backup can capture a partial trailing
line. `scanPackedRecords` calls `json.Unmarshal` per line and returns the
*first* parse error (`packed_store.go:246-248`):

```go
if err := json.Unmarshal([]byte(line), &record); err != nil {
    return fmt.Errorf("decode packed record %s: %w", path, err)
}
```

Consequence on a backup snapshot: a single torn tail line silently disables
**all** lookups against that shard on the restored copy — every read returns
an error rather than a miss, breaking `LookupSourceObject`,
`compoundParentsForChild`, and `Verify`'s recovery checks for any digest
sharded into the same JSONL file.

**Fix shape (in this branch):**
- Frame each record as `<8-hex-length>\t<json>\n`. The length prefix is the
  count of bytes in `<json>` (or the full record, whichever is simpler). On
  read, validate that the line begins with `<hex><tab>` and that the JSON
  payload is the recorded length. If either check fails *and the offending
  record is the last record in the file*, skip it and emit a `Verify` finding
  that the shard has a torn tail (auto-repairable). If a non-tail record is
  malformed, that is real corruption — fail loudly.
- `Verify` learns a `--repair` mode that truncates the torn tail to the last
  intact record boundary (with `fsyncDir`). Without `--repair` it reports.

Test: a unit test that writes N records, truncates the file mid-last-record
by a byte count chosen to land inside the JSON, and asserts:
(a) read iteration skips the torn record and returns the prior N-1,
(b) repair truncates to the last intact record.

---

### G2 — Directory-mutex `.locks/` survives into the backup (CRITICAL for backup)

**Where:** `process_lock.go:14-60`, `packed_store.go:202`.

`os.Mkdir(.locks/packed-metadata.lock)` is the lock primitive. The lock
directory's *presence* is the lock state. If the running process is holding
that lock when the operator runs `rsync`, the backup copy contains the lock
directory. The restored process boots, attempts a packed write, finds the lock
present, and busy-loops forever — because there is no owning process to release
it.

Even ignoring backup, the same hazard applies to ordinary crash recovery: a
SIGKILL during a packed write leaves the lock directory in place permanently,
and `CleanupSourceLocks` only walks `source-locks/`, not `.locks/`.

**Fix shape (in this branch):**
- Replace the directory lock with `flock(LOCK_EX)` on the shard file itself
  (`syscall.Flock` on the open file descriptor in `appendPackedRecord`). The
  kernel releases the lock on process exit, on filesystem unmount, and the
  lock state does not exist on disk — so it cannot survive a backup or a
  crash.
- Delete `process_lock.go` and `withPackedMetadataLock`. Move the in-memory
  `lockKey` per-shard mutex to be the in-process serializer; `flock` is the
  inter-process serializer.
- Update `cli/filestore.go path` classification to no longer expect `.locks/`.
- Update docs.

This single change closes G2 *and* G3 (next item) and removes a class of
on-disk state from the backup.

---

### G3 — Global packed-metadata lock serializes all metadata writes (HIGH scale)

**Where:** `packed_store.go:190-229`.

A single `packed-metadata.lock` covers source-index, compound-parent-index,
and recovery appends across **all** shards. For high-throughput archive
imports (the named motivation of this branch), every message ingested issues
at least one source-index append + one recovery append + N compound-parent
merges — all of them through one mutex.

**Fix shape:** subsumed by G2's per-shard `flock`. With the global lock gone,
shards become independently writable in parallel. The in-process per-key
mutex map (already present, `store.lockKey(lockKey)`) handles intra-process
contention.

---

### G4 — `Verify` does not cover the packed metadata layer (CRITICAL for backup)

**Where:** `verify.go:18-78`.

`Verify` walks `objects/blake3/` only. It never opens `source-index-v2/**`,
`compound-parent-index-v2/**`, or `recovery-v2/**` to validate that:

1. shards parse cleanly end-to-end,
2. every loose object has a recovery entry (or a legacy sidecar),
3. every source-index entry points at a digest whose object actually exists,
4. every compound-parent record points at parents and children that exist.

This is *the* property that has to be true post-restore — without it, "run
verify after restore" (`docs/FILESTORE_BACKUP_RESTORE.md:11`) is a false
guarantee.

**Fix shape (in this branch):**
- Add three shard sweeps to `Verify`:
  - `verifySourceIndexShards`: parse every shard with the new
    torn-tail-tolerant reader, stat the referenced object dir, validate the
    manifest's `Provenance` contains a matching ref.
  - `verifyCompoundParentShards`: same for parent/child existence.
  - `verifyRecoveryShards`: parse every shard; cross-check that the referenced
    digest's loose blob still hashes to the recorded uncompressed BLAKE3.
- Each sweep emits per-tier findings (`source_index_torn_tail`,
  `source_index_dangling_object`, `recovery_shard_unreadable`, etc.).
- Split `gmeow_corrupt_objects` (verify.go:74-75) into per-tier gauges.
- Add a `--repair` flag that:
  - truncates torn JSONL tails (G1),
  - reaps stale staging dirs older than TTL (G7),
  - rebuilds source-index entries for orphan blobs from their manifest
    `Provenance` (G6),
  - rewrites compound-parent shards to dedup duplicate edges (G8 prep).

---

### G5 — Backup is "stop the world" — incompatible with the stated requirement (CRITICAL)

**Where:** `docs/FILESTORE_BACKUP_RESTORE.md:6-11`.

The advertised backup procedure is "stop every service, then copy
`filestore.root`". The requirement is "backup at any moment, no signals".
These are incompatible.

The fix is *not* to add a quiesce primitive (which would itself violate "no
signals") — it is to make the on-disk state crash-consistent at every instant
so that the backup *is* a valid snapshot of a crashed process, and the
post-restore `verify --repair` step normalizes it.

**Fix shape (in this branch, composite):**

The five invariants above must all hold. Concretely, the changes that
together establish them:

1. **G1 + G4:** torn-tail-tolerant packed reader + `verify` shard sweeps.
2. **G2 + G3:** kill on-disk lock directories; use `flock`.
3. **G7:** put all transient staging state under a single top-level
   `staging/` directory with a TTL-based reaper; `verify --repair` reaps it;
   document that backup tools may skip `staging/` and still produce a valid
   restore (no required content lives there).
4. **G6:** `verify --repair` rebuilds source-index entries from manifest
   `Provenance` for any loose object whose source-index lookup misses.
5. **Source-ingest claim files (`source-locks/`):** these already use TTL
   (`source_ingest.go:20`, 15 min) and are reaped by `CleanupSourceLocks`.
   That's correct, no change needed beyond running `cleanup-locks` as part of
   `verify --repair`.
6. **Write-ordering audit:** ensure every place that writes a "pointer" record
   has the target fully fsynced first. Audit `Put`, `PutCompound`,
   `AttachProvenance`, `WriteAnnotation`, `WriteOverlays`,
   `WriteSourceCursor`. Most of `Put` already does this
   (store.go:683-753); confirm and document.

The new documented backup procedure becomes:

> Run `rsync -aH --delete <filestore.root> <dest>` (or any equivalent) at any
> time, with the system running. To activate the backup, run
> `gmeow-admin --config <restore.toml> filestore verify --repair`. If repair
> reports findings, those are the expected post-snapshot artifacts (torn
> tails, transient staging dirs, missed index entries) and have been
> normalized. If repair reports unrepairable findings, the source filesystem
> was corrupted or the backup tool lost data — not a FILESTORE invariant
> violation.

---

### G6 — Orphan blob detection is absent (HIGH for backup)

**Where:** `store.go:58-120` (Put) → `recordSourceObjectIndexes` is called
*after* commit. `verify.go:18-78` does not walk back from objects to ensure
they appear in the source-index.

If a crash (or a backup snapshot) is taken between `commitNewObjectDirectory`
and `recordSourceObjectIndexes`, the loose object exists on disk but is
unreachable through `LookupSourceObject`. The Postgres rebuild path
(`query rebuild`) re-derives identity from the manifest's `Provenance` field
via full-tree walk, so it would in principle re-discover the orphan — but
`Verify` never flags it, and `LookupSourceObject` would never find it for
ordinary reads.

**Fix shape:** in the new `Verify` packed-sweep (G4), build a set of digests
walked from `objects/`, and for each manifest `Provenance` entry, confirm a
matching source-index record exists. Report missing as
`source_index_missing`. In `--repair` mode, call
`recordSourceObjectIndexes(digest, manifest.Provenance)` to write the missing
entry.

---

### G7 — Crashed staging dirs are copied by backup tools (HIGH for backup)

**Where:** `verify.go:215-224` (detects `.*` files only inside *committed*
object dirs); `store.go` staging-dir naming `.DIGEST.XXXXX` and `.incoming/`.

Staging directories live inside the shard parent dirs (`objects/blake3/xx/yy/`).
A crash mid-`commitNewObjectDirectory` leaves a `.DIGEST.XXXXX` directory at
the shard level. Any `rsync`-style backup copies it. Verify currently only
detects `.*` files *inside* committed object dirs, not at the shard level.

**Fix shape (in this branch):**
- Move staging out of `objects/blake3/<xx>/<yy>/` and into a single top-level
  `staging/` tree (e.g., `staging/objects/<ulid>/`). Atomic rename then
  becomes a cross-directory rename within the same filesystem — still atomic
  on POSIX provided the source and target are on the same filesystem. (The
  store is already single-rooted, so this holds.)
- Document that `staging/` may be excluded from backups and is safe to delete
  on restore.
- Extend `verify` to enumerate `staging/` and reap entries older than
  `stagingTTL` (e.g., 1 hour) in `--repair` mode.
- Update `path_report.go` classification.

---

### G8 — Source-index shard grows monotonically with rewrites (MEDIUM dedup/perf)

**Where:** `packed_store.go:45-59` (`writePackedSourceObjectIndex` appends
unconditionally), `packed_store.go:72-87` (read returns last match — implicit
last-write-wins).

Re-importing the same source object writes a new line to the shard every
time. Reads scan O(n) of the shard. Over many import cycles, shards bloat
linearly with rewrites, and lookup latency degrades. `CompactV1Layout`
migrates v1 → v2 but does **not** deduplicate within a v2 shard.

**Fix shape (in this branch):** extend the compactor (rename it
`Compact` so it does both jobs):

```
Compact(ctx, dryRun) → CompactReport:
    compactV1Layout(...)            // existing
    compactV2SourceIndexShards(...) // new: per-shard, keep last per refKey
    compactV2CompoundParentShards(...) // new: per-shard, keep last per childDigest
    compactV2RecoveryShards(...)    // new: per-shard, keep last per digest
```

Each per-shard rewrite must be backup-safe: write to a sibling
`records.jsonl.next`, fsync, rename, fsync parent dir. If a backup catches
the rewrite mid-flight, the backup sees `records.jsonl` (intact) and
`records.jsonl.next` (partial); restore can ignore `.next` via the staging
rule (G7 generalizes: any `.next` is transient).

---

### G9 — Identity key includes `ExternalVersion`; archive re-export bloats metadata (MEDIUM dedup)

**Where:** `source_ingest.go:286-294` (`sourceObjectRefsEqual` compares all
four fields), `archive_import.go` (sets `ExternalVersion` to SHA256 of raw
archive bytes).

A re-export of the same archive (different `ExternalVersion`) is treated as a
new source object even when the underlying message is identical. The branch
*does* handle this at the mail-message layer via low-noise body-line
fingerprint (`archive_import.go:1313-1324`), so content bytes stay deduped
(Blake3 takes care of that), but a new packed source-index entry is appended,
a new Postgres `mail_identities` row is inserted, and a new
`mail_archive_membership` record is written.

Net: zero new blob bytes, but linear metadata bloat per re-export. The
"fully deduped" claim is true for content, weak for identity bookkeeping.

**Fix shape (in this branch):** add an explicit *alias* relation alongside
the existing exact-version source-index:

- New packed shard family: `source-alias-index-v2/<hh>/<hh>/records.jsonl`,
  keyed on `aliasKey = SHA256(SourceKind, SourceName, ExternalID)` — i.e., the
  source-version-agnostic key.
- Each alias record points at the canonical object digest for *that source
  triple*, irrespective of `ExternalVersion`.
- `LookupSourceObject(ref)` checks the exact-version shard first; on miss,
  consults the alias shard (where it exists). If the alias points at a digest
  whose manifest `Provenance` contains a ref with matching SourceKind +
  SourceName + ExternalID (any version), the import treats it as a hit and
  attaches the new `ExternalVersion` as additional `Provenance`/membership
  rather than re-ingesting the body.
- Alias records are written on `Put` if the source-triple appears in
  `Provenance` and no alias exists yet; subsequent imports update the
  `UpdatedAt` only if needed.
- Compactor (G8) also dedups the alias shard.

This is contained, additive (the existing exact-version shard remains), and
the alias-miss path falls through to the current behavior, so legacy stores
keep working.

---

### G10 — `scanPackedRecords` 32 MiB line cap can break large recovery shards (LOW)

**Where:** `packed_store.go:239`
(`scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)`).

A single recovery record contains hashes, sizes, identity-strategy fields, and
`ManifestSourceHints map[string]any`. If hints grow, one record could exceed
32 MiB and the scanner errors out. Via G1, that broke whole shards; with the
G1 fix, it merely fails the per-record decode.

**Fix shape:** validate at write time that the encoded record is < N MiB
(say 1 MiB). Reject violations at the API. Document in `FILESTORE.md`.

---

### G11 — No end-to-end backup → restore → verify integration test (HIGH for confidence)

**Where:** `internal/cli/filestore_test.go`, `internal/filestore/store_test.go`.

Without an integration test, every gap above can silently regress.

**Fix shape (in this branch):** new file
`internal/filestore/backup_restore_test.go` covering at least these scenarios:

1. **Quiet snapshot:** import a corpus of ≥50 mixed objects (loose blobs,
   compound objects with parts, source-indexed, alias-indexed), `cp -a` the
   tree to a second path, run `verify` on the copy, confirm every original
   digest is `Open`-able.
2. **Live snapshot during ingest:** start a background goroutine actively
   ingesting messages; at random points, take a tarball snapshot of the
   tree; extract it to a fresh path; run `verify --repair`; assert
   `verify` returns ok after repair and that every digest committed before
   the snapshot is `Open`-able.
3. **Torn-tail recovery:** write a shard with N records, truncate the file
   to (length-of-last-line - some bytes), run `verify --repair`, assert the
   torn tail is removed and the prior N-1 records are still readable.
4. **Orphan blob recovery:** commit a loose object without recording the
   source-index entry, run `verify --repair`, assert the source-index entry
   is created and `LookupSourceObject` finds it.
5. **Staging reap:** create a stale `staging/objects/<ulid>/` directory,
   run `verify --repair`, assert it is removed.
6. **Alias dedup:** ingest archive at `ExternalVersion=A`, then re-ingest at
   `ExternalVersion=B` (different metadata, same content), assert one
   alias record covers both versions and no new content blob is written.

---

### G12 — No `filestore backup` / `filestore restore` admin commands (LOW operator ergonomics)

**Where:** `internal/cli/filestore.go:26-40`.

The contract is "use any rsync-like tool", but operators still benefit from
canonical wrappers that:

- bundle the right include/exclude patterns (skip `staging/`, skip
  `*.next`),
- emit a small manifest of what was copied + a snapshot timestamp,
- run `verify --repair` on the destination automatically and surface its
  report,
- on restore, accept a manifest and validate before declaring done.

**Fix shape (in this branch):**

- `gmeow-admin filestore backup --to <path> [--use rsync|cp]`:
  performs an `rsync -aH --delete --exclude=staging/ --exclude=*.next
  <root>/ <path>/`, writes `<path>/.filestore-snapshot.json`
  (`{taken_at, source_root, tool, expect_repair: true}`).
- `gmeow-admin filestore restore --from <path> --to <root>`:
  performs the inverse `rsync`, runs `filestore verify --repair` on `<root>`,
  emits the verify report.
- Document the no-coordination contract prominently in
  `docs/FILESTORE_BACKUP_RESTORE.md`.

---

## Summary table

| ID  | Severity | Property affected   | One-line                                                            |
|-----|----------|---------------------|---------------------------------------------------------------------|
| G1  | CRITICAL | Backup/resilience   | Torn JSONL tail line disables entire shard, silently                |
| G2  | CRITICAL | Backup/resilience   | `.locks/` directory mutex survives the backup                       |
| G3  | HIGH     | Robustness/scale    | Global packed lock serializes all metadata writes                   |
| G4  | CRITICAL | Backup/verify       | `Verify` ignores the packed tier where source identity lives        |
| G5  | CRITICAL | Backup-friendly     | Stop-the-world backup contract — must become crash-consistent       |
| G6  | HIGH     | Backup/resilience   | Orphan loose blobs after Put-but-not-indexed snapshot               |
| G7  | HIGH     | Backup-friendly     | Staging dirs interleaved with committed shards, no TTL reaper       |
| G8  | MED      | Dedup/perf          | v2 shards grow unboundedly with rewrites                            |
| G9  | MED      | Dedup               | `ExternalVersion` in identity key bloats metadata on re-export      |
| G10 | LOW      | Resilience          | 32 MiB scanner buffer — narrow ceiling on record size               |
| G11 | HIGH     | Backup confidence   | No end-to-end live-snapshot → verify→repair → open test             |
| G12 | LOW      | Operator ergonomics | No `filestore backup` / `restore` wrapper commands                  |

---

## Build order

All twelve gaps are in scope for this branch. A working build order that
front-loads the on-disk-format changes (so later work doesn't have to
re-migrate) is:

**Phase A — on-disk format changes (do these first, together):**

1. **G1**: length-prefixed record framing in `packed_store.go`. Update
   `scanPackedRecords` and a new `appendPackedRecord` to emit/consume the
   framing. Add a torn-tail unit test.
2. **G2 + G3**: delete `process_lock.go`; swap to `syscall.Flock(LOCK_EX)` on
   the shard fd inside `appendPackedRecord`; keep the in-memory per-key
   mutex.
3. **G7**: introduce `staging/` top-level; move all staging from
   `objects/blake3/xx/yy/.DIGEST.XXXXX/` to `staging/objects/<ulid>/`; keep
   the same atomic-rename-into-final-location pattern. Update
   `path_report.go` classification.
4. **G10**: enforce per-record size cap at the writer.

These four together change every shard format and every staging path. Land
them as one cohesive change so subsequent work has stable foundations.

**Phase B — verify becomes a repair tool:**

5. **G4**: add three packed-shard sweeps (`verifySourceIndexShards`,
   `verifyCompoundParentShards`, `verifyRecoveryShards`). Split the
   `gmeow_corrupt_objects` gauge.
6. **G6**: in those sweeps, build the loose-object → source-index reachability
   cross-check.
7. Add `verify --repair`:
   - truncate torn JSONL tails (uses G1's framing),
   - reap stale `staging/` (uses G7's tree),
   - rewrite missing source-index entries from manifest `Provenance` (G6),
   - call existing `CleanupSourceLocks` (existing TTL behavior).

**Phase C — dedup and compaction completeness:**

8. **G8**: extend `CompactV1Layout` → `Compact`, with per-shard
   last-write-wins rewrites. Each rewrite is `.next` → fsync → rename →
   fsyncDir.
9. **G9**: add `source-alias-index-v2/` shard family + `aliasKey` derivation
   + `LookupSourceObject` alias fallback + import-time alias write. Extend
   `Compact` to also rewrite alias shards.

**Phase D — operator surface and confidence:**

10. **G5**: rewrite `docs/FILESTORE_BACKUP_RESTORE.md` and the relevant
    section of `docs/architecture/FILESTORE.md` to describe the new
    crash-consistent contract; document `verify --repair`'s role.
11. **G12**: add `filestore backup` and `filestore restore` admin commands.
12. **G11**: ship the integration test (`backup_restore_test.go`)
    exercising every Phase A/B/C invariant — this is the regression net.

---

## Verification

Per phase:

- **A**: `go test ./internal/filestore/...` for the packed reader/writer
  fuzz/torn-tail tests; manual smoke that ingest still works end-to-end.
- **B**: integration test seeds known orphan + torn-tail + stale-staging
  states, runs `verify` (must report each), runs `verify --repair` (must
  repair each), runs `verify` again (must report ok).
- **C**: integration test re-imports the same archive at multiple
  `ExternalVersion` values, asserts (i) zero new blob bytes, (ii) one alias
  record covers all versions, (iii) compaction leaves one source-index record
  per `(refKey, version)`.
- **D**: `filestore backup` + `filestore restore` round-trip a populated
  store; `verify --repair` reports `ok` on the restore destination; all
  digests `Open`-able.

End-to-end manual check before merge:

1. Start the running system, begin a long archive import.
2. Without signaling anything, run `rsync -aH --delete <root> <dest>`.
3. Continue running on the original; on `<dest>`, run
   `gmeow-admin --config <dest>.toml filestore verify --repair`.
4. Confirm: report is `ok` after repair; every digest known to be committed
   before the snapshot is `Open`-able on `<dest>`.
5. Repeat the rsync at random instants and reconfirm.

---

## Files most likely to change

- `internal/filestore/packed_store.go` — G1, G3, G8, G9 (alias shard), G10
- `internal/filestore/process_lock.go` — deleted (G2)
- `internal/filestore/store.go` — G7 (staging path), G6 (callers), write-ordering audit (G5)
- `internal/filestore/source_ingest.go` — G6, G9 (alias key + lookup)
- `internal/filestore/maintenance.go` — G8 (Compact extension), G7 reaper, G2/G3 cleanup
- `internal/filestore/verify.go` — G4, G6, G7 (with `--repair`)
- `internal/filestore/path_report.go` — G7 (new `staging/` paths), G9 (alias shards)
- `internal/filestore/storage_report.go` — G9 (alias shard accounting)
- `internal/filestore/interfaces.go` — alias lookup if it changes the public Store contract
- `internal/cli/filestore.go` — G4 (`verify --repair`), G12 (`backup`, `restore`)
- `internal/filestore/backup_restore_test.go` (new) — G11
- `docs/architecture/FILESTORE.md`, `docs/FILESTORE_BACKUP_RESTORE.md` — G5, G7, G9, G12
