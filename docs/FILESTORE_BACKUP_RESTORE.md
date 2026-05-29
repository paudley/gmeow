# FILESTORE Backup And Restore

FILESTORE is the authority. PostgreSQL QUERY state is disposable projection data and RabbitMQ holds
durable work messages that can be redelivered or rederived.

## Backup

FILESTORE is crash-consistent at every instant. Backups may be taken while the
system is actively writing — no quiesce, pause, or signal coordination is
required. Any tool that produces a point-in-time copy of a directory tree is
sufficient:

```
rsync -aH --delete <filestore.root>/ <dest>/
```

The `staging/` directory contains transient in-flight data and may be excluded
from backups without loss:

```
rsync -aH --delete --exclude=staging/ --exclude='*.next' <filestore.root>/ <dest>/
```

Also back up `gmeow.toml`, SOPS age identity material, and keep file
permissions intact for config, secret material, and the FILESTORE root (the
`chunk-packs/` and `metadata/` stores).

A backup snapshot is indistinguishable from a hard power-loss snapshot of the
live process. If the store survives a hard kill, it survives a backup.

## Restore

1. Restore the config, SOPS key, and FILESTORE tree into the target location.
2. Run `gmeow-admin --config <restore.toml> filestore verify --repair`.
3. Verify reports findings that are the expected post-snapshot artifacts (torn
   JSONL tails, stale staging dirs, missing source-index entries) and repairs
   them automatically. If unrepairable findings remain, the source filesystem
   was corrupted or the backup tool lost data.
4. Run `gmeow-admin --config <restore.toml> query migrate`.
5. Run `gmeow-admin --config <restore.toml> query rebuild --confirm-instance <instance_id>`.
6. Run `gmeow-admin --config <restore.toml> scheduler scan`.
7. Smoke test `gmeow --config <restore.toml> search <synthetic-term>` and object retrieval.

If PostgreSQL is lost, recreate the database/extensions, run migrations, and rebuild QUERY from
FILESTORE. Do not use recovery records for normal rebuilds; they are emergency human recovery aids
only. Export one record with `gmeow-admin --config <restore.toml> filestore export-recovery
--digest <digest>`.

Normal runtime paths must not enumerate FILESTORE. Whole-store scans belong to
these explicit operator workflows: backup verification, restore verification,
QUERY rebuild, changed-projection repair, and scheduler scan/repair. SOURCE
lookup and compound parent refresh read the source-index (`si/`/`sa/`) and
compound-parent (`cp/`) records from the metadata LSM; if an index record is
missing for historical data, normal ingest or an explicit repair workflow
recreates it.

## On-Disk Layout

A current FILESTORE root has two stores plus operational scratch:

- `chunk-packs/` — append-only, BLAKE3-addressed, zstd-compressed content
  chunks in large sealed pack segments (the data tier).
- `metadata/` — the embedded Pebble LSM holding manifests (`m/`), content
  recipes (`r/`), chunk index (`c/`), annotations (`a/`), and the
  source/alias/compound-parent/recovery indexes (`si/`, `sa/`, `cp/`, `rec/`).
- `source-cursors/`, `source-locks/`, `staging/` — cursors, ingest claims, and
  in-flight upload scratch.

Back up the entire FILESTORE root as a unit: a chunk pack is only meaningful
together with the metadata LSM that indexes it. Take a consistent snapshot
(stop `filestore-serve`, or use a filesystem/volume snapshot) so the Pebble WAL
and the packs are captured at the same point.

## Metadata Maintenance

A pre-migration root may still contain legacy on-disk metadata: per-object
directories (`objects/blake3/.../manifest.json.zst`, `blob.zstd`,
`recovery.json`), the v1 `source-index/` and `compound-parent-index/` trees, and
the `*-index-v2`/`recovery-v2` packed JSONL shards. Readers still understand
these, and `compact` migrates them into the metadata LSM (and deduplicates
legacy shards, keeping only the last record per key):

```
gmeow-admin --config <restore.toml> filestore compact --dry-run
gmeow-admin --config <restore.toml> filestore compact --confirm-instance <instance_id>
```

Clean stale ingest-lock inode buildup with:

```
gmeow-admin --config <restore.toml> filestore cleanup-locks --confirm-instance <instance_id>
```

Inspect the storage footprint of a suspicious object with:

```
gmeow-admin --config <restore.toml> filestore storage --digest <digest>
gmeow-admin --config <restore.toml> filestore storage --message-id '<message-id>' --json
```

The storage report shows each object's logical footprint — manifest bytes in
the metadata LSM and content bytes in the chunk packs — plus any legacy on-disk
files still present. Because metadata and content are shared/deduped, these are
logical bytes rather than exclusive per-object allocations. Compound objects
include part objects unless `--no-recursive-parts` is set.

Resolve an arbitrary FILESTORE path back to its owning data with:

```
gmeow-admin --config <restore.toml> filestore path /absolute/path/inside/filestore
gmeow-admin --config <restore.toml> filestore path chunk-packs/0000000000.pack --json
```

The path resolver accepts absolute or FILESTORE-relative paths, rejects paths
outside the configured root, and (for legacy packed shards) summarizes records
with a bounded sample controlled by `--records-limit`.
