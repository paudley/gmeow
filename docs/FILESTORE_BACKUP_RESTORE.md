# FILESTORE Backup And Restore

FILESTORE is the authority. PostgreSQL QUERY state is disposable projection data and RabbitMQ holds
durable work messages that can be redelivered or rederived.

## Backup

1. Stop `gmeow`, `gmeow-worker`, FILESTORE, QUERY, and SCHEDULER services.
2. Back up `gmeow.toml`, SOPS age identity material, and the configured `filestore.root`.
3. Keep file permissions intact for config, secret material, and object directories.
4. Restart services and run `gmeow-admin filestore verify`.

## Restore Drill

1. Restore the config, SOPS key, and FILESTORE tree into a temporary target.
2. Run `gmeow-admin --config <restore.toml> filestore verify`.
3. Run `gmeow-admin --config <restore.toml> query migrate`.
4. Run `gmeow-admin --config <restore.toml> query rebuild --confirm-instance <instance_id>`.
5. Run `gmeow-admin --config <restore.toml> scheduler scan`.
6. Smoke test `gmeow --config <restore.toml> search <synthetic-term>` and object retrieval.

If PostgreSQL is lost, recreate the database/extensions, run migrations, and rebuild QUERY from
FILESTORE. Do not use recovery records for normal rebuilds; they are emergency human recovery aids
only. Export one record with `gmeow-admin --config <restore.toml> filestore export-recovery
--digest <digest>`.

Normal runtime paths must not walk FILESTORE. Whole-tree walks belong to these
explicit operator workflows: backup verification, restore verification, QUERY
rebuild, changed-projection repair, and scheduler scan/repair. SOURCE lookup
uses packed `source-index-v2/` shards, and compound parent refresh uses packed
`compound-parent-index-v2/` shards; if either index is missing for historical
data, normal ingest or an explicit repair workflow recreates it.

## Metadata Maintenance

Current FILESTORE roots write packed v2 metadata:

- `source-index-v2/<hh>/<hh>/records.jsonl`
- `compound-parent-index-v2/blake3/<hh>/<hh>/records.jsonl`
- `recovery-v2/blake3/<hh>/<hh>/records.jsonl`

Legacy roots may still contain `source-index/`, `compound-parent-index/`, and
per-object `recovery.json` files. Readers fall back to those files, but they can
be migrated into packed shards:

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

The storage report uses filesystem-allocated bytes as the primary total and
also shows logical file sizes. Compound objects include part objects unless
`--no-recursive-parts` is set.

Resolve an arbitrary FILESTORE path back to its owning data with:

```
gmeow-admin --config <restore.toml> filestore path /absolute/path/inside/filestore
gmeow-admin --config <restore.toml> filestore path source-index-v2/aa/bb/records.jsonl --json
```

The path resolver accepts absolute or FILESTORE-relative paths, rejects paths
outside the configured root, and summarizes packed shard records with a bounded
sample controlled by `--records-limit`.
