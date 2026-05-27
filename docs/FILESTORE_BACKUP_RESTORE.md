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
FILESTORE. Do not use `recovery.json` sidecars for normal rebuilds; they are emergency human
recovery aids only.

Normal runtime paths must not walk FILESTORE. Whole-tree walks belong to these
explicit operator workflows: backup verification, restore verification, QUERY
rebuild, changed-projection repair, and scheduler scan/repair. SOURCE lookup
uses `source-index/`, and compound parent refresh uses
`compound-parent-index/`; if either index is missing for historical data,
normal ingest or an explicit repair workflow recreates it.
