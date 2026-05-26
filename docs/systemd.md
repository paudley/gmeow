# systemd

Go services run as separate systemd units under `gmeow.target`:

- `gmeow-filestore.service` runs `gmeow filestore-serve`.
- `gmeow-query.service` runs `gmeow query-serve`.
- `gmeow-scheduler.service` runs `gmeow scheduler-serve`.

The units in `deploy/systemd/` use `GMEOW_CONFIG=/etc/gmeow/gmeow.toml` and the default Unix
socket endpoints under `/run/gmeow/`. `RuntimeDirectory=gmeow` creates that directory before service
startup. Operators should install the built `gmeow` binary at `/usr/local/bin/gmeow` or edit the
unit `ExecStart` paths.

Install example:

```sh
sudo install -D -m 0644 deploy/systemd/gmeow.target /etc/systemd/system/gmeow.target
sudo install -D -m 0644 deploy/systemd/gmeow-filestore.service /etc/systemd/system/gmeow-filestore.service
sudo install -D -m 0644 deploy/systemd/gmeow-query.service /etc/systemd/system/gmeow-query.service
sudo install -D -m 0644 deploy/systemd/gmeow-scheduler.service /etc/systemd/system/gmeow-scheduler.service
sudo systemctl daemon-reload
sudo systemctl enable --now gmeow.target
```

By default the services communicate over Unix-domain gRPC sockets:

- `/run/gmeow/filestore.sock`
- `/run/gmeow/query.sock`
- `/run/gmeow/scheduler.sock`

TCP is allowed only for loopback addresses in config validation. The service protocol is typed
protobuf; JSON is used only in leaf fields for intentionally dynamic metadata maps.
