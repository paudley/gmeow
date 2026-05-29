<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Local Source-Checkout Deployment Notes

These notes capture how the operator's local gmeow deployment is wired when the
systemd units in `deploy/systemd/` are driven directly from a source checkout
rather than packaged binaries. See `docs/systemd.md` for the canonical unit
topology and hardening.

## Run model

- gmeow runs as **system** units (not `--user`) under the umbrella
  `gmeow.target`. Start or stop the whole stack with
  `sudo systemctl start|stop gmeow.target`. Restart cleanly with `stop` then
  `start`: restarting an already-active target does not re-trigger member
  services that were individually stopped.
- Each `gmeow-*.service` carries a drop-in at
  `/etc/systemd/system/<unit>.d/override.conf` that runs the service as the
  operator user and points `ExecStart`/`ExecStartPre` at the checkout's `bin/`
  (e.g. `<checkout>/bin/gmeow ... <subcommand>`) with `WorkingDirectory` set to
  the checkout. Rebuild those binaries with `make go-build` before restarting;
  `go run` is not used for managed services.
- Because of the drop-ins, the packaged `gmeow` system user and
  `/usr/local/bin/gmeow` from the base unit files are not required.

## External dependencies

- PostgreSQL and RabbitMQ are provided **outside** systemd. Their
  `systemctl is-active` state is irrelevant; the units' `After=` ordering on
  `postgresql.service` is a no-op when that unit is absent.
- Config lives at `/etc/gmeow/gmeow.toml`. It must be readable by the service
  user — keep it group-readable by that user (mode `0640` with the operator
  group). Reinstalling it as `root:root` will cause `permission denied` at
  `ExecStartPre` (`gmeow status`).
- The SOPS age identity is read from `GMEOW_SOPS_UNLOCK_KEY` or
  `~/.config/gmeow/key.txt`.

## Fresh database

- A new PostgreSQL database needs the QUERY schema applied before serving:
  `gmeow-admin --config /etc/gmeow/gmeow.toml query migrate`. `query-serve` also
  auto-applies migrations on startup; the migrations live in `migrations/query`
  and resolve relative to the config directory, then the working directory.

## JMAP authentication

- `gmeow-jmap.service` (loopback HTTP, default port `8767`) authenticates with
  QUERY/PostgreSQL-backed bearer tokens, not a configured password. The JMAP
  `[[interfaces]]` entry needs only `kind`, `host`, and `port`.
- Mint a token per client with
  `gmeow-admin --config /etc/gmeow/gmeow.toml query token create --client-id <id>`.
  The command prints a 128-character token once and stores only its BLAKE3 hash
  in PostgreSQL. Clients send it as `Authorization: Bearer <token>`.

## Regenerating protobuf stubs

- Run `make genproto` to regenerate the Go protobuf and gRPC stubs. It invokes
  `protoc` with the repo-relative `bin/protoc-gen-go` and `bin/protoc-gen-go-grpc`
  plugins (build them into `bin/` first if missing). Avoid absolute plugin paths
  on the `protoc` command line.

## Known test-environment caveat

- Integration tests that bind `AF_UNIX` sockets under `t.TempDir()` (for example
  `internal/contracts` phase-7 workflows and parts of `internal/source`) can fail
  with `filestore grpc did not become ready` when `TMPDIR` is long: the socket
  path exceeds the 108-byte `sun_path` limit so the in-process gRPC server never
  binds. This is environmental, not a code regression; run with a short `TMPDIR`
  (e.g. the default `/tmp`).
