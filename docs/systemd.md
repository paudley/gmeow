<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# systemd Deployment

Gmeow ships systemd units under `deploy/systemd/` for production service
control. The units model the architecture boundaries in `docs/ARCHITECTURE.md`:
services communicate through gRPC, QUERY owns PostgreSQL access, SCHEDULER owns
RabbitMQ access, and analyzers only read and write FILESTORE.

## Unit Topology

- `gmeow.target` is the umbrella unit for the whole deployment.
- `gmeow-core.target` groups core services:
  `gmeow-filestore.service`, `gmeow-query.service`,
  `gmeow-scheduler.service`, and `gmeow-embedding.service`.
- `gmeow-backends.target` groups source/backend services such as
  `gmeow-source@primary.service`.
- `gmeow-workers.target` groups analysis workers such as
  `gmeow-worker@1.service`.
- `gmeow-interfaces.target` groups user-facing interfaces:
  `gmeow-mcp.service`, `gmeow-rest.service`, `gmeow-imap.service`, and
  `gmeow-jmap.service`.
- `gmeow.slice` provides a shared resource-control cgroup for accounting and
  operator overrides.

The target graph lets operators start, stop, and restart Gmeow as a group while
still allowing independent control over sources, workers, and interfaces.
`PartOf=gmeow.target` propagates stop and restart operations from the umbrella
target to managed services.

## Install

Build the Go binaries with `make go-build`. By default this creates
`./bin/gmeow`, `./bin/gmeow-admin`, and `./bin/gmeow-worker`; package or install
those exact binaries at the paths referenced by the unit files or by a local
systemd drop-in. Do not rely on `go run` for managed services.

Install configuration and secrets under the configuration directory referenced
by the unit files, with directory mode `0750` and file mode `0640`.

The optional environment file referenced by the units is for process
environment required by configuration decryption or the runtime, for example a
SOPS unlock key. Do not put secrets on command lines.

Production file storage should live below the persistent Gmeow state directory
created by the units.

Install every file from `deploy/systemd/` into the system unit directory with
mode `0644`, then reload the systemd manager configuration. The deployment set
is `gmeow.slice`, all `gmeow*.target` files, and all `gmeow*.service` files.

## Enable

Enable and start `gmeow.target`, then enable and start the desired instance
units. A typical email deployment enables `gmeow-source@primary.service`,
`gmeow-worker@1.service`, `gmeow-mcp.service`, `gmeow-rest.service`,
`gmeow-imap.service`, and `gmeow-jmap.service`.

The JMAP interface authenticates clients with QUERY-backed bearer tokens rather
than a configured password. Mint a token for each client with
`gmeow-admin --config <config> query token create --client-id <id>`; the command
prints a 128-character token once and stores only its hash in PostgreSQL. Clients
present it as `Authorization: Bearer <token>`.

Add more source or worker instances with additional template instances such as
`gmeow-source@archive.service` or `gmeow-worker@2.service`.

The source instance name must match a configured source name. For Gmail, the
usual production instance is `gmeow-source@primary.service`.

## Operate

Use the system service manager on `gmeow.target` to start, stop, or restart the
full deployment. Use the role targets to restart only core services, backends,
workers, or interfaces.

Use the service manager status view for `gmeow*` units, the journal for
`gmeow-*` units, and the systemd cgroup views for `gmeow.slice` when inspecting
runtime state.

## Runtime Paths

The units create and use these system paths:

- The configuration directory for configuration.
- The runtime directory for sockets and transient state.
- The state directory for persistent application state and filestore data.
- The logs directory for filesystem-visible logs if a service writes any.

The default service hardening makes the rest of the filesystem read-only to the
service processes. Keep configured writable paths inside the state directory or
add a narrow systemd drop-in for the specific unit that needs another path.

For source checkouts used directly as deployments, point the unit command paths
at the checkout's `bin/` directory and set `WorkingDirectory` plus configured
data paths explicitly in a drop-in. The production checkout used by operators
may keep FILESTORE data at a path such as `~/Active.running/gmeow/data`; that
path must be writable by the service user through the configured systemd
hardening.

## Hardening

The service units use `Type=exec`, bounded restart backoff, explicit config
paths, private temporary and device namespaces, a strict read-only system view,
empty ambient capabilities, `NoNewPrivileges=yes`, process visibility
restriction, address-family restriction to Unix and IP sockets, native syscall
architecture, and a system-service syscall allowlist.

These defaults fit the current production shape:

- Internal service calls use Unix or TCP gRPC sockets.
- Gmail source services call Gmail APIs and FILESTORE-facing ingest contracts.
- QUERY is the only service that needs PostgreSQL access.
- SCHEDULER is the only service that needs RabbitMQ access.
- Analysis workers talk to FILESTORE, SCHEDULER job delivery, and the
  EMBEDDING gRPC service for email embedding vectors.
- Worker units set `UV_CACHE_DIR`, `UV_PROJECT_ENVIRONMENT`, and
  `XDG_CACHE_HOME` under `/var/lib/gmeow` so Python analyzer environments and
  caches stay inside the writable state directory.

If a deployment adds local GPU inference, FUSE mounts, hardware devices, or a
nonstandard writable path, use a systemd drop-in for that one unit instead of
weakening the base unit files. Keep overrides narrow and tied to the service
that needs the extra access.

## Resource Control

All Gmeow services run under `gmeow.slice`. Override the slice to set deployment
budgets.

Common slice overrides include CPU weight, IO weight, memory pressure limits,
and task limits.

Use per-service overrides when a single role needs a different budget.

Common service overrides include memory caps and CPU quotas.

## Validation

Validate unit syntax with the systemd analyzer against the installed
`gmeow*.service`, `gmeow*.target`, and `gmeow.slice` units before cutover.

Inspect effective hardening with the systemd security analyzer for the core
units, the enabled source instances, the enabled worker instances, and the
enabled interface services.

`gmeow-mcp.service` runs the production MCP Streamable HTTP interface. The
`gmeow mcp-serve` command remains available for stdio MCP hosts that own stdin
and stdout directly.
