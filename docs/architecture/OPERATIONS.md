# Operations, Hardening, And Python Replacement

The Go runtime is the application runtime. The old Python application package,
old PostgreSQL cache, old MCP/REST/IMAP server code, and old migration stack are
not part of production operation on this branch.

## Production Operation

Production service control is documented in `docs/systemd.md`. The deployment
uses an umbrella `gmeow.target`, role targets for core services, backends,
workers, and interfaces, and a shared `gmeow.slice` for resource control.

Core service units run FILESTORE, QUERY, and SCHEDULER. Backend units run source
servers such as Gmail. Worker units run ANALYSIS. Interface units run MCP
Streamable HTTP, REST, and IMAP.

## Hardening

Systemd units use explicit config paths, optional environment files for process
environment, restart backoff, strict filesystem protections, private temporary
and device namespaces, no ambient capabilities, native syscall architecture,
and system-service syscall filtering.

Services are expected to bind loopback by default for user-facing HTTP/IMAP
interfaces. Gmeow is designed for trusted single-user local systems and should
not be exposed directly to untrusted networks.

## Retained Python Scope

`python/` contains `gmeow-intel`, the retained external ANALYSIS adapter
package. It can provide Python-native NER and categorization behavior through
explicit worker-managed adapter commands.

`gmeow-intel` is not the application server, does not own config loading, does
not publish old runtime interfaces, and does not own scheduling or projection.

## Release Readiness

Before a public push or PR:

- `make check` must pass.
- `scripts/public_release_check.py` must pass.
- `gmeow.toml-example` must remain sanitized.
- No mailbox data, service-account keys, OAuth client IDs, local paths, or
  production credentials may be tracked.
- Go binaries/systemd docs are the supported application deployment path.
- PyPI publication is limited to `gmeow-intel`.
