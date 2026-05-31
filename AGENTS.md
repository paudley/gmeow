# Repository Instructions

## Environment Boundary

This checkout is the DEV repository and may use a development-local
`gmeow.toml`.

A separate production checkout may be running via systemd against production
database and queue resources. Treat it as live infrastructure.

If present, read `.env.gmeow-boundary` for local-only machine specifics. That
file is intentionally ignored and must not be committed.

Agents working in this DEV repository must not touch the production checkout,
must not operate on production systemd services, and must not install or replace
production binaries unless the user explicitly asks for a production deployment
task.
