# Gmeow Source Package

The `gmeow` package contains only narrow transitional Python helpers that are still useful after
Go phases 0-5. The old Python PostgreSQL cache, Gmail/source ingestion, analysis modules, and
Alembic runtime have been retired. Runtime entrypoints now point operators to the Go binaries.

## Package Layout

- **Runtime settings** — [`runtime_config.py`](./runtime_config.py) contains resolved settings
  records retained for transitional helpers. It does not parse TOML, SOPS, or operator config
  files.
- **Protocols** — [`protocols.py`](./protocols.py) hosts structural protocol types used by
  retained helper modules.
- **Object helpers** - [`cache.py`](./cache.py), [`parser.py`](./parser.py),
  [`graph.py`](./graph.py), [`kg.py`](./kg.py), [`metadata.py`](./metadata.py), and
  [`resilience.py`](./resilience.py) remain as transitional non-interface helpers.
- **Utility helpers** - [`_typing.py`](./_typing.py).

## External Tool Prerequisites

- **Exiftool** for optional metadata helper behavior.
- Python model dependencies live in `python/gmeow_intel` external analyzer adapters.

## Operational Notes

- Gmail/source behavior is implemented in Go SOURCE adapters.
- Public REST, MCP, and IMAP interfaces are implemented by the Go runtime.
