# Gmeow Source Package

The `gmeow` package contains transitional Python support code that remains useful while the Go
runtime takes over source, analysis, and interface behavior. The old Python PostgreSQL cache and
Alembic runtime have been retired.

## Package Layout

- **Runtime settings** — [`runtime_config.py`](./runtime_config.py) contains resolved settings
  records for transitional Python tests and later-phase worker behavior. It does not parse TOML,
  SOPS, or operator config files.
- **Protocols** — [`protocols.py`](./protocols.py) hosts the shared `SyncCache`, `IntelligenceCache`,
  `GmeowCache`, `SyncSemantic`, `IntelligenceSemantic`, `GmeowSemantic`, `IntelligenceGraph`,
  `NoGraph`, and `SyncAttachments` structural types used across sync and intelligence.
- **Cache helpers** — [`cache.py`](./cache.py).
- **Sync orchestration** — [`sync.py`](./sync.py) and
  [`sync_backfill.py`](./sync_backfill.py).
- **Gmail client** — [`gmail.py`](./gmail.py) plus shared mutations in
  [`gmail_actions.py`](./gmail_actions.py).
- **Intelligence and graph** — [`intelligence.py`](./intelligence.py),
  [`graph.py`](./graph.py), [`kg.py`](./kg.py),
  [`categories.py`](./categories.py), [`semantic.py`](./semantic.py), [`text_index.py`](./text_index.py).
- **Operations** — [`resilience.py`](./resilience.py), [`provision.py`](./provision.py),
  [`metadata.py`](./metadata.py), [`headers.py`](./headers.py),
  [`http_json.py`](./http_json.py), [`markdown.py`](./markdown.py),
  [`parser.py`](./parser.py), [`toon.py`](./toon.py), [`_typing.py`](./_typing.py).

## External Tool Prerequisites

- **Tesseract** for OCR on image attachments when enabled by resolved runtime settings.
- **Pandoc** for `.docx` / `.odt` / `.rtf` extraction when enabled by resolved runtime settings.
- **Exiftool** for image and document metadata (optional, surfaces in
  `metadata.extract_exiftool_metadata`).
- **Nomic embedding endpoint** running locally at the resolved runtime endpoint when transitional
  Python semantic tests exercise that path.

## Operational Notes

- The Gmail mutation helper surface is intentionally narrow: only label, archive, mark-read, and
  star.
- The compact response encoding helper is TOON; see [`toon.py`](./toon.py) for the encoding rules.
