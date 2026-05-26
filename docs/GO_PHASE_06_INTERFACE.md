# Go Migration Phase 06 - INTERFACE And Application Services

## Goal

Expose the FILESTORE-first system through shared application services and protocol-specific
interfaces. INTERFACE does not own storage, query, analysis, or source behavior.

## Build

- Implement application services for:
  - search;
  - retrieve;
  - analyze/force analysis;
  - provenance;
  - facet inspection;
  - compound expansion;
  - graph exploration;
  - source actions;
  - operational status.
- Implement MCP tools over application services.
- Implement REST endpoints over the same services.
- Implement `gmeow-admin` operational commands.
- Implement IMAP projection over `mail_message` facet objects.
- Keep facet-filtered endpoints explicit: IMAP only sees `mail_message`, filesystem-style views only
  see `file`/`container`, and generic MCP/REST search can cross facets.

## Retire Python Equivalent

Python interface/server code removed with the old Python PostgreSQL runtime:

- `src/gmeow/app.py`
- `src/gmeow/mcp_server.py`
- `src/gmeow/imap_server.py`
- API/MCP/IMAP tests that target the old Python server behavior

Remove remaining Python presentation helpers as Go interfaces reach parity:

- `src/gmeow/http_json.py`
- `src/gmeow/toon.py` after Go response formatting is implemented
- `src/gmeow/markdown.py` if only used by old responses

## Functional Proof

- MCP and REST return equivalent results for shared operations.
- `mail_search("bob@dob.com")` fans out to QUERY and Gmail, fuses results, fetches missing structure
  from FILESTORE, and returns pending states for fresh live hits.
- Forced analysis from MCP/REST raises priority and eventually updates FILESTORE/QUERY.
- IMAP only exposes objects with the `mail_message` facet.
- Source-specific actions reject inapplicable objects through capability checks.

## Exit Gate

- All public interaction planes call application services, not concrete storage/query/source code.
- Protocol-specific behavior is limited to request parsing and response shaping.
- Old Python REST/MCP/IMAP/CLI code is removed once Go INTERFACE is the documented runtime.
