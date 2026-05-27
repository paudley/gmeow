# INTERFACE Architecture

INTERFACE exposes Gmeow through protocol-specific surfaces over shared
application services. It does not own storage, queue, analysis, source adapter,
or database transport details.

## Application Services

Application services provide search, mail search, retrieve, structure,
provenance, facets, compound expansion, graph exploration, analysis status,
forced analysis, source actions, and operational status. Protocol packages call
these services rather than concrete FILESTORE, QUERY, SCHEDULER, or SOURCE
implementations.

Application services use gRPC clients for service boundaries. Live Gmail
behavior is reached through the source gRPC server, not by constructing Gmail
adapters inside INTERFACE.

## Protocol Surfaces

- `gmeow mcp-serve` runs stdio MCP for local MCP hosts.
- `gmeow mcp-http-serve` and `gmeow-mcp.service` run production MCP Streamable
  HTTP with stateful sessions and server-sent event streams.
- `gmeow rest-serve` exposes REST endpoints over the same application services.
- `gmeow imap-serve` exposes read-only IMAP over `mail_message` objects.
- CLI workflows use the same app-service layer where they overlap interface
  behavior.

MCP Streamable HTTP supports `POST /mcp`, `GET /mcp`, `DELETE /mcp`,
`Mcp-Session-Id`, `Last-Event-ID` replay, health, metrics, and loopback-first
operation.

## Search Behavior

Generic MCP and REST search can cross facets. `mail_search` is pre-facetted for
mail and can fuse QUERY results with live Gmail results. IMAP is intentionally
facet-limited and read-only.

Forced analysis requests go to SCHEDULER with appropriate priority and update
FILESTORE/QUERY asynchronously through the normal analysis/projection flow.
