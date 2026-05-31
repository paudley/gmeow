# INTERFACE Architecture

INTERFACE exposes Gmeow through protocol-specific surfaces over shared
application services. It does not own storage, queue, analysis, source adapter,
or database transport details.

## Surface Scope (load-bearing)

The protocol surfaces are deliberately asymmetric:

- **REST exposes the world.** Every application-service capability — search,
  retrieve, structure, provenance, facets, compound expansion, graph
  exploration, analysis status, forced analysis, source actions, durable
  operation status/result, and operational status — is reachable over REST,
  including the read/write and administrative operations. REST is the complete,
  authoritative surface and should be fully documented as an OpenAPI/Swagger
  spec so the entire API is discoverable and machine-consumable.
- **MCP exposes only what agents need to query and work with objects, and is
  primarily read-only.** The MCP tool set is curated down to the verbs an agent
  uses to find and inspect objects in the store. Administrative, scheduling, and
  operation-lifecycle verbs are intentionally **not** MCP tools; agents that
  need them use REST. Keeping MCP small keeps the agent tool palette focused and
  side-effect-light.

Current MCP tools, all read-only: `object_search`, `mail_search`,
`summary_search`, `similar_messages`, `message_summary`, `object_retrieve`,
`get_structure`, `get_provenance`, `get_facets`, `compound_expand`, and
`graph_explore`. `similar_messages` surfaces the most similar messages to a
given message using that message's stored embedding (no query-text embedding).

Deliberately REST-only (not MCP): `analysis_status`, `force_analysis`,
`source_action` (applies a source-specific action such as a Gmail label change),
`operation_status`, `operation_result`, `operation_resume`, and `ops_status`.

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
- `gmeow rest-serve` exposes REST endpoints over the same application services —
  the complete surface (see Surface Scope), to be published as an OpenAPI/Swagger
  spec.
- `gmeow imap-serve` exposes read-only IMAP over `mail_message` objects.
- `gmeow jmap-serve` exposes JMAP Core/Mail/Blob/Quota over HTTP.
- CLI workflows use the same app-service layer where they overlap interface
  behavior.

MCP Streamable HTTP supports `POST /mcp`, `GET /mcp`, `DELETE /mcp`,
`Mcp-Session-Id`, `Last-Event-ID` replay, health, metrics, and loopback-first
operation.

MCP tool results are agent-facing TOON text. Gmeow does not emit JSON text or
`structuredContent` from MCP tools; JSON remains a REST and dynamic metadata
leaf format, while MCP content is shaped explicitly for TOON-speaking agents.

Long-running MCP tools run as durable interface operations. `mail_search`,
`object_retrieve`, and `graph_explore` return an operation envelope with an
`operation_id`, stream progress when the MCP client supplied a progress token,
and persist their final result through QUERY-owned operation storage. Operation
lifecycle inspection and resume — `operation_status`, `operation_result`,
`operation_resume` — are exposed over REST only (e.g. `POST
/v1/operation_status`, `POST /v1/operation_result`); an MCP client that loses
its progress stream re-checks the operation through REST. Analysis status and
forced analysis are likewise REST-only operations, not MCP tools.

## Search Behavior

Generic MCP and REST search can cross facets. `mail_search` is pre-facetted for
mail and can fuse QUERY results with live Gmail results. IMAP is intentionally
facet-limited and read-only.

Forced analysis requests go to SCHEDULER with appropriate priority and update
FILESTORE/QUERY asynchronously through the normal analysis/projection flow.
