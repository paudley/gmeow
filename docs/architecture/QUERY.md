# QUERY Architecture

QUERY owns the PostgreSQL projection over FILESTORE authority. PostgreSQL is an
index and query engine, not the source of truth.

## Projection

QUERY projects FILESTORE manifests and annotations into relational tables,
pgvector rows, and Apache AGE graph state. Projected data includes facets,
provenance, relationships, compound parts, graph facts, keywords, embeddings,
overlays, source cursors, and analysis status.

Projection refresh is scheduled through SCHEDULER after FILESTORE changes.
QUERY does not listen to RabbitMQ directly; SCHEDULER coordinates projection
work and calls QUERY over gRPC.

## Rebuild

PostgreSQL can be wiped and rebuilt from FILESTORE. Rebuild walks authoritative
object manifests and annotations under the FILESTORE root and repopulates QUERY
state. Recovery sidecars are not projection inputs.

Incremental projection uses changed FILESTORE state and preserves the same
authority model as full rebuild.

## Query Surfaces

QUERY exposes typed gRPC for search, projection, structure-related projected
data, graph exploration, analyzer status, source cursor projection, and
operational status. INTERFACE and admin workflows access QUERY through those
contracts.

SQL stays inside QUERY. Calls are parameterized through Go database APIs and are
not assembled from interface-level string formatting.
