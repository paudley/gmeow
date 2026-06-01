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

Runtime projection refreshes are digest-scoped. SCHEDULER resolves the queued
object digests through FILESTORE and sends those projection objects to QUERY.
QUERY changed projection and full rebuild can still walk FILESTORE, but those
paths are explicit admin/recovery operations, not normal runtime refresh.

## Rebuild

PostgreSQL can be wiped and rebuilt from FILESTORE. Rebuild walks authoritative
object manifests and annotations under the FILESTORE root and repopulates QUERY
state. Recovery records are not projection inputs.

Incremental projection uses changed FILESTORE state and preserves the same
authority model as full rebuild. It is an operator command for controlled
catch-up or repair, not the background notification path.

## Query Surfaces

QUERY exposes typed gRPC for search, projection, structure-related projected
data, graph exploration, analyzer status, source cursor projection, and
operational status. INTERFACE and admin workflows access QUERY through those
contracts.

SQL stays inside QUERY. Calls are parameterized through Go database APIs and are
not assembled from interface-level string formatting.

## Contact Import And Export

Contact import writes FILESTORE-authoritative RDF bundle objects and lets normal
projection derive QUERY facts, identity bindings, and rollups. The operator
surface is `gmeow-admin query contact import --format vcard|foaf|native`; it
does not insert rows directly into PostgreSQL.

Contact export reads existing contact aggregate/fact QUERY contracts and renders
vCard, FOAF/RDF Turtle, or Gmeow-native JSON through
`gmeow-admin query contact export --format vcard|foaf|native <contact-id...>`.
Standard exports are intentionally lossy and omit high-cardinality message
back-references; the native behavior is selected with `--format native` and
preserves the projected contact facts and rollup counts available in QUERY.

## Mail Identity Projection

QUERY projects `mail_message` facets, `mail_archive_membership` facets, and
provenance into `query_mail_identities`. This table is rebuildable from
FILESTORE and supports operator questions that compare archive import coverage
against Gmail, such as Message-IDs present in `mail_archive` sources but absent
from Gmail provenance. The projection includes canonical digest, generated-id
state, collision state, version count, and maximum version scale.

`gmeow-admin query mail-missing-gmail` is the admin report surface. It filters by
archive source name, generated Message-ID inclusion, and collision status. The
report reads only QUERY projection tables; it never walks FILESTORE or archive
roots.
