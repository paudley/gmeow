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

## Ingestion And Import Integrity

All ingestion and import processes MUST import 100% of all input data they are
given or import none of it. There is no acceptable partial-ingest mode.

This applies to mail archives, Gmail/source hydration, contact imports, analysis
payload imports, and any future source adapter. If an input format contains
fields that are not yet semantically understood, the importer must still map
those fields into explicit, typed, documented structures. Storing the whole input
as a single opaque blob does not satisfy this requirement. Neither does dumping
unsupported data into a generic "unknown fields", "extra fields", "raw fields",
or catch-all metadata bag. If a field cannot be given a deliberate typed mapping,
the importer must fail closed before committing an incomplete import.

Agents must not add "best effort", "known fields only", truncating, lossy,
skip-on-error, or silently-dropping import behavior. Any change to an
ingestion/import path must include tests that prove every supported source field
is deliberately mapped, or that the whole input is rejected without persistence.

Contact imports are domain-scoped but maximal: if an input contains
contact-related people, organizations, identities, accounts, relationships,
events, media, roles, credentials, or provenance, those contact-domain facts
must be mapped through standards-first RDF or the contact input must fail. Use
well-known vocabularies before local Gmeow terms. Local ontology terms must use
the Blackcat/Gmeow namespace, never a personal domain namespace.

Every contact import source must specify an import importance level from 0 to
10. Projection treats the contact's importance as the maximum level asserted by
imports or later updater claims. Contact analysis modules are updaters: they add
separate FILESTORE-backed, provenance-bearing RDF claims after ingestion rather
than enriching or mutating the source import in place.

Contact temporal claims must distinguish source observation time, real-world
validity time, and interaction evidence. Import/file/archive timestamps may
support source observation provenance, but must not be promoted to account,
relationship, or contact-method validity. Curated provider lifecycle facts may
set hard validity bounds only for the exact affected service endpoint, such as
legacy IM handles, and must not invalidate broader accounts, email addresses,
migrated identities, profiles, or people.
