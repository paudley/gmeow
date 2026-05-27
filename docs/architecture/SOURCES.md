# SOURCE And BACKEND Architecture

SOURCE/BACKEND services feed external data into FILESTORE and expose
source-specific live operations to INTERFACE. They do not own storage identity,
dedupe, analysis, query projection, or RabbitMQ coordination.

## Source Service Model

Gmail runs as a gRPC source service through `gmeow source-serve <source-name>`.
INTERFACE and admin clients call that service for live mail search, hydrate,
retrieve, action, backfill, and inbox refresh behavior. INTERFACE must not
construct Gmail adapters in-process or call Gmail APIs directly.

Local and push sources submit normalized content to FILESTORE-facing ingest
contracts. Drive remains design-only on this email-focused branch, and
unsupported Drive operations fail through capability checks.

## Gmail Ingest Shape

Gmail messages hydrate into compound `mail_message` objects. The compound
contains Gmail metadata, headers, body, MIME structure, and attachment
subobjects. Shared attachments dedupe globally through FILESTORE.

Before any payload transfer, Gmail checks FILESTORE source identity. On a miss,
it acquires a FILESTORE ingest claim so concurrent hydration attempts for the
same Gmail message/source version serialize at the storage authority.
The lookup is served by FILESTORE's source index; Gmail runtime paths must not
walk the filestore to rediscover historical source provenance.

## Backfill And Inbox Refresh

Gmail backfill pages Gmail, hydrates messages, writes compound objects and
source cursors to FILESTORE, and stops there. FILESTORE notification lets
SCHEDULER derive analysis and projection through the normal path.

When enabled, inbox refresh runs the configured rolling inbox query, defaulting
to recent inbox mail, and stores a separate namespaced cursor under Gmail source
state. It runs independently from historical backfill, so a long backfill does
not block rolling inbox population. Gmail history anchor expiry is recorded in
source state so operators can rerun a full backfill.

## Actions And Capabilities

Gmail actions are deliberately narrow: apply/remove label, archive, mark read,
and star. Actions are gated by adapter capability and object provenance/facet
rules. Source capabilities are validated at startup for enabled operations.
