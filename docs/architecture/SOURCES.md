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

## Read-Only Archive Import

`gmeow-admin source import <root...>` imports historical mail archives as a
SOURCE workflow. Supported inputs are Maildir, mbox, Evolution mail stores,
Gnus NNML/MH-style numbered folders, Thunderbird/Mozilla mbox trees, and generic
RFC822/EML directories. The importer treats each root as read-only: it never
changes maildir flags, mbox separators, NNML state, client index files, or
archive metadata.

Non-dry-run archive import is direct. `gmeow-admin source import` first walks
the requested roots locally for an accurate message count, then parses each
message and writes it to FILESTORE over gRPC. Mbox discovery records message
offsets and workers parse one mbox message at a time, so large mbox files are
not materialized in memory. Local run records live under `system.data_dir/import-runs`
by default; they are operator-facing summaries, not authoritative mail data.

Archive import jobs write the same `mail_message` compound shape used by Gmail:
headers, canonical text body, MIME structure metadata, archive metadata, and
attachment parts. Exact bytes still dedupe through FILESTORE BLAKE3 identity.
Archive source identity uses `mail_archive/<source-name>/<relative-path>` with
stable version hashes, and the source name defaults to the first root basename
unless `--source-name` is supplied.

RFC Message-ID is the cross-source mail identity. Imported canonical messages
also carry a synthetic FILESTORE source-index entry
`mail_identity/rfc_message_id/<message-id>` so later archive imports can resolve
Message-ID matches without walking FILESTORE or asking QUERY. Messages missing a
usable Message-ID receive deterministic generated IDs under `gmeow.local` based
on normalized selected headers and post-attachment-removal body text.

When a repeated Message-ID has the same canonical body-line fingerprint, normal
import attaches new provenance. Low-noise import instead writes compact
`mail_archive_membership` records, avoiding repeated canonical manifest rewrites
while preserving coverage projection. Trivial archive-only differences follow
the same low-noise membership path. When content differs, the canonical
`mail_message` compound stores generic FILESTORE version records with scale
(`trivial`, `minor`, or `major`) and can promote a better full canonical
version. See `VERSIONING.md` for the shared version set model.

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
