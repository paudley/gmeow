# JMAP Interface Architecture

Gmeow will expose JMAP as a new interface server over the existing
application-service boundary. JMAP is a protocol surface, not a new storage
owner: QUERY serves indexed JMAP state, FILESTORE remains the durable recovery
source, and source adapters only contribute source-neutral mail metadata.

The initial implementation targets JMAP Core, Mail, Blob Management, and Quota:

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:mail`
- `urn:ietf:params:jmap:blob`
- `urn:ietf:params:jmap:quota`

Submission, vacation response, Sieve, MDN, S/MIME, sharing, contacts,
calendars, WebSocket, and WebPush are intentionally not advertised until they
are implemented.

## Protocol Surface

JMAP runs as a dedicated interface server:

- package: `internal/interface/jmap`
- command: `gmeow jmap-serve`
- config interface kind: `jmap`
- systemd unit: `gmeow-jmap.service`

The HTTP surface is:

- `GET /.well-known/jmap`
- `GET /jmap/session`
- `POST /jmap/api`
- `GET /jmap/download/{accountId}/{blobId}/{name}`

Every JMAP endpoint requires `Authorization: Bearer <token>` except future
health or metrics endpoints if those are added. The v1 bearer token comes from
the configured JMAP interface password. JMAP exposes one initial account:
`gmeow`.

JMAP responses stay protocol-native. The interface must not expose gmeow MCP
operation envelopes or TOON output. Long-running gmeow operation machinery may
inform implementation choices internally, but JMAP clients receive normal JMAP
method responses and JMAP error objects.

## QUERY Serving Model

JMAP is served from QUERY-owned tables. Interface code must not talk directly to
PostgreSQL, FILESTORE internals, Gmail, Teams, or other future source providers.

QUERY owns the online JMAP serving index:

- `jmap_mailboxes`
- `jmap_email_mailboxes`
- `jmap_email_keywords`
- `jmap_email_state`
- `jmap_state_seq`

These tables support fast JMAP reads, filters, counts, and state strings. They
are not the disaster-recovery source of truth. If PostgreSQL is lost, these
tables must be reconstructable from FILESTORE projection data.

Application services provide JMAP-shaped methods over QUERY:

- mailbox get/query/set
- email query/get/set
- thread get
- search snippet get
- blob metadata/lookup
- quota read

`Email/query` is QUERY-native. It must support text search, mailbox filters,
keyword filters, date filters, paging, and received-date sorting without
walking FILESTORE.

## FILESTORE Recovery Contract

FILESTORE is the durable recovery source for all object-local JMAP state.
Deleting PostgreSQL and rebuilding QUERY from the FILESTORE data directory must
preserve:

- custom folders
- message mailbox membership
- `$seen` state
- `$flagged` state
- enough JMAP state sequencing to resume correct behavior after rebuild

Every JMAP mutable state change must write through to FILESTORE as
recoverable object-local state, then update QUERY serving tables. The accepted
consistency model is eventual consistency from FILESTORE to QUERY:

1. JMAP write validates against QUERY state.
2. JMAP write persists recovery state to FILESTORE overlays or annotations.
3. JMAP write updates QUERY serving rows for low-latency read-after-write.
4. Normal projection can later re-read FILESTORE and converge QUERY state.

Projection must never clobber user-edited JMAP state with source-derived
defaults. Source metadata may seed a message's initial JMAP state only when no
existing JMAP state exists for that object.

## Source-Neutral Mail State

JMAP folders and keywords are global gmeow state. They are not provider-native
labels.

System mailbox ids are fixed:

- `all`
- `inbox`
- `sent`
- `drafts`
- `trash`
- `spam`
- `archive`
- `starred`
- `unread`

System mailboxes cannot be deleted. Custom folders are global,
source-neutral, and recoverable from FILESTORE. Gmail `label_ids` and future
Teams metadata are normalized into initial source-neutral state; provider ids
are not exposed as canonical JMAP mailbox ids.

Initial keyword mapping:

- `$seen` is absent when source state indicates unread, present otherwise.
- `$flagged` is present when source state indicates starred or flagged.

JMAP v1 writes are local to gmeow. Provider write-through is out of scope.

## Supported Methods

Read methods:

- `Core/echo`
- `Mailbox/get`
- `Mailbox/query`
- `Thread/get`
- `Email/query`
- `Email/get`
- `SearchSnippet/get`
- `Blob/get`
- `Blob/lookup`
- quota read methods from RFC 9425

Writable methods:

- `Mailbox/set` creates, renames, and deletes custom folders.
- `Email/set` updates `$seen`, `$flagged`, and mailbox membership.

Unsupported methods return standard JMAP errors and are not advertised:

- upload/import/copy/send/submission
- vacation response
- Sieve
- MDN
- S/MIME
- contacts
- calendars
- sharing
- WebSocket
- WebPush

`Email/get` maps canonical gmeow messages into JMAP Email objects. Message ids,
thread ids, headers, body parts, attachments, keywords, and mailbox membership
come from the canonical message view plus QUERY JMAP state. Blob download uses
FILESTORE `Open` through application services.

`SearchSnippet/get` uses QUERY match snippets first, AI summary second, and a
body excerpt last.

## State And Changes

JMAP responses must include state strings for `Email`, `Mailbox`, `Thread`, and
`EmailDelivery`. The first implementation may return polling-safe state tokens
without implementing full `changes` or `queryChanges`, but the table design must
preserve enough sequence information to add those methods without changing the
storage model.

`EmailDelivery` state changes when a new Email is added. Keyword and mailbox
changes update `Email` state but do not need to update `EmailDelivery`.

## Blob And Quota Extensions

Blob Management is a natural fit for gmeow because FILESTORE already addresses
content by digest. `Blob/get` exposes blob metadata and `Blob/lookup` maps body
and attachment blobs back to referencing Email objects.

Quota support exposes operationally useful usage:

- approximate filestore bytes
- message count
- object count where available

If hard limits do not exist, quota limits are reported as absent rather than
invented.

## Acceptance Criteria

Implementation is not complete until the following recovery scenario passes:

1. Ingest mail.
2. Create a custom folder.
3. Move a message into that folder.
4. Mark the message seen and flagged.
5. Delete or recreate the QUERY database.
6. Rebuild QUERY from FILESTORE.
7. Verify JMAP returns the custom folder, mailbox membership, `$seen`, and
   `$flagged` state exactly.

Tests must also prove:

- JMAP auth rejects missing or invalid bearer tokens.
- Session responses advertise only implemented capabilities.
- `Email/query` does not walk FILESTORE.
- JMAP writes persist FILESTORE recovery state before or with QUERY serving
  updates.
- Reprojection does not overwrite user-edited JMAP state with source defaults.
