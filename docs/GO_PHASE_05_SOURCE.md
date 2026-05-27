# Go Migration Phase 05 - SOURCE Adapters

## Goal

Move data ingress and live source capabilities into Go SOURCE adapters that submit normalized content
to FILESTORE. SOURCE adapters may search, hydrate, backfill, export, or only push records, but they
do not own storage identity, dedupe, analysis, or query behavior.

## Build

- Define normalized source event contracts.
- Implement local filesystem importer for repeatable fixtures.
- Implement push/spool ingestor for ringme-style producers.
- Implement Gmail adapter:
  - ingest;
  - hydrate;
  - live search;
  - live retrieve;
  - cursor/backfill using Gmail `messages.list` pagination, Gmail history
    cursors, and FILESTORE source cursor persistence;
  - actions;
  - compound `mail_message` object creation with metadata/header/body/MIME-structure/attachment
    subobjects and no raw RFC822 duplicate when decomposed parts are stored.
- Implement Drive adapter design and the first agreed slice.
- Store source cursors in FILESTORE source-state annotations and project them into QUERY.
- Backfill is operator-triggered through `gmeow-admin source backfill
  <source-name>`. Production-like instances require `--confirm-instance`.
  `--dry-run` lists and hydrates one page without writing FILESTORE. Normal runs
  resume from FILESTORE source cursors, write a cursor after each completed page,
  and rely on FILESTORE source identity lookup/claims for idempotency.
- Gmail history sync uses the stored `history_anchor`; if Gmail reports the
  anchor expired, the cursor records `history_expired=true` and the operator must
  rerun a full backfill.
- Validate source capabilities at startup for enabled operations.
- Before streaming payload bytes, perform FILESTORE source identity lookup by
  `(source_kind, source_name, external_id, external_version)`.
- On lookup miss, acquire a FILESTORE source ingest claim for that source identity before opening
  the remote payload stream; release the claim after successful FILESTORE commit or failure cleanup.
- Stream payload bytes directly from the source network response to FILESTORE. SOURCE adapters must
  not prehash, spool, or write payload bytes to local disk.
- Submit objects through the typed gRPC `FilestoreService`. Payload bytes use the streaming
  `PutObjectFrame` path; source identity lookup and ingest claims use typed protobuf messages.

## Retire Python Equivalent

Remove Python source and Gmail-specific ingestion code as Go adapters reach parity:

- `src/gmeow/gmail.py`
- `src/gmeow/sync.py`
- `src/gmeow/sync_backfill.py`
- `src/gmeow/gmail_actions.py`
- `src/gmeow/provision.py` once Go admin provisioning exists
- source-specific tests that only validate old Python Gmail/cache behavior

## Functional Proof

- Push-only data appears in FILESTORE, schedules analysis, and becomes searchable after projection.
- Gmail live search returns existing FILESTORE digests without rewriting objects.
- Gmail live search hydrates missing messages into compound `mail_message` objects.
- Gmail backfill hydrates paginated mailbox messages into the same compound
  `mail_message` shape as live hydrate, including headers, body, Gmail metadata,
  MIME structure, and attachments.
- Shared attachments dedupe globally through FILESTORE.
- Repeated source identity/version hits do not transfer payload bytes.
- Concurrent hydration attempts for the same Gmail message/source version are serialized by a
  FILESTORE source ingest claim.
- Source actions are gated by source capability and object provenance/facet rules.
- Unsupported Drive/ringme operations fail at the service boundary with actionable errors.

## Exit Gate

- SOURCE adapters submit bytes or stable compound IDs to FILESTORE and never compute dedupe.
- SOURCE adapters use lookup and ingest claims for stable source identities before streaming.
- SOURCE adapters never write source payload bytes to local temp files or staging areas.
- Gmail no longer defines the core data model.
- Old Python Gmail/source ingestion code is removed once Go SOURCE owns ingress and actions.
