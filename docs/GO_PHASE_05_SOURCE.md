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
  - cursor/backfill;
  - actions;
  - compound `mail_message` object creation with body/raw/attachment subobjects.
- Implement Drive adapter design and the first agreed slice.
- Store source cursors in FILESTORE source-state annotations and project them into QUERY.
- Validate source capabilities at startup for enabled operations.

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
- Shared attachments dedupe globally through FILESTORE.
- Source actions are gated by source capability and object provenance/facet rules.
- Unsupported Drive/ringme operations fail at the service boundary with actionable errors.

## Exit Gate

- SOURCE adapters submit bytes or stable compound IDs to FILESTORE and never compute dedupe.
- Gmail no longer defines the core data model.
- Old Python Gmail/source ingestion code is removed once Go SOURCE owns ingress and actions.
