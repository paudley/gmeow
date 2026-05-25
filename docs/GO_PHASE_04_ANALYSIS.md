# Go Migration Phase 04 - ANALYSIS

## Goal

Migrate analysis into idempotent workers that read FILESTORE objects and write FILESTORE annotations.
ANALYSIS works with objects, media types, content roles, dependency outputs, and analyzer specs; it
does not branch on facets.

## Build

- Implement Go worker runtime:
  - RabbitMQ consume/ack;
  - analyzer registry;
  - spec version checks;
  - durable FILESTORE annotation writes before ack.
- Implement initial analyzers:
  - text extraction;
  - RFC822/header parsing;
  - metadata extraction;
  - graph fact extraction;
  - embeddings through configured endpoint;
  - categorization replacement;
  - NER replacement;
  - summary/centroid first pass or explicit placeholder.
- Define analyzer output schemas and contract tests.
- Implement parent compound refresh after subobject analysis commits.
- Keep any external model/runtime integration behind analyzer interfaces and startup validation.

## Retire Python Equivalent

Remove Python analysis code as each Go analyzer reaches functional parity:

- `src/gmeow/attachment_analysis.py`
- `src/gmeow/intelligence.py`
- `src/gmeow/categories.py`
- `src/gmeow/semantic.py`
- `src/gmeow/text_index.py`
- `src/gmeow/headers.py` after RFC822/header analyzer parity
- analysis-related tests that assert old Python outputs or queues

If a specific model must remain Python-backed temporarily, treat it as an external command/runtime
adapter with a documented contract and a removal issue. Do not keep the old Python application as a
library dependency.

## Functional Proof

- Every analyzer is idempotent by digest and analyzer spec version.
- Workers ack only after FILESTORE writes are durable.
- QUERY can lag and later catch up from FILESTORE annotations.
- RFC822/header objects produce identities, graph hints, temporal data, and location hints without
  reading facets.
- File attachments are analyzed like any other file object.
- Compound parent structure reflects subobject analysis after commit.

## Exit Gate

- ANALYSIS has functional worker tests using real RabbitMQ and FILESTORE where practical.
- Analyzer contract fixtures are shared with QUERY and INTERFACE tests.
- Old Python analysis modules are deleted incrementally as their Go replacements are verified.
