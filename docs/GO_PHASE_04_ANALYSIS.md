# Go Migration Phase 04 - ANALYSIS

## Goal

Migrate analysis into idempotent workers that read FILESTORE objects and write FILESTORE annotations
through `FilestoreService` gRPC. ANALYSIS works with objects, media types, content roles, dependency
outputs, and analyzer specs; it does not branch on facets and does not open FILESTORE paths
directly.

Data quality is a hard gate. A Go-native analyzer may replace Python behavior only when contract
fixtures prove it meets or exceeds the existing Python/tool output. If parity is not proven, the
Python/model/tool implementation remains available only behind an explicit external analyzer adapter;
it must not own config loading, RabbitMQ topology, scheduling, FILESTORE writes, or QUERY projection.

## Build

- Implement Go worker runtime:
  - RabbitMQ consume/ack;
  - FILESTORE object reads and annotation writes through typed gRPC;
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
- Implement external analyzer adapter support for quality-critical model/tool analyzers that cannot
  yet be replaced in Go without output regression. External adapters must be configured explicitly
  with command, arguments, timeout, analyzer name, and analyzer version.
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

Do not retire Python NER, categorization, semantic, or attachment analysis behavior merely because a
Go placeholder exists. Retirement requires equal-or-better fixture results and explicit analyzer
contract parity.

## Functional Proof

- Every analyzer is idempotent by digest and analyzer spec version.
- Workers ack only after FILESTORE writes are durable.
- Workers use `FilestoreService` gRPC for object reads, manifest reads, and annotation writes; direct
  filesystem FILESTORE access is limited to the FILESTORE service itself and tests.
- QUERY can lag and later catch up from FILESTORE annotations.
- RFC822/header objects produce identities, graph hints, temporal data, and location hints without
  reading facets.
- File attachments are analyzed like any other file object.
- Compound parent structure reflects subobject analysis after commit.
- Go-native replacements for Python-backed analyzers pass parity gates before the Python path is
  removed.
- Unregistered Python/model analyzers fail closed rather than falling back to lower-quality Go
  placeholders.
- Python/model analyzers without an explicit external adapter command fail at worker startup.
- Embedding jobs call the configured endpoint and record model, vector, and dimension metadata in
  FILESTORE analysis annotations.
- Summary jobs either write an extractive summary or an explicit placeholder status.

## Exit Gate

- ANALYSIS has functional worker tests using real RabbitMQ and FILESTORE where practical.
- Analyzer contract fixtures are shared with QUERY and INTERFACE tests.
- Old Python analysis modules are deleted incrementally as their Go replacements are verified.
- NER and categorization either meet or exceed the Python implementation in shared fixtures, or remain
  external Python/model adapters managed by the Go worker runtime.
- The configured phase-4 analyzer set includes text, RFC822/header, metadata, graph facts,
  embeddings, summary, and explicit external adapters for NER and categorization when Go parity is
  not proven.
