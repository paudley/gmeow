# ANALYSIS Architecture

ANALYSIS workers enrich FILESTORE objects. They do not talk to Gmail, QUERY,
PostgreSQL, RabbitMQ, or backend APIs directly.

## Worker Flow

Workers receive scheduler-owned jobs, read FILESTORE manifests and content
through `FilestoreService` gRPC, check whether the exact analyzer name/version
annotation already exists, and run only missing work. Results are written back
to FILESTORE as annotations, and jobs are acked only after durable writes.

Workers are concurrent but analyzer-specific model calls are constrained where
required. Summary endpoint calls are serialized per worker instance. All
embedding work routes through the EMBEDDING gRPC service; analyzers, importers,
QUERY commands, and interfaces must never call an embedding HTTP endpoint
directly. EMBEDDING owns its managed embedding backend, batching, and the
model-scoped `email_segment` cache namespace. Model failures route back through
SCHEDULER retry handling.

Each analyzer drains its own SCHEDULER work queue through a dedicated consumer
pool, so a slow model-backed analyzer never head-of-line-blocks a fast
in-process one. Pool size is the per-analyzer `workers` count in config (falling
back to `worker_concurrency`); idempotent re-delivery plus an in-memory
annotation-presence cache make a re-seen object a no-op before any FILESTORE
read. External Python adapters run as a persistent backend
(`gmeow-intel serve <analyzer>`): the model loads once, the adapter emits a
ready sentinel, then handles one request per stdin line, eliminating per-object
model reloads.

## Analyzer Set

The email cutover analyzer versions are `phase04-email-v2` for Go analyzers and
`python-email-v1` for explicit external Python adapters. Older `phase04` and
`python-current` annotations are stale and must be rescheduled before cutover
validation passes.

Go analyzers include text extraction, RFC822/header parsing, metadata
extraction, graph fact extraction, EMBEDDING-service-backed email embeddings,
and model-backed summaries. NER and categorization remain explicit
`gmeow-intel` external adapters when Go parity is not proven.

## Quality Rules

Analyzer output must be real production data. Placeholder, regex substitute,
keyword-only degraded output, missing dependency fallback, empty model output,
echoed prompt output, or malformed model JSON is a hard failure.

External analyzer adapters are configured explicitly with command, args,
timeout, name, and version. Commandless Python/model analyzers fail closed at
worker startup.

## Compound Refresh

Subobject analysis can affect compound parent structure. Parent refresh is
scheduled through normal FILESTORE/SCHEDULER/QUERY flow so QUERY can lag and
catch up from FILESTORE authority.
