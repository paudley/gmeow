<p align="center">
  <img src="https://raw.githubusercontent.com/paudley/gmeow/main/docs/gmeow-logo.svg" alt="Gmeow logo" width="280">
</p>

# Gmeow

FILESTORE-first local knowledge services for agents.

Gmeow is the Go runtime for FILESTORE-first local knowledge services. This branch replaces the old Python application with typed Go services for config validation, FILESTORE authority, rebuildable PostgreSQL QUERY projection, RabbitMQ-backed SCHEDULER work derivation, ANALYSIS workers, SOURCE/BACKEND services, and MCP/REST/IMAP/JMAP interfaces.

Gmeow is designed for trusted single-user local systems. By default it binds to `127.0.0.1` and does not add application-level authentication. Do not expose it directly to an untrusted network.

## Current Go Runtime

- Validates one shared `gmeow.toml` config path for all Go binaries.
- Stores authoritative content as deduplicated, content-addressed chunks and keeps manifests, annotations, and indexes in an embedded metadata key-value store (no per-object directories).
- Projects FILESTORE manifests into PostgreSQL QUERY tables, pgvector rows, and Apache AGE graph state.
- Derives analysis work through SCHEDULER and publishes durable RabbitMQ jobs with retry and dead-letter handling.
- Runs typed gRPC service endpoints for FILESTORE, QUERY, and SCHEDULER over Unix sockets by default.
- Keeps concrete backend ownership isolated: QUERY owns PostgreSQL, SCHEDULER owns RabbitMQ,
  ANALYSIS only reads/writes FILESTORE, and services communicate over typed gRPC boundaries.
- Provides admin commands for config, FILESTORE verification, QUERY projection, read-only archive mail import, and SCHEDULER operations.
- Runs Go ANALYSIS workers that consume scheduler jobs, read/write FILESTORE through typed gRPC, and support explicit `gmeow-intel` external analyzer adapters for Python/model behavior.
- Provides Go SOURCE adapters for local filesystem fixtures, push/ringme records, the Gmail SOURCE adapter for ingest/hydrate/live search/live retrieve/actions, and design-only Drive capability checks.
- Exposes shared application services through stdio `gmeow mcp-serve`, Streamable HTTP `gmeow mcp-http-serve`, `gmeow rest-serve`, read-only `gmeow imap-serve`, and HTTP `gmeow jmap-serve`; user-facing `search`, `mail-search`, `retrieve`, `ops-status`, and `force-analysis` commands use the same service layer.

## Features

- BLAKE3 content-addressed chunk storage with zstd (dictionary) compression, an embedded Pebble metadata LSM, and immutable recovery records.
- Atomic manifest, analysis, overlay, and source-cursor annotations.
- Rebuildable PostgreSQL projection for facets, provenance, relationships, compound parts, analysis status, graph facts, keywords, embeddings, overlays, and source cursors.
- Read-only Apache AGE graph inspection over projected graph facts.
- Go SCHEDULER work derivation with RabbitMQ priority, retry, and dead-letter queues.
- Go SOURCE adapters submit normalized content to FILESTORE and use source lookup/ingest claims before payload streaming.
- Read-only archive import ingests Maildir, mbox, Evolution, Gnus NNML/MH, Thunderbird/Mozilla, and RFC822/EML directories as `mail_message` compounds with Message-ID dedupe.
- The old Python runtime code is retired. The remaining `python/` package is the explicit ANALYSIS external adapter package.

## FILESTORE Storage Engine

FILESTORE is a content-addressed object store tuned for a workload of many
small, similar records (email) alongside large immutable media and files. It
splits into two tiers:

- **Content → content-defined chunks in append-only packs.** Object bytes are
  split with FastCDC (content-defined chunking), each chunk is addressed by its
  BLAKE3 hash and zstd-compressed (with a trained dictionary for small similar
  records), then appended into large sealed segments under `chunk-packs/`.
  Identical chunks are stored once.
- **Metadata → an embedded Pebble LSM (`metadata/`).** Manifests, content
  recipes, annotations, recovery sidecars, and the source/alias/compound-parent
  indexes are keyed records in one log-structured key-value store. Objects have
  no per-object directory and no per-object files.

**Why it is useful.** A naive "directory of small files per object" layout
rounds every ~600-byte manifest up to a 4K filesystem block and spends another
block on the directory itself — roughly 12 KB on disk for under 1 KB of data
(~15× space amplification), which at 15M emails is ~175 GB of pure overhead.
Packing metadata into an LSM (hundreds of records per block-compressed table
block, near-zero per-record overhead) and content into shared dedup packs
collapses that back to the actual logical size. Content-defined chunking adds
cross-object dedup — quoted email threads, repeated attachments, and document
revisions share chunks — and the store stays fully rebuildable: every chunk is
self-verifying by its hash, and the metadata store is reconstructable by
scanning pack contents. See `docs/architecture/FILESTORE.md` and
`docs/architecture/OBJECT_STORE_SOTA.md`.

**Operating at scale.** The engine is built for the full ~45 TB target:

- **Streaming I/O** — objects are written and read as chunk streams (single-pass
  chunk-and-hash on put, lazy verify-on-read), so multi-gigabyte media and Drive
  files are never buffered whole in memory. A read-side pack-descriptor cache
  avoids reopening a pack per chunk.
- **Space reclamation** — `delete` removes an object's metadata; online `gc`
  (mark-and-sweep over a metadata snapshot, with a grace window so concurrent
  writes are safe) reclaims unreferenced chunk-index entries; `repack` rewrites
  sealed packs to free the bytes on disk. Reclamation runs while the store keeps
  serving.
- **Content-aware compression** — `train-dictionary` trains a zstd dictionary on
  a sample of small objects and installs it; new chunks adopt it immediately and
  existing chunks on the next `repack`.
- **Live operability** — inspection and reclamation commands run against a
  running `filestore-serve` over gRPC (the metadata store is single-writer), so
  integrity checks and space reclamation need no downtime.

## Install

```bash
mkdir -p ~/.config/gmeow
cp gmeow.toml-example gmeow.toml
age-keygen -o ~/.config/gmeow/key.txt
chmod 600 ~/.config/gmeow/key.txt
```

Edit `gmeow.toml`. All binaries use the shared Go config parser. Startup requires a SOPS age identity from `GMEOW_SOPS_UNLOCK_KEY` or `~/.config/gmeow/key.txt`; config validation cannot be disabled. Keep operational TOML fields readable and store referenced `[secrets]` values as SOPS JSON leaf envelopes.

```toml
[system]
config_version = 1
instance_id = "local"
data_dir = "data"

[filestore]
root = "data/filestore"

[rpc.filestore]
network = "unix"
address = "/run/gmeow/filestore.sock"

[rpc.scheduler]
network = "unix"
address = "/run/gmeow/scheduler.sock"

[rpc.query]
network = "unix"
address = "/run/gmeow/query.sock"

[postgres]
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
ssl_mode = "require"

[rabbitmq]
host = "127.0.0.1"
port = 5672
user = "gmeow"
vhost = "gmeow-prod"
test_user = "gmeow-test"
test_vhost = "gmeow-test"

[scheduler]
queue_prefix = "gmeow."

[secrets]
postgres_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
rabbitmq_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
rabbitmq_test_password = '''
{"data":"ENC[AES256_GCM,data:...leaf ciphertext...,type:str]","sops":{"age":[{"recipient":"age1...","enc":"-----BEGIN AGE ENCRYPTED FILE-----\n...\n-----END AGE ENCRYPTED FILE-----\n"}],"version":"3.13.1"}}
'''
```

Do not put `secrets.file`, `secrets.sops_file`, or `config.file` inside `gmeow.toml`. Config source selection belongs to `--config`, `GMEOW_CONFIG`, or the default `gmeow.toml`. Mandatory password leaves must be SOPS-encrypted; plaintext password leaves are rejected.

Gmail SOURCE behavior is implemented by the Go adapter. Service-account credentials that use
Google Workspace domain-wide delegation set `delegated_subject` on the Gmail source while keeping
the credential JSON in the referenced `[secrets]` leaf. Gmail actions remain deliberately narrow:
apply/remove label, archive, mark read, and star. SOURCE actions are gated by adapter capability and
matching object provenance/facets.
When both backfill and inbox refresh are enabled, the source service runs them concurrently. The
inbox refresh uses its own cursor namespace and does not wait for historical backfill to complete.

## Run

```bash
make go-build
bin/gmeow-admin --config gmeow.toml config validate
bin/gmeow --config gmeow.toml status
bin/gmeow --config gmeow.toml query-serve
bin/gmeow --config gmeow.toml scheduler-serve
bin/gmeow --config gmeow.toml filestore-serve
bin/gmeow-admin --config gmeow.toml scheduler run
bin/gmeow-worker --config gmeow.toml run
bin/gmeow --config gmeow.toml mcp-serve
bin/gmeow --config gmeow.toml mcp-http-serve
bin/gmeow --config gmeow.toml rest-serve
bin/gmeow --config gmeow.toml imap-serve
bin/gmeow --config gmeow.toml jmap-serve
```

`make go-build` writes `gmeow`, `gmeow-admin`, and `gmeow-worker` to `./bin/`
by default. Override `BIN_DIR` only when packaging or deploying to an explicit
operator-owned path.

Operational commands include `gmeow-admin filestore verify`, `gmeow-admin filestore storage`,
`gmeow-admin filestore path`, `gmeow-admin filestore delete`, `gmeow-admin filestore gc`,
`gmeow-admin filestore repack`, `gmeow-admin filestore train-dictionary`,
`gmeow-admin filestore compact`, `gmeow-admin filestore cleanup-locks`,
`gmeow-admin query rebuild`, and `gmeow-admin query project-changed --since <RFC3339>` for
FILESTORE verification, object storage inspection, space reclamation (delete → gc → repack),
dictionary training, metadata maintenance, and QUERY projection work. Inspection and
reclamation commands run against a live `filestore-serve` over gRPC and fall back to local
access when it is stopped; `verify --repair`, `compact`, `cleanup-locks`, and `export-recovery`
require `filestore-serve` stopped.
Broad or destructive production-like operations require explicit instance confirmation.
Historical archive mail import is available through
`gmeow-admin source import --source-name <name> <root...>`. Import roots are
read-only inputs: gmeow never rewrites maildir flags, mbox files, NNML folders,
or source archive metadata. Imported messages use normalized RFC Message-ID as
the primary dedupe key; messages without one receive deterministic
`gmeow-generated` Message-IDs. Non-dry-run archive import is queue-backed:
the command walks the roots, enqueues one scheduler-owned source-import job per
message, and drains those jobs in the foreground. Local run state defaults to
`system.data_dir/import-runs` and can be overridden with `--state-dir`.
`--low-noise` records compact archive membership for existing Message-ID/body-line
matches and trivial archive differences without rewriting canonical message
manifests.
QUERY can report archive Message-IDs absent from Gmail with
`gmeow-admin query mail-missing-gmail --source-name <name>`. See
`docs/architecture/VERSIONING.md` for canonical promotion and version scale.
SCHEDULER provides `gmeow-admin scheduler scan`, `status`, `failed`, `dead-letter`, `requeue`, and
`force`; RabbitMQ is mandatory. FILESTORE notifies SCHEDULER over gRPC after object and annotation
writes. Object changes schedule missing/stale analyzer work, while analysis annotation writes enqueue
projection refresh only. Runtime projection refresh messages carry exact object digests; SCHEDULER
loads and projects only those objects. Full FILESTORE walks are reserved for explicit
operator-initiated scan, rebuild, and verification workflows.

Production RabbitMQ vhost and queue prefix are deployment-specific configuration. The example uses
the `gmeow-prod` vhost and `gmeow.` queue prefix; integration tests use the `gmeow-test` vhost and
`gmeow.test.` queue prefix.

ANALYSIS workers are started with `bin/gmeow-worker --config gmeow.toml run`.
Workers check FILESTORE for an existing matching analyzer name/version before execution, so
rescheduled failures or stale RabbitMQ messages skip completed analyzer outputs instead of rerunning
NER, summaries, embeddings, or categorization.
Python/model analyzers must be configured as explicit persistent external adapters, for example
`gmeow-intel serve ner.spacy` or `gmeow-intel serve categories.sklearn`; commandless Python
analyzers fail closed at startup. Configured analyzers must produce real FILESTORE annotations:
spaCy NER uses `en_core_web_sm`, categorization uses the sklearn/main-branch category engine, and
model-backed summary/embedding endpoints fail closed when dependencies, endpoints, or quality
checks are missing. Model-backed summary and embedding calls are serialized per worker instance,
time out after 60s, and failed jobs return through SCHEDULER retry handling with exponential
backoff.

The email production cutover analyzer versions are `phase04-email-v2` for Go analyzers and
`python-email-v1` for Python analyzers. Older `phase04` and `python-current` annotations are stale
by design and must be rescheduled before production validation is considered complete.

The gRPC service protocol is typed protobuf. JSON is limited to dynamic metadata leaf fields, not
whole request or response envelopes. See `proto/gmeow/v1/`, `docs/RUNTIME_ARCHITECTURE.md`,
`docs/ARCHITECTURE.md`, and `docs/systemd.md`.

## Distribution

Go binaries and future container images are released outside PyPI. The only planned PyPI package is
`python/` (`gmeow-intel`), which contains Python-native ANALYSIS workers and shared job/annotation
contract validation.

## Interfaces

MCP tools, REST endpoints, CLI workflows, and read-only IMAP all call the shared Go application
services. Protocol packages parse requests and shape responses; they do not import concrete
FILESTORE, QUERY, SOURCE, or SCHEDULER implementations.

MCP tool result content is TOON-only for agent-facing output. Gmeow does not keep
JSON text or `structuredContent` compatibility for MCP tools; REST and gRPC keep
their existing JSON/protobuf contracts.

JMAP runs as a dedicated HTTP interface server with JMAP Core, Mail, Blob, and
Quota support. It serves query-backed mailboxes, email state, email queries,
threads, blobs, and local mailbox/keyword writes through the same application
service boundaries as MCP and REST. JMAP mutable state is persisted through
FILESTORE recovery data and projected into QUERY-owned serving tables; interface
code does not talk directly to PostgreSQL. See `docs/architecture/JMAP.md`.

## Local Data

By default, local data is ignored by git and stored under `data/`:

- `data/filestore/` is the Go FILESTORE root.
- `data/filestore/chunk-packs/` holds append-only, BLAKE3-addressed, zstd-compressed content chunks
  in large sealed pack segments.
- `data/filestore/metadata/` is the embedded Pebble LSM holding manifests, content recipes,
  annotations, recovery sidecars, ingest claims, and the source/alias/compound-parent indexes
  (key namespaces `m/`, `r/`, `c/`, `a/`, `si/`, `sa/`, `cp/`, `rec/`, `lk/`).
- `data/filestore/dictionaries/` holds trained zstd dictionaries and the `current` marker.
- PostgreSQL, RabbitMQ, object storage, query indexes, analysis annotations, and source state are
  runtime data.

## Archive, IMAP, and JMAP

IMAP is read-only and exposes only objects with the `mail_message` facet. Non-mail facets remain
invisible to IMAP clients.

JMAP is a local interface over the same mail projection, with local mailbox and
keyword state. Provider write-through is out of scope for the initial JMAP
interface.

## Development

The canonical local quality gate is one command:

```bash
make check
```

PostgreSQL-backed integration tests use the mandatory local `gmeow.toml` configuration and the
running local PostgreSQL service. Tests must clean only test-owned rows and must not create or drop
databases or schemas.

Before publishing, run the checklist in `docs/PUBLIC_RELEASE_CHECKLIST.md`.

The detailed runtime architecture lives in `docs/RUNTIME_ARCHITECTURE.md`.
Subsystem architecture docs live under `docs/architecture/`.
The state-of-the-art object-store target (content-addressed packs, DAG/permanode
versioning, recovery model) and its phased path live in
`docs/architecture/OBJECT_STORE_SOTA.md` and
`docs/architecture/OBJECT_STORE_ROADMAP.md`.
FILESTORE backup and restore procedures live in `docs/FILESTORE_BACKUP_RESTORE.md`.
Component ownership and service boundary rules live in `docs/ARCHITECTURE.md`.
Testing architecture and mock-minimization rules live in `docs/TESTING.md`.

## License

Gmeow is dual-licensed for open-source and proprietary/commercial use.

**Open Source License:** Gmeow is available under the GNU Affero General Public
License v3.0 only (AGPL-3.0-only). See `LICENSE`.

**Proprietary License:** Proprietary and commercial licenses are available by
separate written agreement. Contact <oss@blackcat.ca> to discuss commercial
terms.
