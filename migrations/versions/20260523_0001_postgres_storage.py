# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: AGPL-3.0-only
"""Alembic revision: postgres storage catalog.

This revision is part of the Gmeow PostgreSQL schema history. It is applied in order by
``alembic upgrade`` and must be rolled forward rather than amended once shipped.

Revision ID: 20260523_0001
Revises:
Create Date: 2026-05-23
"""

from alembic import op

revision = "20260523_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade."""
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS bloom")
    op.execute("CREATE EXTENSION IF NOT EXISTS cube")
    op.execute("CREATE EXTENSION IF NOT EXISTS age")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS content_objects (
          digest TEXT PRIMARY KEY,
          path TEXT NOT NULL,
          media_type TEXT NOT NULL,
          compression TEXT NOT NULL,
          original_size BIGINT NOT NULL,
          stored_size BIGINT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS labels (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          type TEXT,
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS threads (
          id TEXT PRIMARY KEY,
          snippet TEXT,
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          thread_id TEXT,
          subject TEXT,
          sender TEXT,
          recipients TEXT,
          message_date TEXT,
          snippet TEXT,
          label_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
          headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          raw_json_digest TEXT REFERENCES content_objects(digest),
          text_body_digest TEXT REFERENCES content_objects(digest),
          markdown_digest TEXT REFERENCES content_objects(digest),
          raw_rfc822_digest TEXT REFERENCES content_objects(digest),
          hydrated BOOLEAN NOT NULL DEFAULT false,
          history_id BIGINT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS messages_thread_idx ON messages(thread_id)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_date_idx ON messages(message_date)")
    op.execute("CREATE INDEX IF NOT EXISTS messages_sender_idx ON messages USING bloom(sender)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS message_parts (
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          part_id TEXT NOT NULL,
          mime_type TEXT NOT NULL,
          filename TEXT,
          body_text_digest TEXT REFERENCES content_objects(digest),
          attachment_id TEXT,
          size BIGINT NOT NULL DEFAULT 0,
          headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          PRIMARY KEY (message_id, part_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS attachments (
          sha1 TEXT PRIMARY KEY,
          digest TEXT NOT NULL REFERENCES content_objects(digest),
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          part_id TEXT,
          gmail_attachment_id TEXT,
          filename TEXT,
          mime_type TEXT,
          size BIGINT NOT NULL DEFAULT 0,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          path TEXT NOT NULL,
          compression TEXT NOT NULL DEFAULT 'identity',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS attachments_message_idx ON attachments(message_id)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS priority_rules (
          name TEXT PRIMARY KEY,
          rule_json JSONB NOT NULL,
          priority INTEGER NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_triples (
          subject TEXT NOT NULL,
          predicate TEXT NOT NULL,
          object TEXT NOT NULL,
          source_message_id TEXT,
          UNIQUE(subject, predicate, object, source_message_id)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS graph_triples_subject_idx ON graph_triples(subject)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_triples_object_idx ON graph_triples(object)")
    op.execute("CREATE INDEX IF NOT EXISTS graph_triples_message_idx ON graph_triples(source_message_id)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS intelligence_jobs (
          id BIGSERIAL PRIMARY KEY,
          kind TEXT NOT NULL,
          target_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE(kind, target_id)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          source TEXT NOT NULL,
          default_hidden BOOLEAN NOT NULL DEFAULT false,
          enabled BOOLEAN NOT NULL DEFAULT true,
          description TEXT,
          profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS message_categories (
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          category TEXT NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
          confidence REAL NOT NULL DEFAULT 1.0,
          source TEXT NOT NULL,
          reason TEXT,
          top_terms_json JSONB NOT NULL DEFAULT '[]'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(message_id, category)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS category_rules (
          id BIGSERIAL PRIMARY KEY,
          category TEXT NOT NULL,
          rule_json JSONB NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT true,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS category_overrides (
          message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
          category TEXT NOT NULL,
          action TEXT NOT NULL,
          reason TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(message_id, category)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS learned_category_runs (
          id BIGSERIAL PRIMARY KEY,
          run_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS people_aliases (
          address TEXT PRIMARY KEY,
          person_key TEXT NOT NULL,
          display_name TEXT,
          note TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS embedding_chunks (
          id TEXT PRIMARY KEY,
          source_kind TEXT NOT NULL,
          source_id TEXT NOT NULL,
          message_id TEXT,
          chunk_index INTEGER NOT NULL,
          chunk_count INTEGER NOT NULL,
          text TEXT NOT NULL,
          metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
          embedding vector,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS embedding_chunks_source_idx ON embedding_chunks(source_kind, source_id)")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_issues (
          id BIGSERIAL PRIMARY KEY,
          message_id TEXT,
          source TEXT NOT NULL,
          severity TEXT NOT NULL,
          detail TEXT NOT NULL,
          artifact_digest TEXT REFERENCES content_objects(digest),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    _comment_objects()


def downgrade() -> None:
    """Downgrade."""
    op.execute("DROP TABLE IF EXISTS ingest_issues")
    op.execute("DROP TABLE IF EXISTS embedding_chunks")
    op.execute("DROP TABLE IF EXISTS people_aliases")
    op.execute("DROP TABLE IF EXISTS learned_category_runs")
    op.execute("DROP TABLE IF EXISTS category_overrides")
    op.execute("DROP TABLE IF EXISTS category_rules")
    op.execute("DROP TABLE IF EXISTS message_categories")
    op.execute("DROP TABLE IF EXISTS categories")
    op.execute("DROP TABLE IF EXISTS intelligence_jobs")
    op.execute("DROP TABLE IF EXISTS graph_triples")
    op.execute("DROP TABLE IF EXISTS priority_rules")
    op.execute("DROP TABLE IF EXISTS sync_state")
    op.execute("DROP TABLE IF EXISTS attachments")
    op.execute("DROP TABLE IF EXISTS message_parts")
    op.execute("DROP TABLE IF EXISTS messages")
    op.execute("DROP TABLE IF EXISTS threads")
    op.execute("DROP TABLE IF EXISTS labels")
    op.execute("DROP TABLE IF EXISTS content_objects")


def _comment_objects() -> None:
    comments = {
        "EXTENSION vector": "pgvector extension used for semantic embedding search.",
        "EXTENSION bloom": "PostgreSQL bloom extension used for broad message metadata indexes.",
        "EXTENSION cube": "PostgreSQL cube extension reserved for future analytical indexes.",
        "EXTENSION age": "Apache AGE extension used for property-graph storage alongside relational triples.",
        "TABLE content_objects": "Content-addressed object catalog for every payload stored in filesystem CAS.",
        "COLUMN content_objects.digest": "BLAKE3 digest of the uncompressed canonical payload bytes.",
        "COLUMN content_objects.path": "Absolute or data-root-relative filesystem path to the stored CAS object.",
        "COLUMN content_objects.media_type": "Best-known MIME media type for compression and retrieval decisions.",
        "COLUMN content_objects.compression": "Storage compression codec used for this object, currently identity or zstd.",
        "COLUMN content_objects.original_size": "Uncompressed payload size in bytes.",
        "COLUMN content_objects.stored_size": "Stored payload size in bytes after optional compression.",
        "TABLE labels": "Gmail label catalog resolved from Gmail API IDs to human-readable names.",
        "COLUMN labels.raw_json": "Original Gmail label API object retained for future metadata use.",
        "TABLE threads": "Gmail thread catalog with lightweight thread metadata.",
        "COLUMN threads.raw_json": "Original Gmail thread metadata when available.",
        "TABLE messages": "Message catalog and metadata; large bodies and raw JSON are referenced by CAS digest.",
        "COLUMN messages.raw_json_digest": "CAS digest for the original Gmail message API response.",
        "COLUMN messages.text_body_digest": "CAS digest for extracted text body content.",
        "COLUMN messages.markdown_digest": "CAS digest for generated markdown body content.",
        "COLUMN messages.raw_rfc822_digest": "CAS digest for raw RFC822 message bytes if hydrated.",
        "COLUMN messages.history_id": "Numeric Gmail historyId observed on the message for history cursor tracking.",
        "TABLE message_parts": "MIME tree part catalog for each message.",
        "COLUMN message_parts.body_text_digest": "CAS digest for decoded text-bearing MIME part content.",
        "COLUMN message_parts.headers_json": "Headers present on this MIME part.",
        "TABLE attachments": "Attachment metadata catalog; attachment payload bytes live in CAS.",
        "COLUMN attachments.sha1": "Compatibility identifier for attachment APIs; currently stores the BLAKE3 CAS digest.",
        "COLUMN attachments.digest": "BLAKE3 CAS digest for attachment payload bytes.",
        "COLUMN attachments.metadata_json": "Sidecar metadata, including source Gmail metadata, exiftool output, and later enrichment.",
        "COLUMN attachments.compression": "Storage compression codec used for the attachment CAS object.",
        "TABLE sync_state": "Small key/value store for Gmail sync cursors and summaries.",
        "TABLE priority_rules": "Configured priority sync rules last observed by the service.",
        "TABLE graph_triples": "Relational authoritative knowledge graph triples mirrored best-effort into Apache AGE.",
        "COLUMN graph_triples.source_message_id": "Message that produced the triple, if message-scoped.",
        "TABLE intelligence_jobs": "Asynchronous message and attachment enrichment work queue.",
        "COLUMN intelligence_jobs.status": "Job status: pending, running, done, or failed.",
        "COLUMN intelligence_jobs.attempts": "Number of times the job has been claimed for processing.",
        "TABLE categories": "Manual, system, and learned email categories.",
        "COLUMN categories.default_hidden": "Whether searches should exclude this category unless explicitly included.",
        "COLUMN categories.profile_json": "Learned/manual category profile metadata such as TF-IDF terms and sender examples.",
        "TABLE message_categories": "Category assignments for messages with confidence and provenance.",
        "COLUMN message_categories.top_terms_json": "Top explanatory terms that contributed to the assignment.",
        "TABLE category_rules": "Manual category rules used by the categorization engine.",
        "TABLE category_overrides": "Per-message category include/exclude/confirm decisions.",
        "TABLE learned_category_runs": "Stored outputs from TF-IDF category discovery runs.",
        "TABLE people_aliases": "Manual contact canonicalization from addresses to person keys.",
        "TABLE embedding_chunks": "pgvector semantic search chunks for messages and attachments.",
        "COLUMN embedding_chunks.embedding": "Vector embedding generated by the configured Nomic embedding endpoint.",
        "COLUMN embedding_chunks.metadata": "Search result metadata copied from the indexed source.",
        "TABLE ingest_issues": "Quarantine and salvage notes for corrupt or partially parsed source messages.",
        "COLUMN ingest_issues.artifact_digest": "CAS digest for the problematic source artifact when retained.",
        "SEQUENCE intelligence_jobs_id_seq": "Identifier sequence for asynchronous intelligence jobs.",
        "SEQUENCE category_rules_id_seq": "Identifier sequence for category rules.",
        "SEQUENCE learned_category_runs_id_seq": "Identifier sequence for learned category discovery runs.",
        "SEQUENCE ingest_issues_id_seq": "Identifier sequence for ingest issue records.",
    }
    for object_name, comment in comments.items():
        _safe_comment(f"COMMENT ON {object_name}", comment)

    indexes = {
        "messages_thread_idx": "Accelerates message lookup by Gmail thread.",
        "messages_date_idx": "Accelerates chronological message filtering.",
        "messages_sender_idx": "Bloom index for broad sender matching at large corpus scale.",
        "attachments_message_idx": "Accelerates attachment listing for a message.",
        "graph_triples_subject_idx": "Accelerates graph traversal from subject node.",
        "graph_triples_object_idx": "Accelerates graph traversal from object node.",
        "graph_triples_message_idx": "Accelerates graph lookup by source message.",
        "embedding_chunks_source_idx": "Accelerates semantic chunk replacement by source.",
    }
    for index_name, comment in indexes.items():
        _safe_comment(f"COMMENT ON INDEX {index_name}", comment)


def _safe_comment(prefix: str, comment: str) -> None:
    sql = f"{prefix} IS {json_literal(comment)}"
    escaped_sql = sql.replace("'", "''")
    op.execute(
        f"""
        DO $$
        BEGIN
          EXECUTE '{escaped_sql}';
        EXCEPTION
          WHEN insufficient_privilege OR undefined_object THEN
            NULL;
        END $$;
        """
    )


def json_literal(value: str) -> str:
    """Json literal."""
    return "'" + value.replace("'", "''") + "'"
