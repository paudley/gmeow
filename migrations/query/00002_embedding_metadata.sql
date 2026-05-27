-- +goose Up
ALTER TABLE query_object_embeddings
  ADD COLUMN IF NOT EXISTS embedding_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS source_digest TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS ordinal INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS text_preview TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE query_object_embeddings
   SET embedding_id = embedding_object_digest
 WHERE embedding_id = '';

CREATE INDEX IF NOT EXISTS query_object_embeddings_kind_idx
  ON query_object_embeddings(kind);

-- +goose Down
DROP INDEX IF EXISTS query_object_embeddings_kind_idx;
ALTER TABLE query_object_embeddings
  DROP COLUMN IF EXISTS metadata_json,
  DROP COLUMN IF EXISTS text_preview,
  DROP COLUMN IF EXISTS ordinal,
  DROP COLUMN IF EXISTS source_digest,
  DROP COLUMN IF EXISTS kind,
  DROP COLUMN IF EXISTS embedding_id;
