-- +goose Up
CREATE TABLE IF NOT EXISTS query_contact_analysis (
  contact_id TEXT NOT NULL,
  analyzer_name TEXT NOT NULL,
  analyzer_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'complete',
  model TEXT NOT NULL DEFAULT '',
  input_hash TEXT NOT NULL DEFAULT '',
  input_bytes INTEGER NOT NULL DEFAULT 0,
  generated_at TIMESTAMPTZ,
  data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (contact_id, analyzer_name, analyzer_version, model, input_hash)
);

CREATE TABLE IF NOT EXISTS query_contact_embeddings (
  contact_id TEXT NOT NULL,
  model TEXT NOT NULL,
  embedding_id TEXT NOT NULL,
  input_hash TEXT NOT NULL DEFAULT '',
  text_preview TEXT NOT NULL DEFAULT '',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  dimensions INTEGER NOT NULL DEFAULT 0,
  embedding vector,
  PRIMARY KEY (contact_id, model, embedding_id)
);

CREATE INDEX IF NOT EXISTS query_contact_analysis_contact_idx
  ON query_contact_analysis(contact_id, analyzer_name, analyzer_version, input_hash);

CREATE INDEX IF NOT EXISTS query_contact_embeddings_model_idx
  ON query_contact_embeddings(model, dimensions);

-- +goose Down
DROP INDEX IF EXISTS query_contact_embeddings_model_idx;
DROP INDEX IF EXISTS query_contact_analysis_contact_idx;
DROP TABLE IF EXISTS query_contact_embeddings;
DROP TABLE IF EXISTS query_contact_analysis;
