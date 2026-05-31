-- +goose Up
CREATE TABLE IF NOT EXISTS query_rdf_terms (
  term_id BIGSERIAL PRIMARY KEY,
  term_kind TEXT NOT NULL,
  term_value TEXT NOT NULL,
  language TEXT NOT NULL DEFAULT '',
  datatype TEXT NOT NULL DEFAULT '',
  UNIQUE (term_kind, term_value, language, datatype)
);

CREATE TABLE IF NOT EXISTS query_rdf_statements (
  source_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  statement_hash TEXT NOT NULL,
  subject_term_id BIGINT NOT NULL REFERENCES query_rdf_terms(term_id),
  predicate_term_id BIGINT NOT NULL REFERENCES query_rdf_terms(term_id),
  object_term_id BIGINT NOT NULL REFERENCES query_rdf_terms(term_id),
  graph_name TEXT NOT NULL DEFAULT '',
  statement_order INTEGER NOT NULL DEFAULT 0,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_digest, statement_hash)
);

CREATE TABLE IF NOT EXISTS query_rdf_statement_annotations (
  source_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  statement_hash TEXT NOT NULL,
  annotation_predicate_term_id BIGINT NOT NULL REFERENCES query_rdf_terms(term_id),
  annotation_object_term_id BIGINT NOT NULL REFERENCES query_rdf_terms(term_id),
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    source_digest,
    statement_hash,
    annotation_predicate_term_id,
    annotation_object_term_id
  ),
  FOREIGN KEY (source_digest, statement_hash)
    REFERENCES query_rdf_statements(source_digest, statement_hash)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS query_contact_facts (
  contact_id TEXT NOT NULL,
  fact_kind TEXT NOT NULL,
  value TEXT NOT NULL,
  value_hash TEXT NOT NULL,
  predicate TEXT NOT NULL,
  source_digest TEXT NOT NULL,
  statement_hash TEXT NOT NULL,
  valid_from TEXT NOT NULL DEFAULT '',
  valid_until TEXT NOT NULL DEFAULT '',
  historical BOOLEAN NOT NULL DEFAULT false,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (contact_id, fact_kind, value_hash, statement_hash)
);

CREATE TABLE IF NOT EXISTS query_contact_identity_bindings (
  token_hash TEXT NOT NULL,
  token TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  statement_hash TEXT NOT NULL,
  valid_from TEXT NOT NULL DEFAULT '',
  valid_until TEXT NOT NULL DEFAULT '',
  source_digest TEXT NOT NULL,
  PRIMARY KEY (token_hash, contact_id, statement_hash)
);

CREATE TABLE IF NOT EXISTS query_contact_rollups (
  contact_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL DEFAULT '',
  primary_email TEXT NOT NULL DEFAULT '',
  fact_count INTEGER NOT NULL DEFAULT 0,
  search_text TEXT NOT NULL DEFAULT '',
  search_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS query_rdf_terms_value_idx
  ON query_rdf_terms(term_value);

CREATE INDEX IF NOT EXISTS query_rdf_statements_source_idx
  ON query_rdf_statements(source_digest);

CREATE INDEX IF NOT EXISTS query_rdf_statements_subject_predicate_idx
  ON query_rdf_statements(subject_term_id, predicate_term_id);

CREATE INDEX IF NOT EXISTS query_rdf_statement_annotations_hash_idx
  ON query_rdf_statement_annotations(statement_hash);

CREATE INDEX IF NOT EXISTS query_contact_facts_kind_idx
  ON query_contact_facts(fact_kind);

CREATE INDEX IF NOT EXISTS query_contact_identity_bindings_token_idx
  ON query_contact_identity_bindings(token_hash);

CREATE INDEX IF NOT EXISTS query_contact_rollups_search_idx
  ON query_contact_rollups USING GIN(search_tsv);

-- +goose Down
DROP TABLE IF EXISTS query_contact_rollups;
DROP TABLE IF EXISTS query_contact_identity_bindings;
DROP TABLE IF EXISTS query_contact_facts;
DROP TABLE IF EXISTS query_rdf_statement_annotations;
DROP TABLE IF EXISTS query_rdf_statements;
DROP TABLE IF EXISTS query_rdf_terms;
