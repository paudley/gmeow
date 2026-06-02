-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP INDEX IF EXISTS query_rdf_terms_value_idx;

ALTER TABLE query_rdf_terms
  DROP CONSTRAINT IF EXISTS query_rdf_terms_term_kind_term_value_language_datatype_key;

ALTER TABLE query_rdf_terms
  ADD COLUMN IF NOT EXISTS term_key TEXT;

UPDATE query_rdf_terms
SET term_key = encode(
  digest(
    convert_to(term_kind, 'UTF8') ||
    decode('00', 'hex') ||
    convert_to(term_value, 'UTF8') ||
    decode('00', 'hex') ||
    convert_to(language, 'UTF8') ||
    decode('00', 'hex') ||
    convert_to(datatype, 'UTF8'),
    'sha256'
  ),
  'hex'
)
WHERE term_key IS NULL OR term_key = '';

ALTER TABLE query_rdf_terms
  ALTER COLUMN term_key SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS query_rdf_terms_identity_idx
  ON query_rdf_terms(term_kind, term_key, language, datatype);

-- +goose Down
DROP INDEX IF EXISTS query_rdf_terms_identity_idx;

ALTER TABLE query_rdf_terms
  DROP COLUMN IF EXISTS term_key;

ALTER TABLE query_rdf_terms
  ADD CONSTRAINT query_rdf_terms_term_kind_term_value_language_datatype_key
  UNIQUE (term_kind, term_value, language, datatype);

CREATE INDEX IF NOT EXISTS query_rdf_terms_value_idx
  ON query_rdf_terms(term_value);
