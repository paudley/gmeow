-- +goose Up
CREATE TABLE IF NOT EXISTS query_contact_aliases (
  contact_alias TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  statement_hash TEXT NOT NULL,
  source_digest TEXT NOT NULL,
  valid_from TEXT NOT NULL DEFAULT '',
  valid_until TEXT NOT NULL DEFAULT '',
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_alias, contact_id, statement_hash)
);

CREATE INDEX IF NOT EXISTS query_contact_aliases_active_alias_idx
  ON query_contact_aliases(contact_alias)
  WHERE valid_until = '';

CREATE INDEX IF NOT EXISTS query_contact_aliases_contact_idx
  ON query_contact_aliases(contact_id, contact_alias);

CREATE TABLE IF NOT EXISTS active_contact_aliases (
  contact_alias TEXT PRIMARY KEY,
  contact_id TEXT NOT NULL,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS active_contact_aliases_contact_idx
  ON active_contact_aliases(contact_id);

-- +goose Down
DROP INDEX IF EXISTS active_contact_aliases_contact_idx;
DROP TABLE IF EXISTS active_contact_aliases;
DROP INDEX IF EXISTS query_contact_aliases_contact_idx;
DROP INDEX IF EXISTS query_contact_aliases_active_alias_idx;
DROP TABLE IF EXISTS query_contact_aliases;
