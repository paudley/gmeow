-- +goose Up
CREATE TABLE IF NOT EXISTS query_mail_identities (
  message_id TEXT NOT NULL,
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  generated BOOLEAN NOT NULL DEFAULT false,
  collision BOOLEAN NOT NULL DEFAULT false,
  variant BOOLEAN NOT NULL DEFAULT false,
  max_scale TEXT NOT NULL DEFAULT '',
  version_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (message_id, object_digest, source_kind, source_name)
);

CREATE INDEX IF NOT EXISTS query_mail_identities_message_idx
  ON query_mail_identities(message_id);

CREATE INDEX IF NOT EXISTS query_mail_identities_source_idx
  ON query_mail_identities(source_kind, source_name);

-- +goose Down
DROP TABLE IF EXISTS query_mail_identities;
