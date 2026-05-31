-- +goose Up
CREATE TABLE IF NOT EXISTS query_mail_participants (
  message_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  message_id TEXT NOT NULL DEFAULT '',
  message_date TEXT NOT NULL DEFAULT '',
  message_time TIMESTAMPTZ,
  role TEXT NOT NULL,
  ordinal INTEGER NOT NULL DEFAULT 0,
  token_hash TEXT NOT NULL,
  token TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',
  raw_value TEXT NOT NULL DEFAULT '',
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (message_digest, role, ordinal, token_hash)
);

CREATE INDEX IF NOT EXISTS query_mail_participants_token_time_idx
  ON query_mail_participants(token_hash, message_time DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS query_mail_participants_role_token_idx
  ON query_mail_participants(role, token_hash);

ALTER TABLE query_contact_rollups
  ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS message_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS participant_count INTEGER NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE query_contact_rollups
  DROP COLUMN IF EXISTS participant_count,
  DROP COLUMN IF EXISTS message_count,
  DROP COLUMN IF EXISTS last_seen_at,
  DROP COLUMN IF EXISTS first_seen_at;

DROP TABLE IF EXISTS query_mail_participants;
