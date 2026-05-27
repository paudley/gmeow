-- +goose Up
CREATE TABLE IF NOT EXISTS jmap_mailboxes (
  mailbox_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT '',
  parent_id TEXT NOT NULL DEFAULT '',
  sort_order INTEGER NOT NULL DEFAULT 0,
  is_system BOOLEAN NOT NULL DEFAULT false,
  is_destroyed BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS jmap_mailboxes_role_unique_idx
  ON jmap_mailboxes(role)
  WHERE role <> '' AND is_destroyed = false;

CREATE UNIQUE INDEX IF NOT EXISTS jmap_mailboxes_parent_name_unique_idx
  ON jmap_mailboxes(parent_id, name)
  WHERE is_destroyed = false;

CREATE TABLE IF NOT EXISTS jmap_email_state (
  object_digest TEXT PRIMARY KEY REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  thread_id TEXT NOT NULL DEFAULT '',
  received_at TIMESTAMPTZ,
  state_seq BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jmap_email_state_thread_idx
  ON jmap_email_state(thread_id)
  WHERE thread_id <> '';

CREATE TABLE IF NOT EXISTS jmap_email_mailboxes (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  mailbox_id TEXT NOT NULL REFERENCES jmap_mailboxes(mailbox_id),
  PRIMARY KEY (object_digest, mailbox_id)
);

CREATE INDEX IF NOT EXISTS jmap_email_mailboxes_mailbox_idx
  ON jmap_email_mailboxes(mailbox_id);

CREATE TABLE IF NOT EXISTS jmap_email_keywords (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  keyword TEXT NOT NULL,
  PRIMARY KEY (object_digest, keyword)
);

CREATE INDEX IF NOT EXISTS jmap_email_keywords_keyword_idx
  ON jmap_email_keywords(keyword);

CREATE TABLE IF NOT EXISTS jmap_state_seq (
  datatype TEXT PRIMARY KEY,
  state_seq BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO jmap_mailboxes(mailbox_id, name, role, sort_order, is_system)
VALUES
  ('all', 'All Mail', '', 0, true),
  ('inbox', 'Inbox', 'inbox', 10, true),
  ('archive', 'Archive', 'archive', 20, true),
  ('sent', 'Sent', 'sent', 30, true),
  ('drafts', 'Drafts', 'drafts', 40, true),
  ('trash', 'Trash', 'trash', 50, true),
  ('spam', 'Spam', 'junk', 60, true)
ON CONFLICT(mailbox_id) DO UPDATE SET
  name = excluded.name,
  role = excluded.role,
  sort_order = excluded.sort_order,
  is_system = excluded.is_system,
  updated_at = now();

INSERT INTO jmap_state_seq(datatype, state_seq)
VALUES
  ('Email', 0),
  ('Mailbox', 0),
  ('Thread', 0),
  ('EmailDelivery', 0)
ON CONFLICT(datatype) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS jmap_state_seq;
DROP TABLE IF EXISTS jmap_email_keywords;
DROP TABLE IF EXISTS jmap_email_mailboxes;
DROP TABLE IF EXISTS jmap_email_state;
DROP TABLE IF EXISTS jmap_mailboxes;
