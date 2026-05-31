-- +goose Up
INSERT INTO jmap_mailboxes(mailbox_id, name, role, sort_order, is_system)
VALUES
  ('starred', 'Starred', '', 70, true),
  ('unread', 'Unread', '', 80, true)
ON CONFLICT(mailbox_id) DO UPDATE SET
  name = excluded.name,
  role = excluded.role,
  sort_order = excluded.sort_order,
  is_system = excluded.is_system,
  is_destroyed = false,
  updated_at = now();

-- +goose Down
DELETE FROM jmap_mailboxes
 WHERE mailbox_id IN ('starred', 'unread')
   AND is_system = true;
