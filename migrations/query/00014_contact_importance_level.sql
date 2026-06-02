-- +goose Up
ALTER TABLE query_contact_rollups
  ADD COLUMN IF NOT EXISTS importance_level INTEGER NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE query_contact_rollups
  DROP COLUMN IF EXISTS importance_level;
