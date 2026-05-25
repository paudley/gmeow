-- +goose Up
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    RAISE EXCEPTION 'required PostgreSQL extension "vector" is not enabled';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'age') THEN
    RAISE EXCEPTION 'required PostgreSQL extension "age" is not enabled';
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS query_objects (
  object_digest TEXT PRIMARY KEY,
  object_id TEXT NOT NULL,
  identity_strategy TEXT NOT NULL,
  media_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  search_text TEXT NOT NULL,
  search_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED,
  manifest_json JSONB NOT NULL,
  annotations_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS query_object_facets (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  version TEXT NOT NULL DEFAULT '',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (object_digest, kind)
);

CREATE TABLE IF NOT EXISTS query_object_provenance (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  external_id TEXT NOT NULL DEFAULT '',
  observed_at TIMESTAMPTZ,
  attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS query_object_relationships (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  relationship_type TEXT NOT NULL,
  from_digest TEXT NOT NULL,
  to_digest TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT '',
  relationship_order INTEGER NOT NULL DEFAULT 0,
  source TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS query_object_compound_parts (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  part_digest TEXT NOT NULL,
  role TEXT NOT NULL,
  part_order INTEGER NOT NULL DEFAULT 0,
  required BOOLEAN NOT NULL DEFAULT false,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (object_digest, part_digest, role, part_order)
);

CREATE TABLE IF NOT EXISTS query_object_analysis (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  analyzer_name TEXT NOT NULL,
  analyzer_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'complete',
  generated_at TIMESTAMPTZ,
  data_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS query_object_graph_edges (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  subject TEXT NOT NULL,
  predicate TEXT NOT NULL,
  object_value TEXT NOT NULL,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS query_object_keywords (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  keyword TEXT NOT NULL,
  PRIMARY KEY (object_digest, keyword)
);

CREATE TABLE IF NOT EXISTS query_object_embeddings (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  model TEXT NOT NULL,
  embedding_object_digest TEXT NOT NULL,
  dimensions INTEGER NOT NULL DEFAULT 0,
  embedding vector,
  PRIMARY KEY (object_digest, model, embedding_object_digest)
);

CREATE TABLE IF NOT EXISTS query_object_overlays (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  overlays_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (object_digest)
);

CREATE TABLE IF NOT EXISTS query_source_cursors (
  source_name TEXT PRIMARY KEY,
  source_kind TEXT NOT NULL DEFAULT '',
  cursor_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS query_summaries (
  object_digest TEXT NOT NULL REFERENCES query_objects(object_digest) ON DELETE CASCADE,
  summary_kind TEXT NOT NULL,
  summary_text TEXT NOT NULL,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (object_digest, summary_kind)
);

CREATE TABLE IF NOT EXISTS query_projection_state (
  key TEXT PRIMARY KEY,
  value_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS query_objects_search_tsv_idx ON query_objects USING GIN(search_tsv);
CREATE INDEX IF NOT EXISTS query_objects_media_type_idx ON query_objects(media_type);
CREATE INDEX IF NOT EXISTS query_object_facets_kind_idx ON query_object_facets(kind);
CREATE INDEX IF NOT EXISTS query_object_provenance_source_idx ON query_object_provenance(source_kind, source_name, external_id);
CREATE INDEX IF NOT EXISTS query_object_relationships_from_idx ON query_object_relationships(from_digest);
CREATE INDEX IF NOT EXISTS query_object_relationships_to_idx ON query_object_relationships(to_digest);
CREATE INDEX IF NOT EXISTS query_object_compound_parts_role_idx ON query_object_compound_parts(role);
CREATE INDEX IF NOT EXISTS query_object_analysis_analyzer_idx ON query_object_analysis(analyzer_name, analyzer_version, status);
CREATE INDEX IF NOT EXISTS query_object_graph_edges_subject_idx ON query_object_graph_edges(subject);
CREATE INDEX IF NOT EXISTS query_object_graph_edges_object_idx ON query_object_graph_edges(object_value);

DO $$
BEGIN
  PERFORM set_config('search_path', 'ag_catalog, public', false);
  IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'gmeow_graph') THEN
    PERFORM ag_catalog.create_graph('gmeow_graph');
  END IF;
END
$$;

-- +goose Down
DROP TABLE IF EXISTS query_projection_state;
DROP TABLE IF EXISTS query_summaries;
DROP TABLE IF EXISTS query_source_cursors;
DROP TABLE IF EXISTS query_object_overlays;
DROP TABLE IF EXISTS query_object_embeddings;
DROP TABLE IF EXISTS query_object_keywords;
DROP TABLE IF EXISTS query_object_graph_edges;
DROP TABLE IF EXISTS query_object_analysis;
DROP TABLE IF EXISTS query_object_compound_parts;
DROP TABLE IF EXISTS query_object_relationships;
DROP TABLE IF EXISTS query_object_provenance;
DROP TABLE IF EXISTS query_object_facets;
DROP TABLE IF EXISTS query_objects;
