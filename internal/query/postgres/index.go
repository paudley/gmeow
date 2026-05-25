// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
	"blackat.ca/gmeow/internal/query"
)

const defaultLimit = 50

type Config struct {
	ConnString    string
	MigrationsDir string
}

type Index struct {
	pool   *pgxpool.Pool
	source query.ProjectionSource
}

type RebuildReport struct {
	Scanned   int
	Projected int
	Failed    int
	Elapsed   time.Duration
}

type AgeStatus struct {
	Available bool
	Graph     string
	GraphID   int64
	Nodes     string
	Error     string
}

func New(
	ctx context.Context,
	config Config,
	source query.ProjectionSource,
) (*Index, error) {
	if strings.TrimSpace(config.ConnString) == "" {
		return nil, errors.New("postgres connection string is required")
	}
	pool, err := pgxpool.New(ctx, config.ConnString)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if err := validateRequiredProjectionCapabilities(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Index{pool: pool, source: source}, nil
}

func (index *Index) Close() {
	index.pool.Close()
}

func Migrate(ctx context.Context, config Config) error {
	if strings.TrimSpace(config.MigrationsDir) == "" {
		return errors.New("query migrations directory is required")
	}
	db, err := sql.Open("pgx", config.ConnString)
	if err != nil {
		return fmt.Errorf("open migration database: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping migration database: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, config.MigrationsDir); err != nil {
		return fmt.Errorf("run query migrations: %w", err)
	}
	return nil
}

func validateRequiredProjectionCapabilities(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	for _, extension := range []string{"vector", "age"} {
		var exists bool
		if err := pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)",
			extension,
		).Scan(&exists); err != nil {
			return fmt.Errorf("validate %s extension: %w", extension, err)
		}
		if !exists {
			return fmt.Errorf("required PostgreSQL extension %q is not enabled", extension)
		}
	}
	if _, err := pool.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return fmt.Errorf("set AGE search path: %w", err)
	}
	var graphExists bool
	if err := pool.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM ag_graph WHERE name = 'gmeow_graph')",
	).Scan(&graphExists); err != nil {
		return fmt.Errorf("validate AGE graph: %w", err)
	}
	if !graphExists {
		return errors.New("required AGE graph \"gmeow_graph\" is not initialized")
	}
	return nil
}

func (index *Index) Project(
	ctx context.Context,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	return index.ProjectObject(ctx, filestore.ProjectionObject{
		Digest:      manifest.ObjectDigest,
		Manifest:    manifest,
		Annotations: annotations,
	})
}

func (index *Index) ProjectObject(
	ctx context.Context,
	object filestore.ProjectionObject,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(object.Findings) > 0 {
		return index.recordProjectionFindings(ctx, object)
	}
	tx, err := index.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin projection transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := deleteAgeFactsForDigest(ctx, tx, object.Manifest.ObjectDigest); err != nil {
		return err
	}
	if err := projectObjectTx(ctx, tx, object); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projection transaction: %w", err)
	}
	return nil
}

func (index *Index) Rebuild(ctx context.Context) error {
	_, err := index.RebuildReport(ctx)
	return err
}

func (index *Index) RebuildReport(ctx context.Context) (RebuildReport, error) {
	if index.source == nil {
		return RebuildReport{}, errors.New("projection source is required for rebuild")
	}
	started := time.Now()
	tx, err := index.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RebuildReport{}, fmt.Errorf("begin rebuild transaction: %w", err)
	}
	for _, table := range []string{
		"query_projection_state",
		"query_summaries",
		"query_source_cursors",
		"query_objects",
	} {
		if _, err := tx.Exec(ctx, "TRUNCATE "+table+" CASCADE"); err != nil {
			tx.Rollback(ctx)
			return RebuildReport{}, fmt.Errorf("truncate %s: %w", table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return RebuildReport{}, fmt.Errorf("commit rebuild truncate: %w", err)
	}
	if err := index.clearAgeGraph(ctx); err != nil {
		return RebuildReport{}, err
	}
	report := RebuildReport{}
	err = index.source.WalkProjection(ctx, func(object filestore.ProjectionObject) error {
		report.Scanned++
		if len(object.Findings) > 0 {
			report.Failed++
		}
		if err := index.ProjectObject(ctx, object); err != nil {
			report.Failed++
			return err
		}
		if len(object.Findings) == 0 {
			report.Projected++
		}
		return nil
	})
	report.Elapsed = time.Since(started)
	return report, err
}

func (index *Index) Search(
	ctx context.Context,
	request contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	args := []any{}
	where := []string{"true"}
	if queryText := strings.TrimSpace(request.Query); queryText != "" {
		args = append(args, queryText)
		where = append(
			where,
			fmt.Sprintf("search_tsv @@ websearch_to_tsquery('simple', $%d)", len(args)),
		)
	}
	if len(request.Facets) > 0 {
		args = append(args, request.Facets)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_facets f WHERE f.object_digest = o.object_digest AND f.kind = ANY($%d))",
			len(args),
		))
	}
	if len(request.MediaTypes) > 0 {
		args = append(args, request.MediaTypes)
		where = append(where, fmt.Sprintf("o.media_type = ANY($%d)", len(args)))
	}
	if len(request.Provenance.SourceKinds) > 0 {
		args = append(args, request.Provenance.SourceKinds)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_provenance p WHERE p.object_digest = o.object_digest AND p.source_kind = ANY($%d))",
			len(args),
		))
	}
	if len(request.Provenance.SourceNames) > 0 {
		args = append(args, request.Provenance.SourceNames)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_provenance p WHERE p.object_digest = o.object_digest AND p.source_name = ANY($%d))",
			len(args),
		))
	}
	if len(request.Provenance.ExternalIDs) > 0 {
		args = append(args, request.Provenance.ExternalIDs)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_provenance p WHERE p.object_digest = o.object_digest AND p.external_id = ANY($%d))",
			len(args),
		))
	}
	if relationshipFilterActive(request.Relationships) {
		predicates := []string{"r.object_digest = o.object_digest"}
		if len(request.Relationships.Types) > 0 {
			args = append(args, request.Relationships.Types)
			predicates = append(
				predicates,
				fmt.Sprintf("r.relationship_type = ANY($%d)", len(args)),
			)
		}
		if request.Relationships.From != "" {
			args = append(args, request.Relationships.From)
			predicates = append(predicates, fmt.Sprintf("r.from_digest = $%d", len(args)))
		}
		if request.Relationships.To != "" {
			args = append(args, request.Relationships.To)
			predicates = append(predicates, fmt.Sprintf("r.to_digest = $%d", len(args)))
		}
		if request.Relationships.Any != "" {
			args = append(args, request.Relationships.Any)
			predicates = append(
				predicates,
				fmt.Sprintf("(r.from_digest = $%d OR r.to_digest = $%d)", len(args), len(args)),
			)
		}
		if len(request.Relationships.Roles) > 0 {
			args = append(args, request.Relationships.Roles)
			predicates = append(predicates, fmt.Sprintf("r.role = ANY($%d)", len(args)))
		}
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_relationships r WHERE %s)",
			strings.Join(predicates, " AND "),
		))
	}
	if len(request.CompoundRoles) > 0 {
		args = append(args, request.CompoundRoles)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_compound_parts c WHERE c.object_digest = o.object_digest AND c.role = ANY($%d))",
			len(args),
		))
	}
	if len(request.AnalyzerNames) > 0 {
		args = append(args, request.AnalyzerNames)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_analysis a WHERE a.object_digest = o.object_digest AND a.analyzer_name = ANY($%d))",
			len(args),
		))
	}
	limit := normalizedLimit(request.Limit)
	offset := request.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	sqlText := fmt.Sprintf(
		`SELECT o.object_digest, o.object_id, o.media_type,
		        COALESCE((o.manifest_json->'titles'->0->>'value'), o.object_id) AS title,
		        ARRAY(SELECT f.kind FROM query_object_facets f WHERE f.object_digest = o.object_digest ORDER BY f.kind) AS facets,
		        COUNT(*) OVER() AS total
		   FROM query_objects o
		  WHERE %s
		  ORDER BY o.updated_at DESC, o.object_digest
		  LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	)
	rows, err := index.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return contracts.SearchResponse{}, fmt.Errorf("search query projection: %w", err)
	}
	defer rows.Close()
	results := []contracts.SearchResult{}
	total := 0
	for rows.Next() {
		var result contracts.SearchResult
		var facets []string
		var objectID, mediaType string
		if err := rows.Scan(
			&result.ObjectDigest,
			&objectID,
			&mediaType,
			&result.Title,
			&facets,
			&total,
		); err != nil {
			return contracts.SearchResponse{}, fmt.Errorf("scan search result: %w", err)
		}
		result.Score = 1
		result.Facets = facets
		result.Attributes = map[string]any{"object_id": objectID, "media_type": mediaType}
		results = append(results, result)
	}
	return contracts.SearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
	}, rows.Err()
}

func (index *Index) Structure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	var objectID string
	if err := index.pool.QueryRow(
		ctx,
		"SELECT object_id FROM query_objects WHERE object_digest = $1",
		digest,
	).Scan(&objectID); err != nil {
		return contracts.Structure{}, fmt.Errorf("read structure object: %w", err)
	}
	structure := contracts.Structure{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		ObjectID:      objectID,
		PartsByRole:   map[string][]contracts.StructurePart{},
	}
	facetRows, err := index.pool.Query(
		ctx,
		"SELECT kind FROM query_object_facets WHERE object_digest = $1 ORDER BY kind",
		digest,
	)
	if err != nil {
		return contracts.Structure{}, err
	}
	for facetRows.Next() {
		var kind string
		if err := facetRows.Scan(&kind); err != nil {
			facetRows.Close()
			return contracts.Structure{}, err
		}
		structure.Facets = append(structure.Facets, kind)
	}
	facetRows.Close()
	partRows, err := index.pool.Query(
		ctx,
		`SELECT c.part_digest, c.role, c.part_order, c.required, c.metadata_json,
		        ARRAY(SELECT f.kind FROM query_object_facets f WHERE f.object_digest = c.part_digest ORDER BY f.kind)
		   FROM query_object_compound_parts c
		  WHERE c.object_digest = $1
		  ORDER BY c.role, c.part_order, c.part_digest`,
		digest,
	)
	if err != nil {
		return contracts.Structure{}, err
	}
	defer partRows.Close()
	for partRows.Next() {
		var part contracts.StructurePart
		var metadata []byte
		if err := partRows.Scan(
			&part.Digest,
			&part.Role,
			&part.Order,
			&part.Required,
			&metadata,
			&part.Facets,
		); err != nil {
			return contracts.Structure{}, err
		}
		if err := json.Unmarshal(metadata, &part.Metadata); err != nil {
			return contracts.Structure{}, err
		}
		structure.PartsByRole[part.Role] = append(structure.PartsByRole[part.Role], part)
	}
	return structure, partRows.Err()
}

func (index *Index) Relationships(
	ctx context.Context,
	request contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	args := []any{}
	where := []string{"true"}
	filter := request.Filter
	if len(filter.Types) > 0 {
		args = append(args, filter.Types)
		where = append(where, fmt.Sprintf("relationship_type = ANY($%d)", len(args)))
	}
	if filter.From != "" {
		args = append(args, filter.From)
		where = append(where, fmt.Sprintf("from_digest = $%d", len(args)))
	}
	if filter.To != "" {
		args = append(args, filter.To)
		where = append(where, fmt.Sprintf("to_digest = $%d", len(args)))
	}
	if filter.Any != "" {
		args = append(args, filter.Any)
		where = append(
			where,
			fmt.Sprintf("(from_digest = $%d OR to_digest = $%d)", len(args), len(args)),
		)
	}
	if len(filter.Roles) > 0 {
		args = append(args, filter.Roles)
		where = append(where, fmt.Sprintf("role = ANY($%d)", len(args)))
	}
	args = append(args, normalizedLimit(request.Limit))
	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT relationship_type, from_digest, to_digest, role, relationship_order, source
		   FROM query_object_relationships
		  WHERE %s
		  ORDER BY relationship_type, from_digest, to_digest
		  LIMIT $%d`,
		strings.Join(where, " AND "),
		len(args),
	), args...)
	if err != nil {
		return contracts.RelationshipResponse{}, err
	}
	defer rows.Close()
	relationships := []contracts.Relationship{}
	for rows.Next() {
		var relationship contracts.Relationship
		if err := rows.Scan(
			&relationship.Type,
			&relationship.From,
			&relationship.To,
			&relationship.Role,
			&relationship.Order,
			&relationship.Source,
		); err != nil {
			return contracts.RelationshipResponse{}, err
		}
		relationships = append(relationships, relationship)
	}
	return contracts.RelationshipResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Relationships: relationships,
	}, rows.Err()
}

func (index *Index) Graph(
	ctx context.Context,
	request contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	args := []any{}
	where := []string{"true"}
	if request.Node != "" {
		args = append(args, request.Node)
		where = append(
			where,
			fmt.Sprintf("(subject = $%d OR object_value = $%d)", len(args), len(args)),
		)
	}
	if request.Predicate != "" {
		args = append(args, request.Predicate)
		where = append(where, fmt.Sprintf("predicate = $%d", len(args)))
	}
	args = append(args, normalizedLimit(request.Limit))
	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT subject, predicate, object_value, metadata_json
		   FROM query_object_graph_edges
		  WHERE %s
		  ORDER BY subject, predicate, object_value
		  LIMIT $%d`,
		strings.Join(where, " AND "),
		len(args),
	), args...)
	if err != nil {
		return contracts.GraphResponse{}, err
	}
	defer rows.Close()
	facts := []contracts.GraphFact{}
	for rows.Next() {
		var fact contracts.GraphFact
		var metadata []byte
		if err := rows.Scan(
			&fact.Subject,
			&fact.Predicate,
			&fact.Object,
			&metadata,
		); err != nil {
			return contracts.GraphResponse{}, err
		}
		if err := json.Unmarshal(metadata, &fact.Metadata); err != nil {
			return contracts.GraphResponse{}, err
		}
		facts = append(facts, fact)
	}
	return contracts.GraphResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Facts:         facts,
	}, rows.Err()
}

func (index *Index) AnalysisStatus(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	args := []any{}
	where := []string{"true"}
	if len(request.ObjectDigests) > 0 {
		args = append(args, request.ObjectDigests)
		where = append(where, fmt.Sprintf("object_digest = ANY($%d)", len(args)))
	}
	if len(request.AnalyzerNames) > 0 {
		args = append(args, request.AnalyzerNames)
		where = append(where, fmt.Sprintf("analyzer_name = ANY($%d)", len(args)))
	}
	args = append(args, normalizedLimit(request.Limit))
	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT object_digest, analyzer_name, analyzer_version, status, generated_at, data_json
		   FROM query_object_analysis
		  WHERE %s
		  ORDER BY object_digest, analyzer_name
		  LIMIT $%d`,
		strings.Join(where, " AND "),
		len(args),
	), args...)
	if err != nil {
		return contracts.AnalysisStatusResponse{}, err
	}
	defer rows.Close()
	statuses := []contracts.AnalysisStatus{}
	for rows.Next() {
		var status contracts.AnalysisStatus
		var data []byte
		if err := rows.Scan(
			&status.ObjectDigest,
			&status.AnalyzerName,
			&status.AnalyzerVer,
			&status.Status,
			&status.GeneratedAt,
			&data,
		); err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}
		if err := json.Unmarshal(data, &status.Data); err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}
		statuses = append(statuses, status)
	}
	return contracts.AnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Statuses:      statuses,
	}, rows.Err()
}

func (index *Index) VectorSearch(
	ctx context.Context,
	request contracts.VectorSearchRequest,
) (contracts.VectorSearchResponse, error) {
	if len(request.Vector) == 0 {
		return contracts.VectorSearchResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
		}, nil
	}
	args := []any{vectorLiteral(request.Vector)}
	where := []string{"e.embedding IS NOT NULL"}
	if request.Model != "" {
		args = append(args, request.Model)
		where = append(where, fmt.Sprintf("e.model = $%d", len(args)))
	}
	if request.Dimensions > 0 {
		args = append(args, request.Dimensions)
		where = append(where, fmt.Sprintf("e.dimensions = $%d", len(args)))
	}
	if len(request.Facets) > 0 {
		args = append(args, request.Facets)
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM query_object_facets f WHERE f.object_digest = e.object_digest AND f.kind = ANY($%d))",
			len(args),
		))
	}
	args = append(args, normalizedLimit(request.Limit))
	sqlText := fmt.Sprintf(
		`SELECT e.object_digest, e.model, e.embedding <=> $1::vector AS distance
		   FROM query_object_embeddings e
		  WHERE %s
		  ORDER BY e.embedding <=> $1::vector
		  LIMIT $%d`,
		strings.Join(where, " AND "),
		len(args),
	)
	rows, err := index.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return contracts.VectorSearchResponse{}, fmt.Errorf("vector search: %w", err)
	}
	defer rows.Close()
	results := []contracts.VectorSearchResult{}
	for rows.Next() {
		var result contracts.VectorSearchResult
		if err := rows.Scan(
			&result.ObjectDigest,
			&result.Model,
			&result.Distance,
		); err != nil {
			return contracts.VectorSearchResponse{}, err
		}
		results = append(results, result)
	}
	return contracts.VectorSearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
	}, rows.Err()
}

func (index *Index) AgeStatus(ctx context.Context) AgeStatus {
	if _, err := index.pool.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return AgeStatus{Graph: "gmeow_graph", Error: err.Error()}
	}
	var status AgeStatus
	status.Graph = "gmeow_graph"
	err := index.pool.QueryRow(
		ctx,
		"SELECT graphid, name FROM ag_graph WHERE name = 'gmeow_graph'",
	).Scan(&status.GraphID, &status.Graph)
	if err != nil {
		status.Error = err.Error()
		return status
	}
	var nodes string
	if err := index.pool.QueryRow(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH (n) RETURN count(n)$$) AS (nodes agtype)",
	).Scan(&nodes); err != nil {
		status.Error = err.Error()
		return status
	}
	status.Available = true
	status.Nodes = nodes
	return status
}

func (index *Index) AgeCypher(
	ctx context.Context,
	cypher string,
	columns string,
	limit int,
) ([]map[string]string, error) {
	if !readOnlyCypher(cypher) {
		return nil, errors.New("only read-only MATCH/RETURN Cypher queries are allowed")
	}
	if strings.TrimSpace(columns) == "" {
		columns = "value agtype"
	}
	if err := validateAgeColumns(columns); err != nil {
		return nil, err
	}
	cypher = strings.TrimSpace(strings.TrimSuffix(cypher, ";"))
	if !strings.Contains(" "+strings.ToLower(cypher)+" ", " limit ") {
		cypher = fmt.Sprintf("%s LIMIT %d", cypher, normalizedLimit(limit))
	}
	if _, err := index.pool.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return nil, err
	}
	rows, err := index.pool.Query(ctx, ageSQL(cypher, columns))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fieldDescriptions := rows.FieldDescriptions()
	results := []map[string]string{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := map[string]string{}
		for index, value := range values {
			row[string(fieldDescriptions[index].Name)] = fmt.Sprint(value)
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func (index *Index) clearAgeGraph(ctx context.Context) error {
	if _, err := index.pool.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return fmt.Errorf("set AGE search path: %w", err)
	}
	if _, err := index.pool.Exec(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH ()-[r]->() DELETE r$$) AS (value agtype)",
	); err != nil {
		return fmt.Errorf("clear AGE graph edges: %w", err)
	}
	if _, err := index.pool.Exec(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH (n) DELETE n$$) AS (value agtype)",
	); err != nil {
		return fmt.Errorf("clear AGE graph nodes: %w", err)
	}
	return nil
}

func projectObjectTx(
	ctx context.Context,
	tx pgx.Tx,
	object filestore.ProjectionObject,
) error {
	manifestJSON, err := json.Marshal(object.Manifest)
	if err != nil {
		return err
	}
	annotationsJSON, err := json.Marshal(object.Annotations)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO query_objects(
		   object_digest, object_id, identity_strategy, media_type, size_bytes,
		   created_at, updated_at, search_text, manifest_json, annotations_json, projected_at
		 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
		 ON CONFLICT(object_digest) DO UPDATE SET
		   object_id = excluded.object_id,
		   identity_strategy = excluded.identity_strategy,
		   media_type = excluded.media_type,
		   size_bytes = excluded.size_bytes,
		   created_at = excluded.created_at,
		   updated_at = excluded.updated_at,
		   search_text = excluded.search_text,
		   manifest_json = excluded.manifest_json,
		   annotations_json = excluded.annotations_json,
		   projected_at = now()`,
		object.Manifest.ObjectDigest,
		object.Manifest.ObjectID,
		object.Manifest.IdentityStrategy,
		object.Manifest.MediaType,
		object.Manifest.Size,
		object.Manifest.CreatedAt,
		object.Manifest.UpdatedAt,
		searchText(object.Manifest, object.Annotations),
		manifestJSON,
		annotationsJSON,
	); err != nil {
		return fmt.Errorf("upsert query object: %w", err)
	}
	for _, table := range []string{
		"query_object_facets",
		"query_object_provenance",
		"query_object_relationships",
		"query_object_compound_parts",
		"query_object_analysis",
		"query_object_graph_edges",
		"query_object_keywords",
		"query_object_embeddings",
		"query_object_overlays",
		"query_summaries",
	} {
		if _, err := tx.Exec(
			ctx,
			"DELETE FROM "+table+" WHERE object_digest = $1",
			object.Manifest.ObjectDigest,
		); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	if err := insertFacetRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertProvenanceRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertRelationshipRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertCompoundRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertAnalysisRows(ctx, tx, object.Annotations); err != nil {
		return err
	}
	if err := insertGraphRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertAgeGraphRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertKeywordRows(ctx, tx, object.Manifest); err != nil {
		return err
	}
	if err := insertEmbeddingRows(
		ctx,
		tx,
		object.Manifest,
		object.Annotations,
	); err != nil {
		return err
	}
	if err := insertOverlayRows(ctx, tx, object.Manifest, object.Annotations); err != nil {
		return err
	}
	if err := insertSummaryRows(ctx, tx, object.Manifest, object.Annotations); err != nil {
		return err
	}
	return nil
}

func deleteAgeFactsForDigest(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
) error {
	if _, err := tx.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return fmt.Errorf("set AGE search path: %w", err)
	}
	cypher := fmt.Sprintf(
		`MATCH ()-[r]->() WHERE r.object_digest = %s DELETE r`,
		ageStringLiteral(string(digest)),
	)
	if _, err := tx.Exec(ctx, ageSQL(cypher, "value agtype")); err != nil {
		return fmt.Errorf("delete AGE facts for %s: %w", digest, err)
	}
	return nil
}

func insertFacetRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, facet := range manifest.Facets {
		metadata, err := json.Marshal(nonNilMap(firstMap(facet.Metadata, facet.Attributes)))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_facets(object_digest, kind, version, metadata_json)
			 VALUES($1,$2,$3,$4)
			 ON CONFLICT(object_digest, kind) DO UPDATE SET
			   version = excluded.version,
			   metadata_json = excluded.metadata_json`,
			manifest.ObjectDigest,
			firstNonEmpty(facet.Kind, facet.Name),
			facet.Version,
			metadata,
		); err != nil {
			return fmt.Errorf("insert facet projection: %w", err)
		}
	}
	return nil
}

func insertProvenanceRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, item := range manifest.Provenance {
		attributes, err := json.Marshal(nonNilMap(item.Attributes))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_provenance(
			   object_digest, source_kind, source_name, external_id, observed_at, attributes_json
			 ) VALUES($1,$2,$3,$4,$5,$6)`,
			manifest.ObjectDigest,
			item.SourceKind,
			item.SourceName,
			item.ExternalID,
			item.ObservedAt,
			attributes,
		); err != nil {
			return fmt.Errorf("insert provenance projection: %w", err)
		}
	}
	return nil
}

func insertRelationshipRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, item := range manifest.Relationships {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_relationships(
			   object_digest, relationship_type, from_digest, to_digest, role, relationship_order, source
			 ) VALUES($1,$2,$3,$4,$5,$6,$7)`,
			manifest.ObjectDigest,
			item.Type,
			item.From,
			item.To,
			item.Role,
			item.Order,
			item.Source,
		); err != nil {
			return fmt.Errorf("insert relationship projection: %w", err)
		}
	}
	return nil
}

func insertCompoundRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, item := range manifest.Compound.Parts {
		metadata, err := json.Marshal(nonNilMap(item.Metadata))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_compound_parts(
			   object_digest, part_digest, role, part_order, required, metadata_json
			 ) VALUES($1,$2,$3,$4,$5,$6)
			 ON CONFLICT(object_digest, part_digest, role, part_order) DO UPDATE SET
			   required = excluded.required,
			   metadata_json = excluded.metadata_json`,
			manifest.ObjectDigest,
			item.Digest,
			item.Role,
			item.Order,
			item.Required,
			metadata,
		); err != nil {
			return fmt.Errorf("insert compound projection: %w", err)
		}
	}
	return nil
}

func insertAnalysisRows(
	ctx context.Context,
	tx pgx.Tx,
	annotations []contracts.Annotation,
) error {
	for _, annotation := range annotations {
		if annotation.Kind != "analysis" {
			continue
		}
		data, err := json.Marshal(nonNilMap(annotation.Data))
		if err != nil {
			return err
		}
		status := "complete"
		if value, ok := annotation.Data["status"].(string); ok && value != "" {
			status = value
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_analysis(
			   object_digest, analyzer_name, analyzer_version, status, generated_at, data_json
			 ) VALUES($1,$2,$3,$4,$5,$6)`,
			annotation.ObjectDigest,
			annotation.AnalyzerName,
			annotation.AnalyzerVer,
			status,
			annotation.GeneratedAt,
			data,
		); err != nil {
			return fmt.Errorf("insert analysis projection: %w", err)
		}
	}
	return nil
}

func insertGraphRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, fact := range manifest.Graph {
		metadata, err := json.Marshal(nonNilMap(fact.Metadata))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_graph_edges(
			   object_digest, subject, predicate, object_value, metadata_json
			 ) VALUES($1,$2,$3,$4,$5)`,
			manifest.ObjectDigest,
			fact.Subject,
			fact.Predicate,
			fact.Object,
			metadata,
		); err != nil {
			return fmt.Errorf("insert graph projection: %w", err)
		}
	}
	return nil
}

func insertAgeGraphRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	if len(manifest.Graph) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, "SET search_path=ag_catalog, public"); err != nil {
		return fmt.Errorf("set AGE search path: %w", err)
	}
	for _, fact := range manifest.Graph {
		cypher := fmt.Sprintf(
			`MERGE (subject:gmeow_node {id: %s})
			 SET subject.label = %s
			 MERGE (object:gmeow_node {id: %s})
			 SET object.label = %s
			 CREATE (subject)-[:gmeow_fact {
			   object_digest: %s,
			   subject: %s,
			   predicate: %s,
			   object: %s
			 }]->(object)
			 RETURN 1`,
			ageStringLiteral(fact.Subject),
			ageStringLiteral(fact.Subject),
			ageStringLiteral(fact.Object),
			ageStringLiteral(fact.Object),
			ageStringLiteral(string(manifest.ObjectDigest)),
			ageStringLiteral(fact.Subject),
			ageStringLiteral(fact.Predicate),
			ageStringLiteral(fact.Object),
		)
		if _, err := tx.Exec(ctx, ageSQL(cypher, "value agtype")); err != nil {
			return fmt.Errorf("insert AGE graph projection: %w", err)
		}
	}
	return nil
}

func insertKeywordRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	for _, keyword := range manifest.Keywords {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_keywords(object_digest, keyword)
			 VALUES($1,$2)
			 ON CONFLICT(object_digest, keyword) DO NOTHING`,
			manifest.ObjectDigest,
			keyword,
		); err != nil {
			return fmt.Errorf("insert keyword projection: %w", err)
		}
	}
	return nil
}

func insertEmbeddingRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	rows := embeddingRowsFrom(manifest, annotations)
	for _, row := range rows {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_embeddings(
			   object_digest, model, embedding_object_digest, dimensions, embedding
			 ) VALUES($1,$2,$3,$4,$5::vector)
			 ON CONFLICT(object_digest, model, embedding_object_digest) DO UPDATE SET
			   dimensions = excluded.dimensions,
			   embedding = excluded.embedding`,
			manifest.ObjectDigest,
			row.model,
			row.digest,
			row.dimensions,
			row.vector,
		); err != nil {
			return fmt.Errorf("insert embedding projection: %w", err)
		}
	}
	return nil
}

func insertOverlayRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	overlays := nonNilMap(manifest.Overlays)
	for _, annotation := range annotations {
		if annotation.Kind == "overlays" {
			for key, value := range annotation.Data {
				overlays[key] = value
			}
		}
	}
	encoded, err := json.Marshal(overlays)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO query_object_overlays(object_digest, overlays_json)
		 VALUES($1,$2)
		 ON CONFLICT(object_digest) DO UPDATE SET overlays_json = excluded.overlays_json`,
		manifest.ObjectDigest,
		encoded,
	); err != nil {
		return fmt.Errorf("insert overlays projection: %w", err)
	}
	return nil
}

func insertSummaryRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	for _, item := range summaryRowsFrom(manifest, annotations) {
		metadata, err := json.Marshal(nonNilMap(item.metadata))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_summaries(object_digest, summary_kind, summary_text, metadata_json)
			 VALUES($1,$2,$3,$4)
			 ON CONFLICT(object_digest, summary_kind) DO UPDATE SET
			   summary_text = excluded.summary_text,
			   metadata_json = excluded.metadata_json`,
			manifest.ObjectDigest,
			item.kind,
			item.text,
			metadata,
		); err != nil {
			return fmt.Errorf("insert summary projection: %w", err)
		}
	}
	return nil
}

func (index *Index) recordProjectionFindings(
	ctx context.Context,
	object filestore.ProjectionObject,
) error {
	encoded, err := json.Marshal(object.Findings)
	if err != nil {
		return err
	}
	tx, err := index.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin projection finding transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := deleteAgeFactsForDigest(ctx, tx, object.Digest); err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx,
		"DELETE FROM query_objects WHERE object_digest = $1",
		object.Digest,
	); err != nil {
		return fmt.Errorf("delete stale projection for %s: %w", object.Digest, err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO query_projection_state(key, value_json, updated_at)
		 VALUES($1,$2,now())
		 ON CONFLICT(key) DO UPDATE SET
		   value_json = excluded.value_json,
		   updated_at = now()`,
		"findings:"+string(object.Digest),
		encoded,
	); err != nil {
		return fmt.Errorf("record projection findings for %s: %w", object.Digest, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projection findings for %s: %w", object.Digest, err)
	}
	return nil
}

type embeddingRow struct {
	model      string
	digest     contracts.ObjectDigest
	dimensions int
	vector     *string
}

func embeddingRowsFrom(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) []embeddingRow {
	rows := []embeddingRow{}
	for _, item := range manifest.Embeddings {
		rows = append(rows, embeddingRow{
			model:      item.Model,
			digest:     item.ObjectDigest,
			dimensions: item.Dimensions,
		})
	}
	for _, annotation := range annotations {
		values, ok := annotation.Data["embeddings"].([]any)
		if !ok {
			continue
		}
		for _, value := range values {
			item, ok := value.(map[string]any)
			if !ok {
				continue
			}
			vector := vectorAnyLiteral(item["vector"])
			rows = append(rows, embeddingRow{
				model:      stringFromAny(item["model"]),
				digest:     contracts.ObjectDigest(stringFromAny(item["object_digest"])),
				dimensions: intFromAny(item["dimensions"]),
				vector:     vector,
			})
		}
	}
	return rows
}

type summaryRow struct {
	kind     string
	text     string
	metadata map[string]any
}

func summaryRowsFrom(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) []summaryRow {
	rows := []summaryRow{}
	for _, annotation := range annotations {
		if annotation.Kind != "analysis" {
			continue
		}
		if summary := stringFromAny(annotation.Data["summary"]); summary != "" {
			rows = append(rows, summaryRow{
				kind:     firstNonEmpty(annotation.AnalyzerName, "analysis"),
				text:     summary,
				metadata: map[string]any{"analyzer_version": annotation.AnalyzerVer},
			})
		}
	}
	if summary := stringFromAny(manifest.Analysis["summary"]); summary != "" {
		rows = append(rows, summaryRow{kind: "manifest", text: summary})
	}
	return rows
}

func searchText(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) string {
	parts := []string{
		manifest.ObjectID,
		manifest.MediaType,
		strings.Join(manifest.ContentRoles, " "),
		strings.Join(manifest.Keywords, " "),
	}
	for _, title := range manifest.Titles {
		parts = append(parts, title.Value)
	}
	for _, facet := range manifest.Facets {
		parts = append(parts, facet.Kind, facet.Name)
	}
	for _, provenance := range manifest.Provenance {
		parts = append(
			parts,
			provenance.SourceKind,
			provenance.SourceName,
			provenance.ExternalID,
		)
	}
	encoded, _ := json.Marshal(annotations)
	parts = append(parts, string(encoded))
	return strings.Join(parts, "\n")
}

func ageSQL(cypher, columns string) string {
	return fmt.Sprintf(
		"SELECT * FROM cypher('gmeow_graph', %s) AS (%s)",
		dollarQuote(cypher),
		columns,
	)
}

func dollarQuote(value string) string {
	tag := "gmeow_age"
	for strings.Contains(value, "$"+tag+"$") {
		tag = "_" + tag
	}
	return "$" + tag + "$" + value + "$" + tag + "$"
}

func ageStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}

func readOnlyCypher(query string) bool {
	lowered := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(lowered, "match ") {
		return false
	}
	padded := " " + lowered + " "
	for _, word := range []string{
		" create ", " merge ", " delete ", " detach ", " set ", " remove ", " drop ", " call ",
	} {
		if strings.Contains(padded, word) {
			return false
		}
	}
	return strings.Contains(padded, " return ")
}

func validateAgeColumns(columns string) error {
	for _, char := range columns {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' ||
			char == ',' ||
			char == ' ' {
			continue
		}
		return fmt.Errorf("invalid AGE column declaration %q", columns)
	}
	for _, part := range strings.Split(columns, ",") {
		fields := strings.Fields(part)
		if len(fields) != 2 {
			return fmt.Errorf("invalid AGE column declaration %q", columns)
		}
		switch fields[1] {
		case "agtype", "text", "bigint", "int", "float8", "boolean":
		default:
			return fmt.Errorf("unsupported AGE column type %q", fields[1])
		}
	}
	return nil
}

func vectorLiteral(values []float32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(float64(value), 'f', -1, 32))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func vectorAnyLiteral(value any) *string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	values := make([]float32, 0, len(items))
	for _, item := range items {
		switch typed := item.(type) {
		case float64:
			if math.IsNaN(typed) || math.IsInf(typed, 0) {
				return nil
			}
			values = append(values, float32(typed))
		case float32:
			values = append(values, typed)
		}
	}
	literal := vectorLiteral(values)
	return &literal
}

func normalizedLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func relationshipFilterActive(filter contracts.RelationshipFilter) bool {
	return len(filter.Types) > 0 ||
		filter.From != "" ||
		filter.To != "" ||
		len(filter.Roles) > 0 ||
		filter.Any != ""
}

func firstMap(left, right map[string]any) map[string]any {
	if left != nil {
		return left
	}
	return right
}

func nonNilMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stringFromAny(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

var _ query.Index = (*Index)(nil)
