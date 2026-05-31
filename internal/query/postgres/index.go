// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/query"
)

const (
	defaultLimit       = 50
	maxSearchTextBytes = 500000
)

type Config struct {
	ConnString     string
	MigrationsDir  string
	MigrationTable string
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
	Graph     string
	Nodes     string
	Error     string
	GraphID   int64
	Available bool
}

var migrationMu sync.Mutex

type ageSearchPathExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
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
	if closer, ok := index.source.(io.Closer); ok {
		_ = closer.Close()
	}
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

	migrationMu.Lock()
	defer migrationMu.Unlock()
	if strings.TrimSpace(config.MigrationTable) != "" {
		previousTable := goose.TableName()
		goose.SetTableName(config.MigrationTable)
		defer goose.SetTableName(previousTable)
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
		err := pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)",
			extension,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("validate %s extension: %w", extension, err)
		}

		if !exists {
			return fmt.Errorf("required PostgreSQL extension %q is not enabled", extension)
		}
	}

	conn, err := acquireAgeConn(ctx, pool)
	if err != nil {
		return err
	}
	defer conn.Release()

	var graphExists bool
	if err := conn.QueryRow(
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

func acquireAgeConn(
	ctx context.Context,
	pool *pgxpool.Pool,
) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire AGE connection: %w", err)
	}

	if err := setAgeSearchPath(ctx, conn); err != nil {
		conn.Release()

		return nil, fmt.Errorf("set AGE search path: %w", err)
	}

	return conn, nil
}

func setAgeSearchPath(ctx context.Context, execer ageSearchPathExecutor) error {
	_, err := execer.Exec(ctx, "SET search_path=ag_catalog, public")

	return err
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

	if err := projectObjectTx(ctx, tx, object, index.source); err != nil {
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

func (index *Index) ProjectChanged(ctx context.Context, since time.Time) error {
	report, err := index.ProjectChangedReport(ctx, since)
	if err != nil {
		return err
	}

	_ = report

	return nil
}

func (index *Index) ProjectChangedReport(
	ctx context.Context,
	since time.Time,
) (RebuildReport, error) {
	if index.source == nil {
		return RebuildReport{}, errors.New(
			"projection source is required for incremental projection",
		)
	}

	source, ok := index.source.(query.IncrementalProjectionSource)
	if !ok {
		return RebuildReport{}, errors.New(
			"projection source does not support incremental projection",
		)
	}

	started := time.Now()
	report := RebuildReport{}
	err := source.WalkChangedProjection(
		ctx,
		since,
		func(object filestore.ProjectionObject) error {
			report.Scanned++
			if len(object.Findings) > 0 {
				report.Failed++
			}

			err := index.ProjectObject(ctx, object)
			if err != nil {
				report.Failed++

				return err
			}

			if len(object.Findings) == 0 {
				report.Projected++
			}

			return nil
		},
	)
	report.Elapsed = time.Since(started)
	observability.DefaultMetrics().ObserveDuration("gmeow_projection_lag", report.Elapsed)

	return report, err
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

	if _, err := tx.Exec(ctx, truncateProjectionTablesSQL()); err != nil {
		tx.Rollback(ctx)

		return RebuildReport{}, fmt.Errorf("truncate query projection tables: %w", err)
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

		err := index.ProjectObject(ctx, object)
		if err != nil {
			report.Failed++

			return err
		}

		if len(object.Findings) == 0 {
			report.Projected++
		}

		return nil
	})
	if err != nil {
		report.Elapsed = time.Since(started)

		return report, err
	}

	if cursorSource, ok := index.source.(query.SourceCursorProjectionSource); ok {
		err = cursorSource.WalkSourceCursors(ctx, func(cursor contracts.SourceCursor) error {
			return index.ProjectSourceCursor(ctx, cursor)
		})
	}

	report.Elapsed = time.Since(started)
	observability.DefaultMetrics().ObserveDuration("gmeow_projection_lag", report.Elapsed)

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
		var (
			result              contracts.SearchResult
			facets              []string
			objectID, mediaType string
		)

		err := rows.Scan(
			&result.ObjectDigest,
			&objectID,
			&mediaType,
			&result.Title,
			&facets,
			&total,
		)
		if err != nil {
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
		err := facetRows.Scan(&kind)
		if err != nil {
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
		var (
			part     contracts.StructurePart
			metadata []byte
		)

		err := partRows.Scan(
			&part.Digest,
			&part.Role,
			&part.Order,
			&part.Required,
			&metadata,
			&part.Facets,
		)
		if err != nil {
			return contracts.Structure{}, err
		}

		err = json.Unmarshal(metadata, &part.Metadata)
		if err != nil {
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
		err := rows.Scan(
			&relationship.Type,
			&relationship.From,
			&relationship.To,
			&relationship.Role,
			&relationship.Order,
			&relationship.Source,
		)
		if err != nil {
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
		var (
			fact     contracts.GraphFact
			metadata []byte
		)

		err := rows.Scan(
			&fact.Subject,
			&fact.Predicate,
			&fact.Object,
			&metadata,
		)
		if err != nil {
			return contracts.GraphResponse{}, err
		}

		err = json.Unmarshal(metadata, &fact.Metadata)
		if err != nil {
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
	if len(request.Analyzers) > 0 {
		return index.analysisStatusWithRequirements(ctx, request)
	}

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
		var (
			status contracts.AnalysisStatus
			data   []byte
		)

		err := rows.Scan(
			&status.ObjectDigest,
			&status.AnalyzerName,
			&status.AnalyzerVer,
			&status.Status,
			&status.GeneratedAt,
			&data,
		)
		if err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}

		err = json.Unmarshal(data, &status.Data)
		if err != nil {
			return contracts.AnalysisStatusResponse{}, err
		}

		statuses = append(statuses, status)
	}

	return contracts.AnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Statuses:      statuses,
	}, rows.Err()
}

func (index *Index) analysisStatusWithRequirements(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	objects, err := index.analysisStatusObjects(ctx, request.ObjectDigests)
	if err != nil {
		return contracts.AnalysisStatusResponse{}, err
	}

	existing, err := index.analysisStatusRowsFor(ctx, request)
	if err != nil {
		return contracts.AnalysisStatusResponse{}, err
	}

	statuses := []contracts.AnalysisStatus{}

	for _, digest := range objects {
		for _, analyzer := range request.Analyzers {
			if len(request.AnalyzerNames) > 0 &&
				!containsString(request.AnalyzerNames, analyzer.Name) {
				continue
			}

			status, ok := existing[digest][analyzer.Name]
			if !ok {
				statuses = append(statuses, contracts.AnalysisStatus{
					ObjectDigest: digest,
					AnalyzerName: analyzer.Name,
					AnalyzerVer:  analyzer.Version,
					Status:       "missing",
					Data:         map[string]any{"required_version": analyzer.Version},
				})

				continue
			}

			if analyzer.Version != "" && status.AnalyzerVer != analyzer.Version {
				status.Status = "stale"
				status.Data = map[string]any{
					"current_version":  status.AnalyzerVer,
					"required_version": analyzer.Version,
				}
			}

			statuses = append(statuses, status)
		}
	}

	limit := normalizedLimit(request.Limit)
	if len(statuses) > limit {
		statuses = statuses[:limit]
	}

	return contracts.AnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Statuses:      statuses,
	}, nil
}

func (index *Index) analysisStatusObjects(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) ([]contracts.ObjectDigest, error) {
	args := []any{}
	where := "true"

	if len(digests) > 0 {
		args = append(args, digests)
		where = fmt.Sprintf("object_digest = ANY($%d)", len(args))
	}

	rows, err := index.pool.Query(
		ctx,
		fmt.Sprintf(
			`SELECT object_digest
			   FROM query_objects
			  WHERE %s
			  ORDER BY object_digest`,
			where,
		),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objects := []contracts.ObjectDigest{}

	for rows.Next() {
		var digest contracts.ObjectDigest
		err := rows.Scan(&digest)
		if err != nil {
			return nil, err
		}

		objects = append(objects, digest)
	}

	return objects, rows.Err()
}

func (index *Index) analysisStatusRowsFor(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (map[contracts.ObjectDigest]map[string]contracts.AnalysisStatus, error) {
	analyzerNames := analyzerSpecNames(request.Analyzers)
	args := []any{analyzerNames}
	where := []string{"analyzer_name = ANY($1)"}

	if len(request.ObjectDigests) > 0 {
		args = append(args, request.ObjectDigests)
		where = append(where, fmt.Sprintf("object_digest = ANY($%d)", len(args)))
	}

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT object_digest, analyzer_name, analyzer_version, status, generated_at, data_json
		   FROM query_object_analysis
		  WHERE %s
		  ORDER BY object_digest, analyzer_name`,
		strings.Join(where, " AND "),
	), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	statuses := map[contracts.ObjectDigest]map[string]contracts.AnalysisStatus{}

	for rows.Next() {
		var (
			status contracts.AnalysisStatus
			data   []byte
		)

		err := rows.Scan(
			&status.ObjectDigest,
			&status.AnalyzerName,
			&status.AnalyzerVer,
			&status.Status,
			&status.GeneratedAt,
			&data,
		)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(data, &status.Data)
		if err != nil {
			return nil, err
		}

		if statuses[status.ObjectDigest] == nil {
			statuses[status.ObjectDigest] = map[string]contracts.AnalysisStatus{}
		}

		statuses[status.ObjectDigest][status.AnalyzerName] = status
	}

	return statuses, rows.Err()
}

func analyzerSpecNames(analyzers []contracts.AnalyzerSpec) []string {
	names := make([]string, 0, len(analyzers))
	for _, analyzer := range analyzers {
		names = append(names, analyzer.Name)
	}

	return names
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

	args = append(args, len(request.Vector))

	where = append(where, fmt.Sprintf("e.dimensions = $%d", len(args)))
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
		`SELECT object_digest, model, embedding_id, kind, source_digest, text_preview, distance
		   FROM (
		         SELECT DISTINCT ON (ranked.object_digest)
		                ranked.object_digest,
		                ranked.model,
		                ranked.embedding_id,
		                ranked.kind,
		                ranked.source_digest,
		                ranked.text_preview,
		                ranked.distance
		           FROM (
		                 SELECT e.object_digest,
		                        e.model,
		                        e.embedding_id,
		                        e.kind,
		                        e.source_digest,
		                        e.text_preview,
		                        e.embedding <=> $1::vector AS distance
		                   FROM query_object_embeddings e
		                  WHERE %s
		                ) ranked
		          ORDER BY ranked.object_digest, ranked.distance
		        ) best
		  ORDER BY distance
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
		err := rows.Scan(
			&result.ObjectDigest,
			&result.Model,
			&result.EmbeddingID,
			&result.Kind,
			&result.SourceDigest,
			&result.TextPreview,
			&result.Distance,
		)
		if err != nil {
			return contracts.VectorSearchResponse{}, err
		}

		results = append(results, result)
	}

	return contracts.VectorSearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
	}, rows.Err()
}

func (index *Index) SourceCursors(
	ctx context.Context,
	request contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	args := []any{}
	where := []string{"true"}

	if len(request.SourceKinds) > 0 {
		args = append(args, request.SourceKinds)
		where = append(where, fmt.Sprintf("source_kind = ANY($%d)", len(args)))
	}

	if len(request.SourceNames) > 0 {
		args = append(args, request.SourceNames)
		where = append(where, fmt.Sprintf("source_name = ANY($%d)", len(args)))
	}

	args = append(args, normalizedLimit(request.Limit))

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT source_kind, source_name, cursor_json, updated_at
		   FROM query_source_cursors
		  WHERE %s
		  ORDER BY source_kind, source_name
		  LIMIT $%d`,
		strings.Join(where, " AND "),
		len(args),
	), args...)
	if err != nil {
		return contracts.SourceCursorResponse{}, err
	}
	defer rows.Close()

	cursors := []contracts.SourceCursor{}

	for rows.Next() {
		var (
			cursor contracts.SourceCursor
			data   []byte
		)

		err := rows.Scan(
			&cursor.SourceKind,
			&cursor.SourceName,
			&data,
			&cursor.UpdatedAt,
		)
		if err != nil {
			return contracts.SourceCursorResponse{}, err
		}

		cursor.SchemaVersion = contracts.SchemaVersionPhase00
		err = json.Unmarshal(data, &cursor.Cursor)
		if err != nil {
			return contracts.SourceCursorResponse{}, err
		}

		cursors = append(cursors, cursor)
	}

	return contracts.SourceCursorResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Cursors:       cursors,
	}, rows.Err()
}

func projectObjectTx(
	ctx context.Context,
	tx pgx.Tx,
	object filestore.ProjectionObject,
	source query.ProjectionSource,
) error {
	rdfProjectionChanged, err := objectHadRDFRowsTx(ctx, tx, object.Manifest.ObjectDigest)
	if err != nil {
		return err
	}

	rdfProjectionChanged = rdfProjectionChanged ||
		manifestHasFacetKind(object.Manifest, contracts.RDFSourceBundleFacetKind) ||
		manifestHasFacetKind(object.Manifest, contracts.RDFClaimBundleFacetKind)

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
		"query_mail_identities",
		"query_rdf_statement_annotations",
		"query_rdf_statements",
	} {
		if _, err := tx.Exec(
			ctx,
			deleteProjectionRowsSQL(table),
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

	if err := insertMailIdentityRows(ctx, tx, object.Manifest); err != nil {
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
		source,
	); err != nil {
		return err
	}

	if err := insertOverlayRows(ctx, tx, object.Manifest, object.Annotations); err != nil {
		return err
	}

	if err := insertSummaryRows(ctx, tx, object.Manifest, object.Annotations); err != nil {
		return err
	}

	rdfRowsInserted, err := insertRDFRows(ctx, tx, object.Manifest, source)
	if err != nil {
		return err
	}

	if rdfProjectionChanged && !rdfRowsInserted {
		err := refreshContactProjectionTx(ctx, tx)
		if err != nil {
			return err
		}
	}

	if err := seedJMAPEmailStateTx(
		ctx,
		tx,
		object.Manifest,
		object.Annotations,
	); err != nil {
		return err
	}

	return nil
}

func truncateProjectionTablesSQL() string {
	return strings.Join([]string{
		"TRUNCATE",
		"query_projection_state,",
		"query_summaries,",
		"query_source_cursors,",
		"query_contact_rollups,",
		"query_contact_identity_bindings,",
		"query_contact_facts,",
		"query_rdf_statement_annotations,",
		"query_rdf_statements,",
		"query_objects",
		"CASCADE",
	}, " ")
}

func deleteProjectionRowsSQL(table string) string {
	switch table {
	case "query_object_facets":
		return "DELETE FROM query_object_facets WHERE object_digest = $1"
	case "query_object_provenance":
		return "DELETE FROM query_object_provenance WHERE object_digest = $1"
	case "query_object_relationships":
		return "DELETE FROM query_object_relationships WHERE object_digest = $1"
	case "query_object_compound_parts":
		return "DELETE FROM query_object_compound_parts WHERE object_digest = $1"
	case "query_object_analysis":
		return "DELETE FROM query_object_analysis WHERE object_digest = $1"
	case "query_object_graph_edges":
		return "DELETE FROM query_object_graph_edges WHERE object_digest = $1"
	case "query_object_keywords":
		return "DELETE FROM query_object_keywords WHERE object_digest = $1"
	case "query_object_embeddings":
		return "DELETE FROM query_object_embeddings WHERE object_digest = $1"
	case "query_object_overlays":
		return "DELETE FROM query_object_overlays WHERE object_digest = $1"
	case "query_summaries":
		return "DELETE FROM query_summaries WHERE object_digest = $1"
	case "query_mail_identities":
		return "DELETE FROM query_mail_identities WHERE object_digest = $1"
	case "query_rdf_statement_annotations":
		return "DELETE FROM query_rdf_statement_annotations WHERE source_digest = $1"
	case "query_rdf_statements":
		return "DELETE FROM query_rdf_statements WHERE source_digest = $1"
	default:
		panic("unsupported query projection delete table: " + table)
	}
}

func deleteAgeFactsForDigest(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
) error {
	err := setAgeSearchPath(ctx, tx)
	if err != nil {
		return fmt.Errorf("set AGE search path: %w", err)
	}

	cypher := fmt.Sprintf(
		`MATCH ()-[r]->() WHERE r.object_digest = %s DELETE r`,
		ageStringLiteral(string(digest)),
	)
	if _, err := tx.Exec(ctx, ageSQL(cypher, "value agtype")); err != nil {
		return fmt.Errorf("delete AGE facts for %s: %w", digest, err)
	}

	if _, err := tx.Exec(
		ctx,
		ageSQL(`MATCH (n) WHERE NOT EXISTS((n)--()) DELETE n`, "value agtype"),
	); err != nil {
		return fmt.Errorf("delete stale AGE nodes for %s: %w", digest, err)
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
		attributes, err := json.Marshal(nonNilMap(firstMap(item.Metadata, item.Attributes)))
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

	err := setAgeSearchPath(ctx, tx)
	if err != nil {
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
	source query.ProjectionSource,
) error {
	rows := embeddingRowsFrom(ctx, source, manifest, annotations)
	for _, row := range rows {
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_object_embeddings(
			   object_digest, model, embedding_object_digest, embedding_id, kind,
			   source_digest, ordinal, text_preview, metadata_json, dimensions, embedding
			 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::vector)
			 ON CONFLICT(object_digest, model, embedding_object_digest) DO UPDATE SET
			   embedding_id = excluded.embedding_id,
			   kind = excluded.kind,
			   source_digest = excluded.source_digest,
			   ordinal = excluded.ordinal,
			   text_preview = excluded.text_preview,
			   metadata_json = excluded.metadata_json,
			   dimensions = excluded.dimensions,
			   embedding = excluded.embedding`,
			manifest.ObjectDigest,
			row.model,
			firstNonEmpty(row.id, string(manifest.ObjectDigest)),
			firstNonEmpty(row.id, string(manifest.ObjectDigest)),
			row.kind,
			row.source,
			row.ordinal,
			row.preview,
			row.metadata,
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

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}

	return false
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
