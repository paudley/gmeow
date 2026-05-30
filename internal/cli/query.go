// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

func newQueryCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Manage QUERY projection",
	}
	command.AddCommand(newQueryMigrateCommand(configPath))
	command.AddCommand(newQueryRebuildCommand(out, configPath))
	command.AddCommand(newQueryProjectChangedCommand(out, configPath))
	command.AddCommand(newQueryProjectCommand(out, configPath))
	command.AddCommand(newQueryBreakdownCommand(out, configPath))
	command.AddCommand(newQuerySearchCommand(out, configPath))
	command.AddCommand(newQueryMailMissingGmailCommand(out, configPath))
	command.AddCommand(newQueryAgeCommand(out, configPath))
	command.AddCommand(newQueryTokenCommand(out, configPath))
	command.AddCommand(newQueryServeCommand(out, configPath, "serve"))

	return command
}

func newQueryTokenCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "token",
		Short: "Manage JMAP bearer tokens",
	}
	command.AddCommand(newQueryTokenCreateCommand(out, configPath))

	return command
}

func newQueryTokenCreateCommand(out io.Writer, configPath *string) *cobra.Command {
	var clientID string

	command := &cobra.Command{
		Use:   "create",
		Short: "Create a JMAP bearer token for a client",
		RunE: func(command *cobra.Command, _ []string) error {
			if clientID == "" {
				return errors.New("--client-id is required")
			}
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			token, err := index.CreateBearerToken(command.Context(), clientID)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, token)

			return err
		},
	}
	command.Flags().
		StringVar(&clientID, "client-id", "", "client identifier for the token")

	return command
}

func newQueryServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: "Run the QUERY gRPC service",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			endpoint := rpcEndpoint(loaded.Resolved.RPC.Query)
			if _, err := fmt.Fprintf(
				out,
				"query serve: %s %s\n",
				endpoint.Network,
				endpoint.Address,
			); err != nil {
				return err
			}

			return rpc.Serve(command.Context(), endpoint, func(server *grpc.Server) {
				pb.RegisterQueryServiceServer(server, rpc.NewQueryServer(index))
			})
		},
	}
}

func newQueryMigrateCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Run QUERY PostgreSQL migrations",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			return querypg.Migrate(command.Context(), queryPostgresConfig(loaded))
		},
	}
}

func newQueryRebuildCommand(out io.Writer, configPath *string) *cobra.Command {
	var confirmInstance string

	command := &cobra.Command{
		Use:   "rebuild",
		Short: "Rebuild QUERY from FILESTORE",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"query rebuild",
				confirmInstance,
			); err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			report, err := index.RebuildReport(command.Context())
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"query rebuild: scanned=%d projected=%d failed=%d elapsed=%s\n",
				report.Scanned,
				report.Projected,
				report.Failed,
				report.Elapsed,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "confirm production-like instance id before rebuilding")

	return command
}

func newQueryBreakdownCommand(out io.Writer, configPath *string) *cobra.Command {
	var jsonOutput bool

	command := &cobra.Command{
		Use:   "breakdown",
		Short: "Detailed aggregate breakdown of projected objects (facets, sources, media types, analysis)",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			index, err := openQueryIndexFromPath(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer index.Close()

			breakdown, err := index.ObjectBreakdown(command.Context())
			if err != nil {
				return err
			}

			if jsonOutput {
				encoded, err := json.MarshalIndent(breakdown, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(out, string(encoded))

				return err
			}

			return printObjectBreakdown(out, breakdown)
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit the breakdown as JSON")

	return command
}

func printObjectBreakdown(out io.Writer, breakdown querypg.ObjectBreakdown) error {
	analyzedPct := 0.0
	if breakdown.TotalObjects > 0 {
		analyzedPct = 100 * float64(
			breakdown.ObjectsWithAnalysis,
		) / float64(
			breakdown.TotalObjects,
		)
	}

	fmt.Fprintf(out, "objects:            %d\n", breakdown.TotalObjects)
	fmt.Fprintf(out, "  compound:         %d\n", breakdown.CompoundObjects)
	fmt.Fprintf(out, "  simple:           %d\n", breakdown.SimpleObjects)
	fmt.Fprintf(out, "content bytes:      %d\n", breakdown.TotalSizeBytes)
	fmt.Fprintf(
		out,
		"with analysis:      %d (%.1f%%)\n",
		breakdown.ObjectsWithAnalysis,
		analyzedPct,
	)

	writer := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	section := func(title string, rows []querypg.CountRow) {
		fmt.Fprintf(writer, "\n%s\t\n", title)
		for _, row := range rows {
			label := row.Label
			if label == "" {
				label = "(none)"
			}
			fmt.Fprintf(writer, "  %s\t%d\n", label, row.Count)
		}
	}

	section("BY FACET", breakdown.ByFacet)

	fmt.Fprintf(writer, "\nBY SOURCE\t\n")
	for _, row := range breakdown.BySource {
		fmt.Fprintf(writer, "  %s/%s\t%d\n", row.SourceKind, row.SourceName, row.Objects)
	}

	section("BY MEDIA TYPE", breakdown.ByMediaType)
	section("BY IDENTITY STRATEGY", breakdown.ByIdentityStrategy)
	section("BY ANALYZER", breakdown.ByAnalyzer)

	return writer.Flush()
}

func newQueryProjectChangedCommand(out io.Writer, configPath *string) *cobra.Command {
	var sinceValue string

	command := &cobra.Command{
		Use:   "project-changed",
		Short: "Project changed FILESTORE annotations into QUERY",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			since, err := parseSince(sinceValue)
			if err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			report, err := index.ProjectChangedReport(command.Context(), since)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				out,
				"query project-changed: scanned=%d projected=%d failed=%d elapsed=%s\n",
				report.Scanned,
				report.Projected,
				report.Failed,
				report.Elapsed,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&sinceValue, "since", "", "only project objects changed after RFC3339 timestamp")

	return command
}

func newQueryProjectCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "project <digest>",
		Short: "Project one FILESTORE object into QUERY",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			store, err := openProjectionStore(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()

			digest := contracts.ObjectDigest(args[0])
			object, found, err := store.ProjectionObject(command.Context(), digest)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("object %s was not found in FILESTORE", digest)
			}
			if err := index.ProjectObject(command.Context(), object); err != nil {
				return err
			}

			_, err = fmt.Fprintf(out, "query project: digest=%s\n", digest)

			return err
		},
	}
}

func parseSince(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse --since as RFC3339 timestamp: %w", err)
	}

	return parsed, nil
}

func newQuerySearchCommand(out io.Writer, configPath *string) *cobra.Command {
	var facets []string

	command := &cobra.Command{
		Use:   "search <text>",
		Short: "Search projected QUERY objects",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			index, err := openQueryIndex(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer index.Close()

			response, err := index.Search(command.Context(), contracts.SearchRequest{
				SchemaVersion: contracts.SchemaVersionPhase00,
				Query:         args[0],
				Facets:        facets,
				Limit:         20,
			})
			if err != nil {
				return err
			}

			encoded, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().StringSliceVar(&facets, "facet", nil, "facet filter")

	return command
}

func newQueryMailMissingGmailCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		sourceNames      []string
		includeGenerated bool
		collisionsOnly   bool
		limit            int
		offset           int
	)

	command := &cobra.Command{
		Use:   "mail-missing-gmail",
		Short: "List archive Message-IDs absent from Gmail",
		RunE: func(command *cobra.Command, _ []string) error {
			index, err := openQueryIndexFromPath(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer index.Close()

			response, err := index.MailArchiveMissingGmail(
				command.Context(),
				contracts.MailIdentityReportRequest{
					SourceNames:      sourceNames,
					IncludeGenerated: includeGenerated,
					CollisionsOnly:   collisionsOnly,
					Limit:            limit,
					Offset:           offset,
				},
			)
			if err != nil {
				return err
			}
			encoded, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().
		StringSliceVar(&sourceNames, "source-name", nil, "archive source name filter")
	command.Flags().
		BoolVar(&includeGenerated, "include-generated", false, "include deterministic generated Message-IDs")
	command.Flags().
		BoolVar(&collisionsOnly, "collisions-only", false, "include only Message-ID collision groups")
	command.Flags().IntVar(&limit, "limit", 50, "maximum rows to return")
	command.Flags().IntVar(&offset, "offset", 0, "rows to skip")

	return command
}

func newQueryAgeCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "age",
		Short: "Inspect Apache AGE graph projection",
	}
	command.AddCommand(newQueryAgeStatusCommand(out, configPath))
	command.AddCommand(newQueryAgeCypherCommand(out, configPath))

	return command
}

func newQueryAgeStatusCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print AGE graph status",
		RunE: func(command *cobra.Command, _ []string) error {
			index, err := openQueryIndexFromPath(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer index.Close()

			status := index.AgeStatus(command.Context())
			if status.Error != "" {
				return errors.New(status.Error)
			}

			_, err = fmt.Fprintf(
				out,
				"query age: available=%t graph=%s graphid=%d nodes=%s\n",
				status.Available,
				status.Graph,
				status.GraphID,
				status.Nodes,
			)

			return err
		},
	}
}

func newQueryAgeCypherCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		columns string
		limit   int
	)

	command := &cobra.Command{
		Use:   "cypher <match-query>",
		Short: "Run a read-only AGE MATCH query",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			index, err := openQueryIndexFromPath(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer index.Close()

			rows, err := index.AgeCypher(command.Context(), args[0], columns, limit)
			if err != nil {
				return err
			}

			encoded, err := json.MarshalIndent(rows, "", "  ")
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().
		StringVar(&columns, "columns", "value agtype", "AGE result column declaration")
	command.Flags().IntVar(&limit, "limit", 100, "maximum rows when query omits LIMIT")

	return command
}

func openQueryIndexFromPath(
	ctx context.Context,
	configPath *string,
) (*querypg.Index, error) {
	loaded, err := config.Load(config.Options{Path: *configPath})
	if err != nil {
		return nil, err
	}

	return openQueryIndex(ctx, loaded)
}

func openQueryIndex(
	ctx context.Context,
	loaded *config.Loaded,
) (*querypg.Index, error) {
	store, err := openProjectionStore(ctx, loaded)
	if err != nil {
		return nil, err
	}

	config := queryPostgresConfig(loaded)
	if err := querypg.Migrate(ctx, config); err != nil {
		return nil, err
	}

	return querypg.New(ctx, config, store)
}

// openProjectionStore returns a projection source backed by the FILESTORE gRPC
// service. QUERY must not open the FILESTORE root directly: FILESTORE owns its
// metadata store (a single-writer Pebble LSM), so projection reads stream over
// the typed service boundary like every other cross-service access.
func openProjectionStore(
	ctx context.Context,
	loaded *config.Loaded,
) (*rpc.FilestoreClient, error) {
	return rpc.NewFilestoreClient(ctx, rpcEndpoint(loaded.Resolved.RPC.Filestore))
}

func queryPostgresConfig(loaded *config.Loaded) querypg.Config {
	return querypg.Config{
		ConnString:    queryPostgresConnString(loaded.Resolved.Postgres),
		MigrationsDir: queryMigrationsDir(loaded),
	}
}

func queryPostgresConnString(postgres config.ResolvedPostgres) string {
	values := url.Values{}
	if postgres.SSLMode != "" {
		values.Set("sslmode", postgres.SSLMode)
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(postgres.User, postgres.Password),
		Host:     postgres.Host + ":" + strconv.Itoa(postgres.Port),
		Path:     postgres.Database,
		RawQuery: values.Encode(),
	}

	return dsn.String()
}

func queryMigrationsDir(loaded *config.Loaded) string {
	base := "."
	if loaded.Path != "" {
		base = filepath.Dir(loaded.Path)
	}

	candidate := filepath.Join(base, "migrations", "query")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}

	return "migrations/query"
}
