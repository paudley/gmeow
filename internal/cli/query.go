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
	"time"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
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
	command.AddCommand(newQuerySearchCommand(out, configPath))
	command.AddCommand(newQueryAgeCommand(out, configPath))
	return command
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
	return &cobra.Command{
		Use:   "rebuild",
		Short: "Rebuild QUERY from FILESTORE",
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
			store, err := openProjectionStore(loaded)
			if err != nil {
				return err
			}
			digest := contracts.ObjectDigest(args[0])
			projected := false
			if err := store.WalkProjection(
				command.Context(),
				func(object filestore.ProjectionObject) error {
					if object.Digest != digest {
						return nil
					}
					projected = true
					return index.ProjectObject(command.Context(), object)
				},
			); err != nil {
				return err
			}
			if !projected {
				return fmt.Errorf("object %s was not found in FILESTORE", digest)
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
	var columns string
	var limit int
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
	store, err := openProjectionStore(loaded)
	if err != nil {
		return nil, err
	}
	config := queryPostgresConfig(loaded)
	if err := querypg.Migrate(ctx, config); err != nil {
		return nil, err
	}
	return querypg.New(ctx, config, store)
}

func openProjectionStore(loaded *config.Loaded) (*filestore.FilesystemStore, error) {
	root, err := resolvedFilestoreRoot(loaded)
	if err != nil {
		return nil, err
	}
	return filestore.NewFilesystemStore(root), nil
}

func queryPostgresConfig(loaded *config.Loaded) querypg.Config {
	return querypg.Config{
		ConnString:    queryPostgresConnString(loaded.Resolved.Postgres),
		MigrationsDir: queryMigrationsDir(loaded),
	}
}

func queryPostgresConnString(postgres config.ResolvedPostgres) string {
	if !postgres.Enabled {
		return ""
	}
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
