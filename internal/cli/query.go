// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contactio"
	"blackcat.ca/gmeow/internal/contracts"
	querypg "blackcat.ca/gmeow/internal/query/postgres"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/source"
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
	command.AddCommand(newQueryContactCommand(out, configPath))
	command.AddCommand(newQueryServeCommand(out, configPath, "serve"))

	return command
}

func newQueryContactCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "contact",
		Short: "Manage contact intelligence projections",
	}
	command.AddCommand(newQueryContactSearchCommand(out, configPath))
	command.AddCommand(newQueryContactAggregateCommand(out, configPath))
	command.AddCommand(newQueryContactResolveCommand(out, configPath))
	command.AddCommand(newQueryContactIdentitiesCommand(out, configPath))
	command.AddCommand(newQueryContactFactsCommand(out, configPath))
	command.AddCommand(newQueryContactNeighborhoodCommand(out, configPath))
	command.AddCommand(newQueryContactMessagesCommand(out, configPath))
	command.AddCommand(newQueryContactSimilarCommand(out, configPath))
	command.AddCommand(newQueryContactVectorSearchCommand(out, configPath))
	command.AddCommand(newQueryContactAnalysisInputsCommand(out, configPath))
	command.AddCommand(newQueryContactAnalysisStatusCommand(out, configPath))
	command.AddCommand(newQueryContactAnalyzeCommand(out, configPath))

	return command
}

// newContactCommand is the top-level contact group. import/export talk only to
// FILESTORE (never QUERY), so they live here rather than under the query
// projection namespace.
func newContactCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "contact",
		Short: "Import and export contacts (FILESTORE-only)",
	}
	command.AddCommand(newContactImportCommand(out, configPath))
	command.AddCommand(newContactExportCommand(out, configPath))
	command.AddCommand(newContactResetEntitiesCommand(out, configPath))
	command.AddCommand(newContactEmbeddingStatusCommand(out, configPath))

	return command
}

// newContactEmbeddingStatusCommand reports the EMBEDDING service's cache/index
// counters and the cumulative cache hit rate (= 1 - embedder_calls/lookups),
// which should converge toward 1.0 as the corpus's distinct claims get cached.
func newContactEmbeddingStatusCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "embedding-status",
		Short: "Report EMBEDDING cache size, entity count, and cache hit rate",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			client, err := rpc.NewEmbeddingClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Embedding),
			)
			if err != nil {
				return err
			}
			defer client.Close()
			status, err := client.Status(command.Context())
			if err != nil {
				return err
			}
			hitRate := 0.0
			if status.TotalLookups > 0 {
				hitRate = 1 - float64(status.EmbedderCalls)/float64(status.TotalLookups)
			}

			return writeAppJSON(out, map[string]any{
				"model":          status.Model,
				"cached_claims":  status.CachedClaims,
				"entities":       status.Entities,
				"total_lookups":  status.TotalLookups,
				"embedder_calls": status.EmbedderCalls,
				"cache_hit_rate": hitRate,
			}, nil)
		},
	}
}

// newContactResetEntitiesCommand clears the EMBEDDING service's resolved entity
// space (index + ledger) while KEEPING its claim-vector cache, so a threshold
// sweep can re-import the same corpus at new --match-threshold/--name-threshold
// without re-embedding. (Wipe FILESTORE separately, with filestore-serve stopped.)
func newContactResetEntitiesCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "reset-entities",
		Short: "Clear the EMBEDDING entity space (keep the claim cache) for a threshold sweep",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			client, err := rpc.NewEmbeddingClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Embedding),
			)
			if err != nil {
				return err
			}
			defer client.Close()
			if err := client.Reset(command.Context()); err != nil {
				return err
			}
			_, err = fmt.Fprintln(out, "embedding entity space reset (claim cache kept)")

			return err
		},
	}
}

func newQueryContactSearchCommand(out io.Writer, configPath *string) *cobra.Command {
	var limitOffset contactLimitOffsetFlags

	command := &cobra.Command{
		Use:   "search <text>",
		Short: "Search contact rollups",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactSearchResponse,
					error,
				) {
					return index.ContactSearch(ctx, contracts.ContactSearchRequest{
						Query:  args[0],
						Limit:  limitOffset.limit,
						Offset: limitOffset.offset,
					})
				},
			)
		},
	}
	addLimitOffsetFlags(command, &limitOffset)

	return command
}

func newQueryContactAggregateCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "aggregate <contact-id>",
		Short: "Fetch a contact aggregate",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (contracts.ContactAggregate, error) {
					return index.ContactAggregate(
						ctx,
						contracts.ContactAggregateRequest{ContactID: args[0]},
					)
				},
			)
		},
	}
}

func newQueryContactResolveCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "resolve <identity>",
		Short: "Resolve an identity token to contacts",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactIdentityResolveResponse,
					error,
				) {
					return index.ResolveContactIdentity(
						ctx,
						contracts.ContactIdentityResolveRequest{Identity: args[0]},
					)
				},
			)
		},
	}
}

func newQueryContactIdentitiesCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var request contactIdentityFlags

	command := &cobra.Command{
		Use:   "identities",
		Short: "List contact identity bindings",
		RunE: func(command *cobra.Command, _ []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactIdentityDetailResponse,
					error,
				) {
					return index.ContactIdentityDetails(
						ctx,
						contracts.ContactIdentityDetailRequest{
							Identities: request.identities,
							ContactIDs: request.contactIDs,
							Limit:      request.limitOffset.limit,
							Offset:     request.limitOffset.offset,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringSliceVar(&request.identities, "identity", nil, "identity token filter")
	command.Flags().
		StringSliceVar(&request.contactIDs, "contact-id", nil, "contact id filter")
	addLimitOffsetFlags(command, &request.limitOffset)

	return command
}

func newQueryContactFactsCommand(out io.Writer, configPath *string) *cobra.Command {
	var request contactFactFlags

	command := &cobra.Command{
		Use:   "facts",
		Short: "List projected contact facts",
		RunE: func(command *cobra.Command, _ []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactFactResponse,
					error,
				) {
					return index.ContactFacts(
						ctx,
						contracts.ContactFactRequest{
							At:         request.at,
							From:       request.from,
							Until:      request.until,
							ContactIDs: request.contactIDs,
							FactKinds:  request.factKinds,
							Limit:      request.limitOffset.limit,
							Offset:     request.limitOffset.offset,
							Current:    request.current,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringSliceVar(&request.contactIDs, "contact-id", nil, "contact id filter")
	command.Flags().
		StringSliceVar(&request.factKinds, "fact-kind", nil, "contact fact kind filter")
	command.Flags().StringVar(&request.at, "at", "", "temporal point filter")
	command.Flags().StringVar(&request.from, "from", "", "temporal interval start filter")
	command.Flags().StringVar(&request.until, "until", "", "temporal interval end filter")
	command.Flags().
		BoolVar(&request.current, "current", false, "include only current facts")
	addLimitOffsetFlags(command, &request.limitOffset)

	return command
}

func newQueryContactNeighborhoodCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var (
		factKinds   []string
		limitOffset contactLimitOffsetFlags
	)

	command := &cobra.Command{
		Use:   "neighborhood <contact-id>",
		Short: "List a contact neighborhood",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactNeighborhoodResponse,
					error,
				) {
					return index.ContactNeighborhood(
						ctx,
						contracts.ContactNeighborhoodRequest{
							ContactID: args[0],
							FactKinds: factKinds,
							Limit:     limitOffset.limit,
							Offset:    limitOffset.offset,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringSliceVar(&factKinds, "fact-kind", nil, "contact fact kind filter")
	addLimitOffsetFlags(command, &limitOffset)

	return command
}

func newQueryContactMessagesCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		role        string
		limitOffset contactLimitOffsetFlags
	)

	command := &cobra.Command{
		Use:   "messages <contact-id>",
		Short: "List messages joined through contact identity bindings",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactMessageResponse,
					error,
				) {
					return index.ContactMessages(
						ctx,
						contracts.ContactMessageRequest{
							ContactID: args[0],
							Role:      role,
							Limit:     limitOffset.limit,
							Offset:    limitOffset.offset,
						},
					)
				},
			)
		},
	}
	command.Flags().StringVar(&role, "role", "", "mail participant role filter")
	addLimitOffsetFlags(command, &limitOffset)

	return command
}

func newQueryContactSimilarCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		model string
		limit int
	)

	command := &cobra.Command{
		Use:   "similar <contact-id>",
		Short: "Find contacts with similar embeddings",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.SimilarContactsResponse,
					error,
				) {
					return index.SimilarContacts(
						ctx,
						contracts.SimilarContactsRequest{
							ContactID: args[0],
							Model:     model,
							Limit:     limit,
						},
					)
				},
			)
		},
	}
	command.Flags().StringVar(&model, "model", "", "embedding model filter")
	command.Flags().IntVar(&limit, "limit", 20, "maximum results")

	return command
}

func newQueryContactVectorSearchCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var request contactVectorFlags

	command := &cobra.Command{
		Use:   "vector-search",
		Short: "Search contacts by embedding vector",
		RunE: func(command *cobra.Command, _ []string) error {
			vector, err := parseFloat32CSV(request.vector)
			if err != nil {
				return err
			}

			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactVectorSearchResponse,
					error,
				) {
					return index.ContactVectorSearch(
						ctx,
						contracts.ContactVectorSearchRequest{
							Vector:     vector,
							Model:      request.model,
							ContactIDs: request.contactIDs,
							Limit:      request.limit,
							Dimensions: request.dimensions,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringVar(&request.vector, "vector", "", "comma-separated float32 vector")
	_ = command.MarkFlagRequired("vector")
	command.Flags().StringVar(&request.model, "model", "", "embedding model filter")
	command.Flags().
		StringSliceVar(&request.contactIDs, "contact-id", nil, "contact id filter")
	command.Flags().
		IntVar(&request.dimensions, "dimensions", 0, "embedding dimensions filter")
	command.Flags().IntVar(&request.limit, "limit", 20, "maximum results")

	return command
}

func newQueryContactAnalysisInputsCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var request contactAnalysisInputFlags

	command := &cobra.Command{
		Use:   "analysis-inputs",
		Short: "List contact analysis inputs",
		RunE: func(command *cobra.Command, _ []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactAnalysisInputResponse,
					error,
				) {
					return index.ContactAnalysisInputs(
						ctx,
						contracts.ContactAnalysisInputRequest{
							ContactIDs: request.contactIDs,
							FactKinds:  request.factKinds,
							Limit:      request.limitOffset.limit,
							Offset:     request.limitOffset.offset,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringSliceVar(&request.contactIDs, "contact-id", nil, "contact id filter")
	command.Flags().
		StringSliceVar(&request.factKinds, "fact-kind", nil, "contact fact kind filter")
	addLimitOffsetFlags(command, &request.limitOffset)

	return command
}

func newQueryContactAnalysisStatusCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var request contactAnalysisStatusFlags

	command := &cobra.Command{
		Use:   "analysis-status",
		Short: "List contact analysis status rows",
		RunE: func(command *cobra.Command, _ []string) error {
			return writeContactQueryJSON(
				command,
				out,
				configPath,
				func(ctx context.Context, index *querypg.Index) (
					contracts.ContactAnalysisStatusResponse,
					error,
				) {
					return index.ContactAnalysisStatus(
						ctx,
						contracts.ContactAnalysisStatusRequest{
							ContactIDs:      request.contactIDs,
							InputHashes:     request.inputHashes,
							AnalyzerName:    request.analyzerName,
							AnalyzerVersion: request.analyzerVersion,
							Model:           request.model,
							Limit:           request.limitOffset.limit,
							Offset:          request.limitOffset.offset,
						},
					)
				},
			)
		},
	}
	command.Flags().
		StringSliceVar(&request.contactIDs, "contact-id", nil, "contact id filter")
	command.Flags().
		StringSliceVar(&request.inputHashes, "input-hash", nil, "analysis input hash filter")
	command.Flags().
		StringVar(&request.analyzerName, "analyzer-name", "", "analyzer name filter")
	command.Flags().
		StringVar(&request.analyzerVersion, "analyzer-version", "", "analyzer version filter")
	command.Flags().StringVar(&request.model, "model", "", "embedding model filter")
	addLimitOffsetFlags(command, &request.limitOffset)

	return command
}

func newQueryContactAnalyzeCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		contactIDs []string
		factKinds  []string
		limit      int
		offset     int
		forced     bool
	)

	command := &cobra.Command{
		Use:   "analyze",
		Short: "Generate QUERY-owned contact embeddings",
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

			analyzer, err := analysis.NewEmbeddingAnalyzer(analysis.EmbeddingConfig{
				Endpoint: loaded.Config.Analysis.Embeddings.Endpoint,
				Model:    loaded.Config.Analysis.Embeddings.Model,
			})
			if err != nil {
				return err
			}

			response, err := analysis.AnalyzeContacts(
				command.Context(),
				index,
				analyzer,
				contracts.ContactAnalysisRequest{
					ContactIDs: contactIDs,
					FactKinds:  factKinds,
					Limit:      limit,
					Offset:     offset,
					Forced:     forced,
				},
			)
			return writeAppJSON(out, response, err)
		},
	}
	command.Flags().StringSliceVar(&contactIDs, "contact-id", nil, "contact id filter")
	command.Flags().
		StringSliceVar(&factKinds, "fact-kind", nil, "contact fact kind filter")
	command.Flags().IntVar(&limit, "limit", 50, "maximum contacts to analyze")
	command.Flags().IntVar(&offset, "offset", 0, "contact offset")
	command.Flags().
		BoolVar(&forced, "force", false, "recompute current contact embeddings")

	return command
}

func newContactImportCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		format         string
		importLevel    int
		sourceName     string
		matchThreshold float64
		nameThreshold  float64
	)

	command := &cobra.Command{
		Use:   "import <path...>",
		Short: "Import vCard, BBDB, CSV, GEDCOM, RDF/Turtle, or native contact bundles",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if strings.TrimSpace(sourceName) == "" {
				return errors.New("--source-name is required")
			}

			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			filestoreClient, err := rpc.NewFilestoreClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Filestore),
			)
			if err != nil {
				return err
			}
			defer filestoreClient.Close()

			ingestService, err := source.NewService(filestoreClient)
			if err != nil {
				return err
			}

			// Ingest-time resolution always runs through the EMBEDDING service over
			// gRPC — it owns the single claim-vector cache + entity index + ledger
			// and persists them across runs. There is deliberately no in-process
			// pathway: one resolution authority, one converging cache.
			client, err := rpc.NewEmbeddingClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Embedding),
			)
			if err != nil {
				return fmt.Errorf("connect to EMBEDDING service: %w", err)
			}
			defer client.Close()
			var resolver contactio.Resolver = client

			paths, err := expandContactImportPaths(args, format)
			if err != nil {
				return err
			}

			results := []contactio.ImportResult{}
			for _, item := range paths {
				result, err := importContactPath(
					command.Context(),
					ingestService,
					resolver,
					matchThreshold,
					nameThreshold,
					item.format,
					importLevel,
					sourceName,
					item.path,
				)
				switch {
				case errors.Is(err, contactio.ErrSkipNonContactDomain):
					results = append(results, contactio.ImportResult{
						SourceKind:  contactio.ImportSourceKind,
						SourceName:  sourceName,
						ExternalID:  filepath.ToSlash(item.path),
						Format:      contactio.NormalizeFormat(item.format),
						ImportLevel: importLevel,
						Error:       "skipped: not contact-domain",
					})
				case err != nil:
					results = append(results, contactio.ImportResult{
						SourceKind:  contactio.ImportSourceKind,
						SourceName:  sourceName,
						ExternalID:  filepath.ToSlash(item.path),
						Format:      contactio.NormalizeFormat(item.format),
						ImportLevel: importLevel,
						Error:       err.Error(),
					})
				default:
					results = append(results, result)
				}
			}

			return writeAppJSON(out, results, nil)
		},
	}
	command.Flags().StringVar(
		&format,
		"format",
		contactio.FormatVCard,
		"contact import format: auto (detect per file), vcard, apple-addressbook, "+
			"apple-addressbook-group, bbdb, csv, gedcom, rdf/ttl, or native",
	)
	command.Flags().StringVar(&sourceName, "source-name", "", "contact import source name")
	command.Flags().IntVar(
		&importLevel,
		"import-level",
		-1,
		"contact importance level for this source, 0 noticed through 10 core",
	)
	_ = command.MarkFlagRequired("import-level")
	command.Flags().Float64Var(
		&matchThreshold,
		"match-threshold",
		0.72,
		"entity-resolution cosine-similarity threshold: a record at or above this "+
			"matches an existing entity, otherwise it mints a new one (tune per corpus)",
	)
	command.Flags().Float64Var(
		&nameThreshold,
		"name-threshold",
		0.88,
		"name-confirmation cosine threshold: a centroid match is rejected unless the "+
			"names also agree this strongly (stricter than --match-threshold; "+
			"separates same-first-name people)",
	)

	return command
}

type contactImportItem struct {
	path   string
	format string
}

// expandContactImportPaths resolves the import arguments into concrete
// (file, format) work items. Directories are walked recursively. When format is
// "auto" the format is inferred per file from its extension and files with no
// recognized contact extension are skipped; otherwise the explicit format is
// applied to every file.
func expandContactImportPaths(
	args []string,
	format string,
) ([]contactImportItem, error) {
	auto := strings.EqualFold(strings.TrimSpace(format), "auto")
	items := []contactImportItem{}

	add := func(path string) {
		resolved := format
		if auto {
			resolved = contactio.FormatForPath(path)
			if resolved == "" {
				return // not a recognized contact-domain file
			}
		}
		items = append(items, contactImportItem{path: path, format: resolved})
	}

	for _, arg := range args {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			add(arg)

			continue
		}
		walkErr := filepath.WalkDir(
			arg,
			func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					add(path)
				}

				return nil
			},
		)
		if walkErr != nil {
			return nil, walkErr
		}
	}

	return items, nil
}

func importContactPath(
	ctx context.Context,
	ingestService source.IngestService,
	resolver contactio.Resolver,
	matchThreshold float64,
	nameThreshold float64,
	format string,
	importLevel int,
	sourceName string,
	path string,
) (contactio.ImportResult, error) {
	if err := contactio.ValidateImportLevel(importLevel); err != nil {
		return contactio.ImportResult{}, err
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return contactio.ImportResult{}, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return contactio.ImportResult{}, err
	}

	// Ingest-time entity resolution: each logical contact is resolved against the
	// accumulating entity space, and only its NEW claims are persisted as an
	// immutable delta record keyed by the resolved entity ULID. A record that
	// carries no new information resolves to a NOOP and writes nothing.
	observedAt := stat.ModTime().UTC()
	records, result, err := contactio.ResolveImport(
		ctx,
		resolver,
		matchThreshold,
		nameThreshold,
		format,
		sourceName,
		content,
		contactio.ImportOptions{ImportLevel: importLevel},
		observedAt,
	)
	if err != nil {
		return contactio.ImportResult{}, err
	}

	created := 0
	for _, record := range records {
		if record.IsNoop {
			continue
		}
		fingerprint := contactDeltaFingerprint(record.Content)
		digest, wasCreated, ingestErr := ingestService.Ingest(ctx, source.IngestObject{
			ObservedAt:   observedAt,
			Reader:       strings.NewReader(record.Content),
			MediaType:    contactio.MediaTypeTurtle,
			SourceKind:   contactio.ImportSourceKind,
			SourceName:   sourceName,
			ExternalID:   contactio.EntityPrefix + record.Entity + ":" + fingerprint[:16],
			ExternalVer:  fingerprint,
			SourceHint:   filepath.ToSlash(path),
			ContentRoles: []string{contracts.RDFSourceBundleRole, contracts.ContactSourceRole},
			Facets:       record.Facets,
		})
		if ingestErr != nil {
			return contactio.ImportResult{}, ingestErr
		}
		if wasCreated {
			created++
		}
		if result.ObjectDigest == "" {
			result.ObjectDigest = string(digest)
		}
	}

	result.Created = created > 0
	return result, nil
}

// contactDeltaFingerprint is the content version key for a contact delta: equal
// content (e.g. the same contact across repeated snapshots) yields the same key
// and dedups via the source-object index.
func contactDeltaFingerprint(content string) string {
	sum := sha256.Sum256([]byte(content))

	return hex.EncodeToString(sum[:])
}

func newContactExportCommand(out io.Writer, configPath *string) *cobra.Command {
	var format string

	command := &cobra.Command{
		Use:   "export <contact-id...>",
		Short: "Export contacts as vCard, FOAF/RDF, or native bundles",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			index, err := openQueryIndexFromPath(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer index.Close()

			contacts := make([]contracts.ContactAggregate, 0, len(args))
			for _, contactID := range args {
				aggregate, err := index.ContactAggregate(
					command.Context(),
					contracts.ContactAggregateRequest{ContactID: contactID},
				)
				if err != nil {
					return err
				}
				contacts = append(contacts, aggregate)
			}

			output, err := contactio.Export(format, contacts)
			if err != nil {
				return err
			}
			_, err = fmt.Fprint(out, output)

			return err
		},
	}
	command.Flags().StringVar(
		&format,
		"format",
		contactio.FormatVCard,
		"contact export format: vcard, foaf, or native",
	)

	return command
}

func writeContactQueryJSON[T any](
	command *cobra.Command,
	out io.Writer,
	configPath *string,
	query func(context.Context, *querypg.Index) (T, error),
) error {
	index, err := openQueryIndexFromPath(command.Context(), configPath)
	if err != nil {
		var zero T

		return writeAppJSON(out, zero, err)
	}
	defer index.Close()

	response, err := query(command.Context(), index)

	return writeAppJSON(out, response, err)
}

type contactLimitOffsetFlags struct {
	limit  int
	offset int
}

type contactIdentityFlags struct {
	limitOffset contactLimitOffsetFlags
	identities  []string
	contactIDs  []string
}

type contactFactFlags struct {
	limitOffset contactLimitOffsetFlags
	at          string
	from        string
	until       string
	contactIDs  []string
	factKinds   []string
	current     bool
}

type contactVectorFlags struct {
	vector     string
	model      string
	contactIDs []string
	limit      int
	dimensions int
}

type contactAnalysisInputFlags struct {
	limitOffset contactLimitOffsetFlags
	contactIDs  []string
	factKinds   []string
}

type contactAnalysisStatusFlags struct {
	limitOffset     contactLimitOffsetFlags
	analyzerName    string
	analyzerVersion string
	model           string
	contactIDs      []string
	inputHashes     []string
}

func addLimitOffsetFlags(command *cobra.Command, target *contactLimitOffsetFlags) {
	command.Flags().IntVar(&target.limit, "limit", 50, "maximum rows to return")
	command.Flags().IntVar(&target.offset, "offset", 0, "rows to skip")
}

func parseFloat32CSV(raw string) ([]float32, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("--vector is required")
	}

	parts := strings.Split(raw, ",")
	vector := make([]float32, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		value, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			return nil, fmt.Errorf("parse --vector value %q: %w", trimmed, err)
		}
		vector = append(vector, float32(value))
	}

	return vector, nil
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
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			client, err := rpc.NewQueryClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Query),
			)
			if err != nil {
				return err
			}
			defer client.Close()

			breakdown, err := client.ObjectBreakdown(command.Context())
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

func printObjectBreakdown(out io.Writer, breakdown contracts.ObjectBreakdown) error {
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
	section := func(title string, rows []contracts.BreakdownCount) {
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
