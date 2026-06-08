// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

// filestoreReader is the read-only FILESTORE surface the inspection commands
// need. Both the local *filestore.FilesystemStore and the gRPC
// *rpc.FilestoreClient satisfy it, so inspection works whether or not
// filestore-serve is running.
type filestoreReader interface {
	LookupSourceObject(
		context.Context,
		contracts.SourceObjectRef,
	) (contracts.ObjectDigest, bool, error)
	Verify(context.Context, filestore.VerifyRequest) (filestore.VerifyReport, error)
	StorageBreakdown(
		context.Context,
		filestore.StorageBreakdownRequest,
	) (filestore.StorageBreakdownReport, error)
	ResolvePath(
		context.Context,
		filestore.PathResolveRequest,
	) (filestore.PathResolveReport, error)
	DeleteObject(context.Context, contracts.ObjectDigest) error
	Gc(context.Context) (filestore.GCReport, error)
	Repack(context.Context) (filestore.RepackReport, error)
	TrainDictionary(
		context.Context,
		filestore.TrainDictionaryRequest,
	) (filestore.TrainDictionaryReport, error)
	io.Closer
}

// openFilestoreReader returns a read-only FILESTORE handle. When filestore-serve
// is running it holds the metadata store's exclusive lock, so the gRPC service
// is preferred; otherwise the store is opened locally for offline use.
func openFilestoreReader(
	ctx context.Context,
	loaded *config.Loaded,
) (filestoreReader, error) {
	endpoint := rpcEndpoint(loaded.Resolved.RPC.Filestore)
	client, err := rpc.NewFilestoreClient(ctx, endpoint)
	if err == nil {
		_, _, probeErr := client.LookupSourceObject(ctx, contracts.SourceObjectRef{
			SourceKind: "cli", SourceName: "readiness", ExternalID: "readiness",
		})
		if probeErr == nil || status.Code(probeErr) != codes.Unavailable {
			return client, nil
		}
		_ = client.Close()
	}

	root, err := resolvedFilestoreRoot(loaded)
	if err != nil {
		return nil, err
	}

	return filestore.NewFilesystemStore(root), nil
}

func newFilestoreCommand(out io.Writer, configPath *string) *cobra.Command {
	command := &cobra.Command{
		Use:   "filestore",
		Short: "Inspect and verify FILESTORE data",
	}
	command.AddCommand(newFilestoreVerifyCommand(out, configPath))
	command.AddCommand(newFilestoreCleanupLocksCommand(out, configPath))
	command.AddCommand(newFilestoreCompactCommand(out, configPath))
	command.AddCommand(newFilestoreExportRecoveryCommand(out, configPath))
	command.AddCommand(newFilestoreStorageCommand(out, configPath))
	command.AddCommand(newFilestorePathCommand(out, configPath))
	command.AddCommand(newFilestoreDeleteCommand(out, configPath))
	command.AddCommand(newFilestoreGcCommand(out, configPath))
	command.AddCommand(newFilestoreRepackCommand(out, configPath))
	command.AddCommand(newFilestoreTrainDictionaryCommand(out, configPath))
	command.AddCommand(newFilestoreServeCommand(out, configPath, "serve"))
	command.AddCommand(newFilestoreBackupCommand(out, configPath))
	command.AddCommand(newFilestoreRestoreCommand(out, configPath))

	return command
}

func newFilestoreServeCommand(
	out io.Writer,
	configPath *string,
	use string,
) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: "Run the FILESTORE gRPC service",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}

			store := filestore.NewFilesystemStore(root)
			defer func() { _ = store.Close() }()
			schedulerClient, err := rpc.NewSchedulerClient(
				command.Context(),
				rpcEndpoint(loaded.Resolved.RPC.Scheduler),
			)
			if err != nil {
				return err
			}
			defer schedulerClient.Close()

			endpoint := rpcEndpoint(loaded.Resolved.RPC.Filestore)
			if _, err := fmt.Fprintf(
				out,
				"filestore serve: %s %s\n",
				endpoint.Network,
				endpoint.Address,
			); err != nil {
				return err
			}

			filestoreServer := rpc.NewFilestoreServer(
				store,
				rpc.WithObjectChangeNotifier(schedulerClient),
			)
			// Flush the change-notification batcher after the gRPC server has
			// gracefully stopped (no handler is still enqueuing) so analysis
			// notifications buffered during shutdown are not lost.
			defer filestoreServer.Stop()

			return rpc.Serve(command.Context(), endpoint, func(server *grpc.Server) {
				pb.RegisterFilestoreServiceServer(server, filestoreServer)
			})
		},
	}
}

func newFilestorePathCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		jsonOutput   bool
		recordsLimit int
	)

	command := &cobra.Command{
		Use:   "path <path>",
		Short: "Resolve a FILESTORE path back to the data it belongs to",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			report, err := store.ResolvePath(
				command.Context(),
				filestore.PathResolveRequest{
					Path:         args[0],
					RecordsLimit: recordsLimit,
				},
			)
			if err != nil {
				return err
			}
			if jsonOutput {
				encoded, err := json.MarshalIndent(report, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(out, string(encoded))

				return err
			}

			return writePathResolveHuman(out, report)
		},
	}
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit structured JSON")
	command.Flags().
		IntVar(&recordsLimit, "records-limit", 20, "maximum decoded shard records to include")

	return command
}

func writePathResolveHuman(out io.Writer, report filestore.PathResolveReport) error {
	estimate := ""
	if report.Estimated {
		estimate = " estimated=true"
	}
	if _, err := fmt.Fprintf(
		out,
		"filestore path: kind=%s role=%s path=%s logical_bytes=%d allocated_bytes=%d%s\n",
		report.Kind,
		report.Role,
		report.Path,
		report.LogicalBytes,
		report.AllocatedBytes,
		estimate,
	); err != nil {
		return err
	}
	if report.ObjectDigest != "" {
		if _, err := fmt.Fprintf(out, "object: %s\n", report.ObjectDigest); err != nil {
			return err
		}
	}
	if report.Manifest != nil {
		if _, err := fmt.Fprintf(
			out,
			"manifest: object_id=%s media_type=%s facets=%s compound=%t parts=%d\n",
			report.Manifest.ObjectID,
			report.Manifest.MediaType,
			strings.Join(report.Manifest.Facets, ","),
			report.Manifest.Compound,
			len(report.Manifest.Parts),
		); err != nil {
			return err
		}
	}
	if report.SourceObject != nil {
		if _, err := fmt.Fprintf(
			out,
			"source: kind=%s name=%s external_id=%s external_version=%s\n",
			report.SourceObject.SourceKind,
			report.SourceObject.SourceName,
			report.SourceObject.ExternalID,
			report.SourceObject.ExternalVersion,
		); err != nil {
			return err
		}
	}
	if report.SourceCursor != nil {
		if _, err := fmt.Fprintf(
			out,
			"cursor: kind=%s name=%s updated_at=%s\n",
			report.SourceCursor.SourceKind,
			report.SourceCursor.SourceName,
			report.SourceCursor.UpdatedAt,
		); err != nil {
			return err
		}
	}
	if report.IngestClaim != nil {
		if _, err := fmt.Fprintf(
			out,
			"claim: id=%s acquired_at=%s\n",
			report.IngestClaim.ClaimID,
			report.IngestClaim.AcquiredAt,
		); err != nil {
			return err
		}
	}
	if report.RecordCount > 0 {
		if _, err := fmt.Fprintf(
			out,
			"records: total=%d shown=%d truncated=%t limit=%d\n",
			report.RecordCount,
			len(report.Records),
			report.Truncated,
			report.RecordsLimit,
		); err != nil {
			return err
		}
		table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(table, "TYPE\tDIGEST\tSOURCE\tDETAIL"); err != nil {
			return err
		}
		for _, record := range report.Records {
			if _, err := fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\n",
				pathRecordType(record),
				firstNonEmptyString(string(record.ObjectDigest), string(record.ChildDigest)),
				pathRecordSource(record),
				pathRecordDetail(record),
			); err != nil {
				return err
			}
		}
		return table.Flush()
	}

	return nil
}

func pathRecordType(record filestore.PathResolveRecord) string {
	switch {
	case record.SourceObject != nil:
		return "source"
	case record.Recovery != nil:
		return "recovery"
	case record.ChildDigest != "":
		return "parent"
	default:
		return "record"
	}
}

func pathRecordSource(record filestore.PathResolveRecord) string {
	if record.SourceObject == nil {
		return ""
	}

	return record.SourceObject.SourceKind + "/" +
		record.SourceObject.SourceName + ":" +
		record.SourceObject.ExternalID
}

func pathRecordDetail(record filestore.PathResolveRecord) string {
	if record.Recovery != nil {
		return record.Recovery.ObjectID
	}
	if len(record.Parents) > 0 {
		return fmt.Sprintf("parents=%d", len(record.Parents))
	}
	if record.UpdatedAt != "" {
		return "updated_at=" + record.UpdatedAt
	}

	return ""
}

func newFilestoreStorageCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		digestValue      string
		messageID        string
		sourceKind       string
		sourceName       string
		externalID       string
		externalVersion  string
		noRecursiveParts bool
		jsonOutput       bool
	)

	command := &cobra.Command{
		Use:   "storage",
		Short: "Break down FILESTORE storage used by one object",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			digest, err := resolveFilestoreStorageTarget(
				command.Context(),
				loaded,
				store,
				storageTargetOptions{
					Digest:          digestValue,
					MessageID:       messageID,
					SourceKind:      sourceKind,
					SourceName:      sourceName,
					ExternalID:      externalID,
					ExternalVersion: externalVersion,
					JSONOutput:      jsonOutput,
					Out:             out,
				},
			)
			if err != nil {
				return err
			}

			report, err := store.StorageBreakdown(
				command.Context(),
				filestore.StorageBreakdownRequest{
					Digest:         digest,
					RecursiveParts: !noRecursiveParts,
				},
			)
			if err != nil {
				return err
			}
			if jsonOutput {
				encoded, err := json.MarshalIndent(report, "", "  ")
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(out, string(encoded))

				return err
			}

			return writeStorageBreakdownHuman(out, report)
		},
	}
	command.Flags().StringVar(&digestValue, "digest", "", "object digest to inspect")
	command.Flags().
		StringVar(&messageID, "message-id", "", "RFC Message-ID to resolve through QUERY")
	command.Flags().StringVar(&sourceKind, "source-kind", "", "source object kind")
	command.Flags().StringVar(&sourceName, "source-name", "", "source object name")
	command.Flags().StringVar(&externalID, "external-id", "", "source object external id")
	command.Flags().
		StringVar(&externalVersion, "external-version", "", "source object external version")
	command.Flags().
		BoolVar(&noRecursiveParts, "no-recursive-parts", false, "exclude compound part objects")
	command.Flags().BoolVar(&jsonOutput, "json", false, "emit structured JSON")

	return command
}

type storageTargetOptions struct {
	Out             io.Writer
	Digest          string
	MessageID       string
	SourceKind      string
	SourceName      string
	ExternalID      string
	ExternalVersion string
	JSONOutput      bool
}

func resolveFilestoreStorageTarget(
	ctx context.Context,
	loaded *config.Loaded,
	store filestoreReader,
	options storageTargetOptions,
) (contracts.ObjectDigest, error) {
	digestSet := strings.TrimSpace(options.Digest) != ""
	messageIDSet := strings.TrimSpace(options.MessageID) != ""
	sourceSet := strings.TrimSpace(options.SourceKind) != "" ||
		strings.TrimSpace(options.SourceName) != "" ||
		strings.TrimSpace(options.ExternalID) != "" ||
		strings.TrimSpace(options.ExternalVersion) != ""
	modeCount := 0
	for _, set := range []bool{digestSet, messageIDSet, sourceSet} {
		if set {
			modeCount++
		}
	}
	if modeCount != 1 {
		return "", errors.New(
			"exactly one target mode is required: --digest, --message-id, or --source-kind/--source-name/--external-id",
		)
	}
	if digestSet {
		digest := contracts.ObjectDigest(strings.TrimSpace(options.Digest))
		if err := validateCLIDigest(digest); err != nil {
			return "", err
		}

		return digest, nil
	}
	if sourceSet {
		ref := contracts.SourceObjectRef{
			SourceKind:      strings.TrimSpace(options.SourceKind),
			SourceName:      strings.TrimSpace(options.SourceName),
			ExternalID:      strings.TrimSpace(options.ExternalID),
			ExternalVersion: strings.TrimSpace(options.ExternalVersion),
		}
		if ref.SourceKind == "" || ref.SourceName == "" || ref.ExternalID == "" {
			return "", errors.New(
				"--source-kind, --source-name, and --external-id are required for source ref resolution",
			)
		}
		digest, found, err := store.LookupSourceObject(ctx, ref)
		if err != nil {
			return "", err
		}
		if !found {
			return "", fmt.Errorf(
				"source object was not found: %s/%s external_id=%q external_version=%q",
				ref.SourceKind,
				ref.SourceName,
				ref.ExternalID,
				ref.ExternalVersion,
			)
		}

		return digest, nil
	}

	return resolveStorageMessageID(ctx, loaded, options)
}

func resolveStorageMessageID(
	ctx context.Context,
	loaded *config.Loaded,
	options storageTargetOptions,
) (contracts.ObjectDigest, error) {
	queryClient, err := rpc.NewQueryClient(ctx, rpcEndpoint(loaded.Resolved.RPC.Query))
	if err != nil {
		return "", err
	}
	defer queryClient.Close()

	messageID := strings.TrimSpace(options.MessageID)
	response, err := queryClient.ResolveMailIdentity(
		ctx,
		contracts.MailIdentityResolveRequest{
			MessageID: messageID,
			Limit:     50,
		},
	)
	if err != nil {
		return "", err
	}
	candidates := response.Digests
	if len(candidates) == 0 {
		return "", fmt.Errorf("message_id %q not found", messageID)
	}
	if len(candidates) > 1 {
		if options.JSONOutput {
			encoded, err := json.MarshalIndent(map[string]any{
				"message_id":  messageID,
				"candidates":  candidates,
				"ambiguous":   true,
				"target_mode": "message_id",
			}, "", "  ")
			if err != nil {
				return "", err
			}
			if _, err := fmt.Fprintln(options.Out, string(encoded)); err != nil {
				return "", err
			}
		}

		return "", fmt.Errorf(
			"message_id %q matched %d object digests",
			messageID,
			len(candidates),
		)
	}

	return candidates[0], nil
}

func writeStorageBreakdownHuman(
	out io.Writer,
	report filestore.StorageBreakdownReport,
) error {
	estimate := ""
	if report.EstimatedAllocated {
		estimate = " estimated=true"
	}
	if _, err := fmt.Fprintf(
		out,
		"filestore storage: digest=%s allocated_bytes=%d logical_bytes=%d files=%d objects=%d recursive_parts=%t%s\n",
		report.RootDigest,
		report.TotalAllocatedBytes,
		report.TotalLogicalBytes,
		report.FileCount,
		report.ReferencedObjectCount,
		report.RecursiveParts,
		estimate,
	); err != nil {
		return err
	}
	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		table,
		"ROLE\tOBJECT\tLOGICAL\tALLOCATED\tCHUNKS\tDICT\tFAMILY\tPATH",
	); err != nil {
		return err
	}
	for _, file := range report.Files {
		role := file.Role
		if file.RecursivePart {
			role = "part:" + firstNonEmptyString(file.CompoundRole, role)
		}
		dictID := storageDictionaryLabel(file)
		if _, err := fmt.Fprintf(
			table,
			"%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			role,
			file.ObjectDigest,
			file.LogicalBytes,
			file.AllocatedBytes,
			storageChunkCountLabel(file),
			dictID,
			storageDictionaryFamilyLabel(file),
			file.Path,
		); err != nil {
			return err
		}
	}

	return table.Flush()
}

func storageChunkCountLabel(file filestore.StorageBreakdownFile) string {
	if file.ChunkCount <= 0 {
		return "-"
	}

	return strconv.Itoa(file.ChunkCount)
}

func storageDictionaryLabel(file filestore.StorageBreakdownFile) string {
	if file.DictID != "" {
		return file.DictID
	}
	if len(file.DictIDs) > 0 {
		return strings.Join(file.DictIDs, ",")
	}

	return "-"
}

func storageDictionaryFamilyLabel(file filestore.StorageBreakdownFile) string {
	if file.DictFamily != "" {
		return file.DictFamily
	}
	if len(file.DictFamilies) > 0 {
		return strings.Join(file.DictFamilies, ",")
	}

	return "-"
}

func validateCLIDigest(digest contracts.ObjectDigest) error {
	value := string(digest)
	if len(value) != 64 {
		return fmt.Errorf("invalid object digest %q", digest)
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return fmt.Errorf("invalid object digest %q", digest)
		}
	}

	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func newFilestoreDeleteCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		digestValue     string
		confirmInstance string
	)

	command := &cobra.Command{
		Use:   "delete",
		Short: "Delete a FILESTORE object's metadata (chunks reclaimed by gc)",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"filestore delete",
				confirmInstance,
			); err != nil {
				return err
			}
			digest := contracts.ObjectDigest(strings.TrimSpace(digestValue))
			if err := validateCLIDigest(digest); err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			if err := store.DeleteObject(command.Context(), digest); err != nil {
				return err
			}
			_, err = fmt.Fprintf(out, "filestore delete: digest=%s deleted\n", digest)

			return err
		},
	}
	command.Flags().StringVar(&digestValue, "digest", "", "object digest to delete")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreGcCommand(out io.Writer, configPath *string) *cobra.Command {
	var confirmInstance string

	command := &cobra.Command{
		Use:   "gc",
		Short: "Reclaim chunk-index entries no longer referenced by any object",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"filestore gc",
				confirmInstance,
			); err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			report, err := store.Gc(command.Context())
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(
				out,
				"filestore gc: scanned_chunks=%d swept_chunks=%d retained_chunks=%d swept_recipes=%d\n",
				report.ScannedChunks,
				report.SweptChunks,
				report.RetainedChunks,
				report.SweptRecipes,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreRepackCommand(out io.Writer, configPath *string) *cobra.Command {
	var confirmInstance string

	command := &cobra.Command{
		Use:   "repack",
		Short: "Rewrite sealed packs to reclaim bytes left by gc",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"filestore repack",
				confirmInstance,
			); err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			report, err := store.Repack(command.Context())
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(
				out,
				"filestore repack: packs_scanned=%d packs_repacked=%d packs_removed=%d chunks_moved=%d bytes_before=%d bytes_after=%d\n",
				report.PacksScanned,
				report.PacksRepacked,
				report.PacksRemoved,
				report.ChunksMoved,
				report.BytesBefore,
				report.BytesAfter,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreTrainDictionaryCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var (
		confirmInstance string
		sampleLimit     int
		family          string
		allFamilies     bool
	)

	command := &cobra.Command{
		Use:   "train-dictionary",
		Short: "Train and install a zstd dictionary from a sample of small objects",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"filestore train-dictionary",
				confirmInstance,
			); err != nil {
				return err
			}
			store, err := openFilestoreReader(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer func() { _ = store.Close() }()
			report, err := store.TrainDictionary(
				command.Context(),
				filestore.TrainDictionaryRequest{
					Family:      family,
					AllFamilies: allFamilies,
					SampleLimit: sampleLimit,
				},
			)
			if err != nil {
				return err
			}
			for _, result := range report.Results {
				if _, err := fmt.Fprintf(
					out,
					"filestore train-dictionary: family=%s dictionary_id=%s dictionary_bytes=%d samples=%d\n",
					result.Family,
					result.DictionaryID,
					result.DictionaryBytes,
					result.Samples,
				); err != nil {
					return err
				}
			}

			return nil
		},
	}
	command.Flags().
		IntVar(&sampleLimit, "sample-limit", 0, "maximum objects to sample (0 = default)")
	command.Flags().
		StringVar(&family, "family", "", "dictionary family to train")
	command.Flags().
		BoolVar(&allFamilies, "all-families", false, "train every eligible dictionary family")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreCleanupLocksCommand(out io.Writer, configPath *string) *cobra.Command {
	var confirmInstance string

	command := &cobra.Command{
		Use:   "cleanup-locks",
		Short: "Remove stale FILESTORE source lock files and empty lock shard directories",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if err := requireInstanceConfirmation(
				loaded,
				"filestore cleanup-locks",
				confirmInstance,
			); err != nil {
				return err
			}
			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}
			report, err := filestore.NewFilesystemStore(root).
				CleanupSourceLocks(command.Context())
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(
				out,
				"filestore cleanup-locks: removed_files=%d removed_dirs=%d scanned_dirs=%d\n",
				report.RemovedFiles,
				report.RemovedDirs,
				report.ScannedDirs,
			)

			return err
		},
	}
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreCompactCommand(out io.Writer, configPath *string) *cobra.Command {
	var (
		confirmInstance string
		dryRun          bool
	)

	command := &cobra.Command{
		Use:   "compact",
		Short: "Migrate v1 indexes to packed v2 shards and deduplicate v2 shards",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			if !dryRun {
				if err := requireInstanceConfirmation(
					loaded,
					"filestore compact",
					confirmInstance,
				); err != nil {
					return err
				}
			}
			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}
			report, err := filestore.NewFilesystemStore(root).
				Compact(command.Context(), dryRun)
			if err != nil {
				return err
			}
			encoded, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(out, string(encoded))

			return err
		},
	}
	command.Flags().
		BoolVar(&dryRun, "dry-run", false, "count compactable v1 files without writing or removing data")
	command.Flags().
		StringVar(&confirmInstance, "confirm-instance", "", "required production-like instance id confirmation")

	return command
}

func newFilestoreExportRecoveryCommand(
	out io.Writer,
	configPath *string,
) *cobra.Command {
	var digest string

	command := &cobra.Command{
		Use:   "export-recovery",
		Short: "Export one FILESTORE recovery sidecar from packed or legacy recovery data",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}
			content, err := filestore.NewFilesystemStore(root).
				ExportRecoveryJSON(command.Context(), contracts.ObjectDigest(digest))
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(out, string(content))

			return err
		},
	}
	command.Flags().
		StringVar(&digest, "digest", "", "object digest to export recovery data for")
	_ = command.MarkFlagRequired("digest")

	return command
}

func newFilestoreVerifyCommand(out io.Writer, configPath *string) *cobra.Command {
	var repair bool

	command := &cobra.Command{
		Use:   "verify",
		Short: "Verify FILESTORE objects, content chunks, and metadata",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}

			// Read-only verification can run against the live gRPC service;
			// --repair mutates the store, so it requires exclusive local access
			// (run it with filestore-serve stopped).
			var report filestore.VerifyReport
			if repair {
				root, rootErr := resolvedFilestoreRoot(loaded)
				if rootErr != nil {
					return rootErr
				}
				store := filestore.NewFilesystemStore(root)
				defer func() { _ = store.Close() }()
				report, err = store.Verify(command.Context(), filestore.VerifyRequest{Repair: true})
			} else {
				store, openErr := openFilestoreReader(command.Context(), loaded)
				if openErr != nil {
					return openErr
				}
				defer func() { _ = store.Close() }()
				report, err = store.Verify(command.Context(), filestore.VerifyRequest{})
			}
			if err != nil {
				return err
			}

			if _, err := fmt.Fprintf(
				out,
				"filestore verify: status=%s checked=%d findings=%d repaired=%d\n",
				report.Status,
				report.Checked,
				len(report.Findings),
				report.Repaired,
			); err != nil {
				return err
			}

			for _, finding := range report.Findings {
				if _, err := fmt.Fprintf(
					out,
					"%s %s %s %s\n",
					finding.Code,
					finding.Digest,
					finding.Path,
					finding.Message,
				); err != nil {
					return err
				}
			}

			if report.Status == filestore.VerifyStatusError && !repair {
				return errors.New("filestore verification failed")
			}

			return nil
		},
	}
	command.Flags().
		BoolVar(&repair, "repair", false, "attempt to repair torn tails and stale staging directories")

	return command
}

func newFilestoreBackupCommand(out io.Writer, configPath *string) *cobra.Command {
	var destPath string

	command := &cobra.Command{
		Use:   "backup",
		Short: "Back up FILESTORE to a destination directory using rsync",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}
			if destPath == "" {
				return errors.New("--to is required")
			}

			src := strings.TrimRight(root, "/") + "/"
			dst := strings.TrimRight(destPath, "/") + "/"

			args := []string{
				"-aH", "--delete",
				"--exclude=staging/",
				"--exclude=*.next",
				src, dst,
			}
			if _, err := fmt.Fprintf(
				out,
				"filestore backup: rsync %s\n",
				strings.Join(args, " "),
			); err != nil {
				return err
			}

			rsync := exec.CommandContext(command.Context(), "rsync", args...)
			rsync.Stdout = out
			rsync.Stderr = out
			if err := rsync.Run(); err != nil {
				return fmt.Errorf("rsync failed: %w", err)
			}

			snapshot := map[string]any{
				"taken_at":    time.Now().UTC().Format(time.RFC3339),
				"source_root": root,
				"tool":        "rsync",
			}
			encoded, err := json.MarshalIndent(snapshot, "", "  ")
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(out, "filestore backup: complete\n%s\n", encoded)
			return err
		},
	}
	command.Flags().StringVar(&destPath, "to", "", "destination directory for the backup")
	_ = command.MarkFlagRequired("to")

	return command
}

func newFilestoreRestoreCommand(out io.Writer, configPath *string) *cobra.Command {
	var fromPath string

	command := &cobra.Command{
		Use:   "restore",
		Short: "Restore FILESTORE from a backup directory and run verify --repair",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			root, err := resolvedFilestoreRoot(loaded)
			if err != nil {
				return err
			}
			if fromPath == "" {
				return errors.New("--from is required")
			}

			src := strings.TrimRight(fromPath, "/") + "/"
			dst := strings.TrimRight(root, "/") + "/"

			args := []string{"-aH", "--delete", src, dst}
			if _, err := fmt.Fprintf(
				out,
				"filestore restore: rsync %s\n",
				strings.Join(args, " "),
			); err != nil {
				return err
			}

			rsync := exec.CommandContext(command.Context(), "rsync", args...)
			rsync.Stdout = out
			rsync.Stderr = out
			if err := rsync.Run(); err != nil {
				return fmt.Errorf("rsync failed: %w", err)
			}

			if _, err := fmt.Fprintln(
				out,
				"filestore restore: running verify --repair",
			); err != nil {
				return err
			}

			store := filestore.NewFilesystemStore(root)
			report, err := store.Verify(command.Context(), filestore.VerifyRequest{Repair: true})
			if err != nil {
				return err
			}

			if _, err := fmt.Fprintf(
				out,
				"filestore restore: verify status=%s checked=%d findings=%d repaired=%d\n",
				report.Status,
				report.Checked,
				len(report.Findings),
				report.Repaired,
			); err != nil {
				return err
			}

			for _, finding := range report.Findings {
				if _, err := fmt.Fprintf(
					out,
					"%s %s %s %s\n",
					finding.Code,
					finding.Digest,
					finding.Path,
					finding.Message,
				); err != nil {
					return err
				}
			}

			return nil
		},
	}
	command.Flags().
		StringVar(&fromPath, "from", "", "source backup directory to restore from")
	_ = command.MarkFlagRequired("from")

	return command
}

func resolvedFilestoreRoot(loaded *config.Loaded) (string, error) {
	root := loaded.Config.Filestore.Root
	if filepath.IsAbs(root) {
		return root, nil
	}

	if loaded.Path == "" {
		return "", errors.New("relative filestore.root requires a loaded config path")
	}

	return filepath.Join(filepath.Dir(loaded.Path), root), nil
}
