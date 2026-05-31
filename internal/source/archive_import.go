// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/mailmessage"
	"blackcat.ca/gmeow/internal/rpc"
)

const (
	ArchiveImportFormatAuto    = "auto"
	ArchiveImportFormatMaildir = "maildir"
	ArchiveImportFormatMbox    = "mbox"
	ArchiveImportFormatNNML    = "nnml"
	ArchiveImportFormatMH      = "mh"
	ArchiveImportFormatEMLDir  = "eml-dir"
)

var numberedMailFilePattern = regexp.MustCompile(`^[0-9]+$`)

type ArchiveImportRequest struct {
	SourceName      string
	Format          string
	Roots           []string
	RunID           string
	StateDir        string
	DryRun          bool
	LowNoise        bool
	Resume          bool
	QueueHighWater  int
	Publisher       ArchiveImportPublisher
	Status          ArchiveImportQueueStatusFunc
	CapacityDrainer func(context.Context) error
	// Progress, when set, is called periodically (every ProgressInterval, default
	// 2s) and once at completion with a running snapshot of import counts so a
	// caller can render live progress on a long import.
	Progress         func(ArchiveImportProgress)
	ProgressInterval time.Duration
}

// ArchiveImportProgress is a point-in-time snapshot of an in-flight import.
type ArchiveImportProgress struct {
	Scanned           int64
	Parsed            int64
	Ingested          int64
	Failures          int64
	Elapsed           time.Duration
	MessagesPerSecond float64
	LastPath          string
}

// importProgress accumulates live counters shared by the directory walk and the
// ingest workers. Counters are atomic so progress can be sampled concurrently;
// lastPath is guarded separately. It is always allocated (counters are cheap);
// only the periodic emit is gated on a configured callback.
type importProgress struct {
	scanned  atomic.Int64
	parsed   atomic.Int64
	ingested atomic.Int64
	failures atomic.Int64
	start    time.Time
	mu       sync.Mutex
	lastPath string
}

func (progress *importProgress) setLastPath(path string) {
	progress.mu.Lock()
	progress.lastPath = path
	progress.mu.Unlock()
}

func (progress *importProgress) snapshot() ArchiveImportProgress {
	elapsed := time.Since(progress.start)
	ingested := progress.ingested.Load()
	rate := 0.0
	if seconds := elapsed.Seconds(); seconds > 0 {
		rate = float64(ingested) / seconds
	}
	progress.mu.Lock()
	lastPath := progress.lastPath
	progress.mu.Unlock()

	return ArchiveImportProgress{
		Scanned:           progress.scanned.Load(),
		Parsed:            progress.parsed.Load(),
		Ingested:          ingested,
		Failures:          progress.failures.Load(),
		Elapsed:           elapsed,
		MessagesPerSecond: rate,
		LastPath:          lastPath,
	}
}

type ArchiveImportReport struct {
	SourceName          string   `json:"source_name"`
	RunID               string   `json:"run_id,omitempty"`
	Scanned             int      `json:"scanned"`
	Parsed              int      `json:"parsed"`
	Enqueued            int      `json:"enqueued"`
	Processed           int      `json:"processed"`
	Imported            int      `json:"imported"`
	ExactDuplicates     int      `json:"exact_duplicates"`
	MessageIDDuplicates int      `json:"message_id_duplicates"`
	GeneratedMessageIDs int      `json:"generated_message_ids"`
	LowNoiseSkipped     int      `json:"low_noise_skipped"`
	TrivialSkipped      int      `json:"trivial_skipped"`
	MinorVersions       int      `json:"minor_versions"`
	MajorVersions       int      `json:"major_versions"`
	Promoted            int      `json:"promoted"`
	Collisions          int      `json:"collisions"`
	ParseFailures       int      `json:"parse_failures"`
	Skipped             int      `json:"skipped"`
	SkippedMessageIDs   []string `json:"skipped_message_ids,omitempty"`
	Failures            []string `json:"failures,omitempty"`
}

type ArchiveImportPublisher interface {
	PublishSourceImportJob(context.Context, contracts.SourceImportJob) error
	ProcessSourceImportFailures(context.Context, int) (int, error)
	SourceImportStatus(context.Context) (contracts.SourceImportQueueStatus, error)
}

type ArchiveImportQueueStatusFunc func(context.Context) (contracts.SourceImportQueueStatus, error)

// defaultIngestConcurrency bounds the total number of concurrent object Puts a
// single import drives — across both messages (the direct importRoot fans
// messages out to a worker pool) and the parts within a message (headers, body,
// metadata, mime-structure, attachments). One shared semaphore caps the total so
// the two layers compose without an N×M goroutine blow-up.
const defaultIngestConcurrency = 8

// messageIDLockShards stripes the per-Message-ID lock that serializes ingestion
// of messages sharing a Message-ID, so concurrent workers cannot both pass the
// "canonical not found" check and double-ingest a duplicate as a new canonical.
const messageIDLockShards = 1024

type ArchiveImporter struct {
	service     *Service
	store       FilestoreClient
	concurrency int
	// ingestSem caps concurrent object Puts; nil means unbounded (never, since the
	// constructor seeds the default). messageLocks serializes same-Message-ID work.
	ingestSem    chan struct{}
	messageLocks [messageIDLockShards]sync.Mutex
}

// SetConcurrency sets how many object Puts the import runs in parallel (across
// messages and their parts). A value <= 0 restores the default. It is not safe
// to call concurrently with Import.
func (importer *ArchiveImporter) SetConcurrency(workers int) {
	if workers <= 0 {
		workers = defaultIngestConcurrency
	}
	importer.concurrency = workers
	importer.ingestSem = make(chan struct{}, workers)
}

// Concurrency reports the effective ingest concurrency after clamping, so
// callers can size related resources (e.g. queue prefetch) to the same value.
func (importer *ArchiveImporter) Concurrency() int {
	return importer.concurrency
}

// lockMessageID serializes ingestion keyed by Message-ID (sharded). Distinct ids
// (including distinct generated ids) hash to independent stripes and run freely;
// identical ids serialize. A hash collision only over-serializes, never corrupts.
func (importer *ArchiveImporter) lockMessageID(messageID string) func() {
	shard := &importer.messageLocks[messageIDShard(messageID)]
	shard.Lock()

	return shard.Unlock
}

func messageIDShard(messageID string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(messageID))

	return hash.Sum32() % messageIDLockShards
}

type archiveMessage struct {
	mailmessage.Message

	ObservedAt      time.Time
	SourcePath      string
	Mailbox         string
	Format          string
	ExternalID      string
	ExternalVersion string
}

func NewArchiveImporter(store FilestoreClient) (*ArchiveImporter, error) {
	// Stamp every object an import creates with medium (repair) analysis priority,
	// so a bulk import's analysis preempts low-priority backfill but yields to
	// high-priority inbox sync and search. The decorator covers all import writes
	// — parts, version records, and the compound message — without threading the
	// class through each call site.
	prioritized := archivePriorityClient{
		FilestoreClient: store,
		priorityClass:   contracts.PriorityRepair,
	}

	service, err := NewService(prioritized)
	if err != nil {
		return nil, err
	}

	return &ArchiveImporter{
		service:     service,
		store:       prioritized,
		concurrency: defaultIngestConcurrency,
		ingestSem:   make(chan struct{}, defaultIngestConcurrency),
	}, nil
}

// archivePriorityClient wraps a FilestoreClient to default the analysis priority
// class on every write, tagging all objects a bulk producer creates.
type archivePriorityClient struct {
	FilestoreClient
	priorityClass string
}

func (client archivePriorityClient) Put(
	ctx context.Context,
	request rpc.PutRequest,
) (contracts.ObjectDigest, error) {
	if request.PriorityClass == "" {
		request.PriorityClass = client.priorityClass
	}

	return client.FilestoreClient.Put(ctx, request)
}

func (client archivePriorityClient) PutCompound(
	ctx context.Context,
	request rpc.CompoundPutRequest,
) (contracts.ObjectDigest, error) {
	if request.PriorityClass == "" {
		request.PriorityClass = client.priorityClass
	}

	return client.FilestoreClient.PutCompound(ctx, request)
}

func (client archivePriorityClient) AttachProvenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	return client.FilestoreClient.AttachProvenanceWithPriority(
		ctx,
		digest,
		provenance,
		client.priorityClass,
	)
}

func (client archivePriorityClient) AttachProvenanceWithPriority(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
	priorityClass string,
) error {
	if priorityClass == "" {
		priorityClass = client.priorityClass
	}

	return client.FilestoreClient.AttachProvenanceWithPriority(
		ctx,
		digest,
		provenance,
		priorityClass,
	)
}

func (importer *ArchiveImporter) Import(
	ctx context.Context,
	request ArchiveImportRequest,
) (ArchiveImportReport, error) {
	sourceName := strings.TrimSpace(request.SourceName)
	if sourceName == "" {
		if len(request.Roots) == 0 {
			return ArchiveImportReport{}, errors.New("archive import root is required")
		}
		sourceName = sourceNameFromRoot(request.Roots[0])
	}

	runID := archiveImportRunID(request, sourceName)
	report := ArchiveImportReport{SourceName: sourceName, RunID: runID}

	// Record the run so it is listable and deletable later. Dry runs do not write
	// anything, so they are not registered. Registry writes are best-effort: a
	// failure to record must not fail the import itself.
	if !request.DryRun {
		record := ImportRunRecord{
			RunID:      runID,
			SourceName: sourceName,
			SourceKind: contracts.MailArchiveSourceKind,
			Roots:      append([]string{}, request.Roots...),
			Format:     request.Format,
			Status:     ImportRunStatusRunning,
			StartedAt:  time.Now().UTC(),
			LowNoise:   request.LowNoise,
		}
		_ = WriteImportRunRecord(request.StateDir, record)
		defer func() {
			record.Status = ImportRunStatusCompleted
			if len(report.Failures) > 0 {
				record.Status = ImportRunStatusFailed
			}
			record.FinishedAt = time.Now().UTC()
			record.Scanned = report.Scanned
			record.Parsed = report.Parsed
			record.Imported = report.Imported
			record.Duplicates = report.ExactDuplicates + report.MessageIDDuplicates
			record.Failures = len(report.Failures)
			_ = WriteImportRunRecord(request.StateDir, record)
		}()
	}

	progress := &importProgress{start: time.Now()}
	stopProgress := importer.startProgress(ctx, request, progress)
	defer stopProgress()

	for _, root := range request.Roots {
		if err := importer.importRoot(
			ctx,
			request,
			sourceName,
			root,
			&report,
			progress,
		); err != nil {
			report.Failures = append(report.Failures, err.Error())
		}
	}

	if len(report.Failures) > 0 {
		return report, fmt.Errorf(
			"archive import completed with %d root failure(s)",
			len(report.Failures),
		)
	}

	return report, nil
}

// startProgress launches the periodic progress emitter (if a callback is set)
// and returns a stop function that halts the ticker, waits for the emitter to
// exit, and delivers one final snapshot. The stop function is safe to defer.
func (importer *ArchiveImporter) startProgress(
	ctx context.Context,
	request ArchiveImportRequest,
	progress *importProgress,
) func() {
	if request.Progress == nil {
		return func() {}
	}

	interval := request.ProgressInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		defer close(finished)
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				request.Progress(progress.snapshot())
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(done)
		<-finished
		request.Progress(progress.snapshot())
	}
}

func (importer *ArchiveImporter) importRoot(
	ctx context.Context,
	request ArchiveImportRequest,
	sourceName string,
	root string,
	report *ArchiveImportReport,
	progress *importProgress,
) error {
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		message, parseErr := parseArchiveFile(root, root, request.Format, 0)
		if parseErr != nil {
			report.ParseFailures++
			return parseErr
		}
		report.Scanned++
		report.Parsed++
		progress.scanned.Add(1)
		progress.parsed.Add(1)
		progress.setLastPath(root)
		ingestErr := importer.ingestArchiveMessage(ctx, sourceName, message, request, report)
		if ingestErr != nil {
			progress.failures.Add(1)
		} else {
			progress.ingested.Add(1)
		}

		return ingestErr
	}

	// The directory walk is the single producer: it owns the parse-side counters
	// on the shared report and feeds parsed messages to a pool of ingest workers,
	// each of which accumulates ingest-side counters into its own report. The
	// per-Message-ID lock inside ingestArchiveMessage keeps same-id dedup correct;
	// the worker reports are merged once the walk and all workers have finished.
	workers := importer.concurrency
	if workers < 1 {
		workers = 1
	}
	messages := make(chan archiveMessage, workers)
	locals := make([]ArchiveImportReport, workers)

	var workerGroup sync.WaitGroup
	for index := range workers {
		workerGroup.Add(1)
		go func(local *ArchiveImportReport) {
			defer workerGroup.Done()
			for message := range messages {
				if err := importer.ingestArchiveMessage(
					ctx, sourceName, message, request, local,
				); err != nil {
					local.Failures = append(local.Failures, err.Error())
					progress.failures.Add(1)
				} else {
					progress.ingested.Add(1)
				}
			}
		}(&locals[index])
	}

	walkErr := filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				report.Failures = append(report.Failures, walkErr.Error())
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() {
				if shouldSkipArchiveDir(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if shouldSkipArchiveFile(entry.Name()) {
				report.Skipped++
				return nil
			}
			format := detectArchiveFileFormat(path, root, request.Format)
			if format == "" {
				report.Skipped++
				return nil
			}
			report.Scanned++
			progress.scanned.Add(1)
			progress.setLastPath(path)
			if format == ArchiveImportFormatMbox {
				parseErr := forEachMboxMessage(path, root, func(message archiveMessage) error {
					report.Parsed++
					progress.parsed.Add(1)
					messages <- message

					return nil
				})
				if parseErr != nil {
					report.ParseFailures++
					report.Failures = append(report.Failures, parseErr.Error())
					return nil
				}
				return nil
			}

			message, parseErr := parseArchiveFile(path, root, format, 0)
			if parseErr != nil {
				report.ParseFailures++
				report.Failures = append(report.Failures, parseErr.Error())
				return nil
			}
			report.Parsed++
			progress.parsed.Add(1)
			messages <- message

			return nil
		},
	)

	close(messages)
	workerGroup.Wait()
	for index := range locals {
		mergeArchiveIngestReport(report, &locals[index])
	}

	return walkErr
}

// mergeArchiveIngestReport folds a worker's ingest-side counters and message
// lists into the shared report. Parse-side counters (Scanned/Parsed/Skipped/
// ParseFailures) are owned by the single walk goroutine and are not merged here.
func mergeArchiveIngestReport(dst, src *ArchiveImportReport) {
	dst.Imported += src.Imported
	dst.ExactDuplicates += src.ExactDuplicates
	dst.MessageIDDuplicates += src.MessageIDDuplicates
	dst.GeneratedMessageIDs += src.GeneratedMessageIDs
	dst.LowNoiseSkipped += src.LowNoiseSkipped
	dst.TrivialSkipped += src.TrivialSkipped
	dst.MinorVersions += src.MinorVersions
	dst.MajorVersions += src.MajorVersions
	dst.Promoted += src.Promoted
	dst.Collisions += src.Collisions
	dst.SkippedMessageIDs = append(dst.SkippedMessageIDs, src.SkippedMessageIDs...)
	dst.Failures = append(dst.Failures, src.Failures...)
}

func (importer *ArchiveImporter) ingestArchiveMessage(
	ctx context.Context,
	sourceName string,
	message archiveMessage,
	request ArchiveImportRequest,
	report *ArchiveImportReport,
) error {
	if message.GeneratedMessage {
		report.GeneratedMessageIDs++
	}
	if request.DryRun {
		report.Imported++
		return nil
	}

	// Serialize messages sharing a Message-ID so two workers cannot both observe
	// "canonical not found" and each ingest it as a new canonical; the second to
	// acquire the lock then correctly takes the duplicate/variant path.
	unlock := importer.lockMessageID(message.MessageID)
	defer unlock()

	identityRef := contracts.SourceObjectRef{
		SourceKind: contracts.MailIdentitySourceKind,
		SourceName: contracts.MailIdentitySourceName,
		ExternalID: message.MessageID,
	}
	if existing, found, err := importer.store.LookupSourceObject(
		ctx,
		identityRef,
	); err != nil {
		return err
	} else if found {
		return importer.ingestDuplicateOrVariant(
			ctx,
			sourceName,
			existing,
			message,
			request.LowNoise,
			report,
		)
	}

	digest, created, err := importer.ingestCanonicalMessage(ctx, sourceName, message)
	if err != nil {
		return err
	}
	if created {
		report.Imported++
	} else {
		report.ExactDuplicates++
	}
	if digest == "" {
		return errors.New("archive import produced empty digest")
	}

	return nil
}

func (importer *ArchiveImporter) ingestDuplicateOrVariant(
	ctx context.Context,
	sourceName string,
	canonical contracts.ObjectDigest,
	message archiveMessage,
	lowNoise bool,
	report *ArchiveImportReport,
) error {
	manifest, err := importer.store.ReadManifest(ctx, canonical)
	if err != nil {
		return err
	}
	metadata := mailFacetMetadata(manifest)
	bodyLineHash := stringValue(metadata["body_line_fingerprint"])
	if bodyLineHash == message.BodyLineHash {
		report.MessageIDDuplicates++
		if lowNoise {
			report.LowNoiseSkipped++
			report.TrivialSkipped++
			appendSkippedMessageID(report, message.MessageID)

			return importer.writeArchiveMembershipRecord(ctx, sourceName, message, canonical)
		}

		return importer.store.AttachProvenance(ctx, canonical, []contracts.Provenance{
			archiveProvenance(sourceName, message),
		})
	}

	scale := archiveVersionScale(metadata, message)
	if lowNoise && scale == contracts.VersionScaleTrivial {
		report.LowNoiseSkipped++
		report.TrivialSkipped++
		appendSkippedMessageID(report, message.MessageID)

		return importer.writeArchiveMembershipRecord(ctx, sourceName, message, canonical)
	}

	report.Collisions++
	switch scale {
	case contracts.VersionScaleMajor:
		report.MajorVersions++
	default:
		report.MinorVersions++
	}

	return importer.ingestVariantMessage(
		ctx,
		sourceName,
		canonical,
		manifest,
		message,
		scale,
		report,
	)
}

func (importer *ArchiveImporter) ingestCanonicalMessage(
	ctx context.Context,
	sourceName string,
	message archiveMessage,
) (contracts.ObjectDigest, bool, error) {
	parts, err := importer.writeArchiveMessageParts(ctx, sourceName, message, false)
	if err != nil {
		return "", false, err
	}
	recordDigest, err := importer.writeVersionRecord(
		ctx,
		sourceName,
		message,
		contracts.VersionRepresentationFull,
		contracts.VersionScaleMinor,
		true,
		"",
		"",
		"",
	)
	if err != nil {
		return "", false, err
	}
	parts = append(parts, contracts.CompoundPart{
		Digest: recordDigest,
		Role:   contracts.VersionRecordRole,
		Order:  100,
	})
	provenance := []contracts.Provenance{
		archiveProvenance(sourceName, message),
		identityProvenance(message),
	}
	object := IngestObject{
		ObservedAt:  message.ObservedAt,
		SourceKind:  contracts.MailArchiveSourceKind,
		SourceName:  sourceName,
		ExternalID:  message.ExternalID,
		ExternalVer: message.ExternalVersion,
		SourceHint:  message.Subject,
		Compound: &CompoundObject{
			ObjectID:   "mail_message:" + message.MessageID,
			MediaType:  "application/vnd.gmeow.archive-message+json",
			SourceHint: message.Subject,
			ContentRoles: []string{
				contracts.MailMessageContentRole,
				contracts.MailMessageContainerRole,
			},
			Facets: []contracts.Facet{
				{
					Kind: contracts.MailMessageFacetKind,
					Metadata: archiveMailMetadata(
						message,
						false,
						contracts.VersionScaleMinor,
						1,
						message.Fingerprint,
					),
				},
				{
					Kind: contracts.VersionSetFacetKind,
					Metadata: map[string]any{
						"domain_kind":          contracts.MailVersionSetDomain,
						"logical_id":           message.MessageID,
						"canonical_version_id": message.Fingerprint,
						"version_count":        1,
						"max_scale":            contracts.VersionScaleMinor,
						"updated_at":           message.ObservedAt.Format(time.RFC3339Nano),
					},
				},
				{Kind: "container"},
			},
			Provenance: provenance,
			Parts:      parts,
		},
	}

	return importer.service.Ingest(ctx, object)
}

func (importer *ArchiveImporter) ingestVariantMessage(
	ctx context.Context,
	sourceName string,
	canonical contracts.ObjectDigest,
	canonicalManifest contracts.Manifest,
	message archiveMessage,
	scale string,
	report *ArchiveImportReport,
) error {
	parts, err := importer.writeArchiveMessageParts(ctx, sourceName, message, true)
	if err != nil {
		return err
	}
	bodyOrder := importer.canonicalBodyOrder(
		ctx,
		canonicalManifest,
		int64(len(message.Body)),
	)
	for index, part := range parts {
		if part.Role == contracts.MailBodyRole {
			parts[index].Order = bodyOrder
			break
		}
	}

	promote := shouldPromoteCanonical(ctx, importer.store, canonicalManifest, message)
	headerPatch, bodyPatch, err := importer.writeVariantPatches(
		ctx,
		sourceName,
		canonicalManifest,
		message,
	)
	if err != nil {
		return err
	}
	recordDigest, err := importer.writeVersionRecord(
		ctx,
		sourceName,
		message,
		contracts.VersionRepresentationDelta,
		scale,
		promote,
		stringValue(mailFacetMetadata(canonicalManifest)["canonical_version_id"]),
		canonical,
		firstNonEmptyDigest(bodyPatch, headerPatch),
	)
	if err != nil {
		return err
	}
	canonicalParts := []contracts.CompoundPart{{
		Digest: recordDigest,
		Role:   contracts.VersionRecordRole,
		Order:  100 + versionCount(canonicalManifest),
	}}
	if promote {
		canonicalParts = append(canonicalParts, parts...)
	}
	maxScale := maxVersionScale(
		stringValue(mailFacetMetadata(canonicalManifest)["max_scale"]),
		scale,
	)
	nextVersionCount := versionCount(canonicalManifest) + 1
	canonicalVersionID := firstNonEmpty(
		promotedCanonicalVersionID(promote, message),
		stringValue(mailFacetMetadata(canonicalManifest)["canonical_version_id"]),
	)
	canonicalMailMetadata := archiveMailMetadata(
		message,
		true,
		maxScale,
		nextVersionCount,
		canonicalVersionID,
	)
	if !promote {
		canonicalMailMetadata = cloneMetadata(mailFacetMetadata(canonicalManifest))
		canonicalMailMetadata["message_id_collision"] = true
		canonicalMailMetadata["version_count"] = nextVersionCount
		canonicalMailMetadata["max_scale"] = maxScale
		canonicalMailMetadata["canonical_version_id"] = canonicalVersionID
	}

	object := IngestObject{
		ObservedAt:  message.ObservedAt,
		SourceKind:  contracts.MailArchiveSourceKind,
		SourceName:  sourceName,
		ExternalID:  message.ExternalID,
		ExternalVer: message.ExternalVersion,
		SourceHint:  message.Subject,
		Compound: &CompoundObject{
			ObjectID:   "mail_message:" + message.MessageID,
			MediaType:  "application/vnd.gmeow.archive-message+json",
			SourceHint: message.Subject,
			ContentRoles: []string{
				contracts.MailMessageContentRole,
				contracts.MailMessageContainerRole,
			},
			Facets: []contracts.Facet{{
				Kind:     contracts.MailMessageFacetKind,
				Metadata: canonicalMailMetadata,
			}, {
				Kind: contracts.VersionSetFacetKind,
				Metadata: map[string]any{
					"domain_kind":          contracts.MailVersionSetDomain,
					"logical_id":           message.MessageID,
					"canonical_version_id": canonicalVersionID,
					"version_count":        nextVersionCount,
					"max_scale":            maxScale,
					"updated_at":           message.ObservedAt.Format(time.RFC3339Nano),
				},
			}, {Kind: "container"}},
			Provenance: []contracts.Provenance{archiveProvenance(sourceName, message)},
			Parts:      canonicalParts,
		},
	}
	_, _, err = importer.service.Ingest(ctx, object)
	if err != nil {
		return err
	}
	if promote {
		report.Promoted++
	}

	variantParts := []contracts.CompoundPart{}
	if headerPatch != "" {
		variantParts = append(
			variantParts,
			contracts.CompoundPart{Digest: headerPatch, Role: contracts.MailPatchDiffRole},
		)
	}
	if bodyPatch != "" {
		variantParts = append(
			variantParts,
			contracts.CompoundPart{
				Digest: bodyPatch,
				Role:   contracts.MailPatchDiffRole,
				Order:  1,
			},
		)
	}
	if len(variantParts) == 0 {
		return nil
	}
	variantObject := IngestObject{
		ObservedAt:  message.ObservedAt,
		SourceKind:  contracts.MailArchiveSourceKind,
		SourceName:  sourceName,
		ExternalID:  message.ExternalID + ":variant",
		ExternalVer: message.ExternalVersion,
		SourceHint:  "variant " + message.Subject,
		Compound: &CompoundObject{
			ObjectID:     "mail_message_variant:" + message.Fingerprint,
			MediaType:    "application/vnd.gmeow.mail-message-variant+json",
			ContentRoles: []string{contracts.MailMessageVariantRole},
			Facets: []contracts.Facet{
				{
					Kind: contracts.MailVariantFacetKind,
					Metadata: archiveMailMetadata(
						message,
						true,
						scale,
						versionCount(canonicalManifest)+1,
						message.Fingerprint,
					),
				},
			},
			Provenance: []contracts.Provenance{archiveProvenance(sourceName, message)},
			Parts:      variantParts,
		},
	}
	_, _, err = importer.service.Ingest(ctx, variantObject)

	return err
}

func (importer *ArchiveImporter) writeArchiveMessageParts(
	ctx context.Context,
	sourceName string,
	message archiveMessage,
	variant bool,
) ([]contracts.CompoundPart, error) {
	headers, err := json.Marshal(mailmessage.SortedHeaderRows(message.Headers))
	if err != nil {
		return nil, err
	}
	archiveData, err := json.Marshal(map[string]any{
		"format":                message.Format,
		"source_path":           message.SourcePath,
		"mailbox":               message.Mailbox,
		"external_id":           message.ExternalID,
		"external_version":      message.ExternalVersion,
		"message_id":            message.MessageID,
		"generated_message_id":  message.GeneratedMessage,
		"canonical_fingerprint": message.Fingerprint,
		"variant":               variant,
	})
	if err != nil {
		return nil, err
	}
	inputs := []struct {
		role      string
		mediaType string
		payload   []byte
		order     int
		facets    []contracts.Facet
	}{
		{
			contracts.MailHeadersRole,
			"text/rfc822-headers",
			headers,
			0,
			[]contracts.Facet{{Kind: "email_part"}},
		},
		{
			contracts.MailBodyRole,
			firstNonEmpty(message.BodyMediaType, "text/plain"),
			message.Body,
			1,
			[]contracts.Facet{{Kind: "email_part"}},
		},
		{
			contracts.MailArchiveMetadataRole,
			"application/json",
			archiveData,
			2,
			[]contracts.Facet{{Kind: "file"}},
		},
		{
			contracts.MailMIMEStructureRole,
			"application/vnd.gmeow.mime-structure+json",
			[]byte(`{"source":"archive_import"}`),
			3,
			[]contracts.Facet{{Kind: "email_part"}},
		},
	}

	jobs := make([]partIngestJob, 0, len(inputs)+len(message.Attachments))
	for _, input := range inputs {
		jobs = append(jobs, partIngestJob{
			object: IngestObject{
				ObservedAt:   message.ObservedAt,
				Reader:       bytes.NewReader(input.payload),
				MediaType:    input.mediaType,
				SourceKind:   contracts.MailArchiveSourceKind,
				SourceName:   sourceName,
				ExternalID:   message.ExternalID + ":" + input.role,
				ExternalVer:  message.ExternalVersion,
				SourceHint:   input.role,
				ContentRoles: []string{input.role},
				Facets:       input.facets,
			},
			part: contracts.CompoundPart{Role: input.role, Order: input.order},
		})
	}
	for index, attachment := range message.Attachments {
		// Prefix with the attachment index so two attachments that share a file
		// name (or both lack one) still get distinct, unambiguous source refs.
		attachmentID := fmt.Sprintf("%d:%s", index, attachment.FileName)
		jobs = append(jobs, partIngestJob{
			object: IngestObject{
				ObservedAt:   message.ObservedAt,
				Reader:       bytes.NewReader(attachment.Content),
				MediaType:    firstNonEmpty(attachment.MediaType, "application/octet-stream"),
				SourceKind:   contracts.MailArchiveSourceKind,
				SourceName:   sourceName,
				ExternalID:   fmt.Sprintf("%s:attachment:%s", message.ExternalID, attachmentID),
				ExternalVer:  message.ExternalVersion,
				SourceHint:   attachment.FileName,
				ContentRoles: []string{contracts.MailAttachmentRole},
				Facets: []contracts.Facet{{
					Kind: "file",
					Metadata: map[string]any{
						"display_name": attachment.FileName,
					},
				}},
			},
			part: contracts.CompoundPart{
				Role:  contracts.MailAttachmentRole,
				Order: 10 + index,
				Metadata: map[string]any{
					"filename": attachment.FileName,
				},
			},
		})
	}

	return importer.ingestMessageParts(ctx, jobs)
}

type partIngestJob struct {
	object IngestObject
	part   contracts.CompoundPart
}

// ingestMessageParts ingests a message's parts concurrently and returns them in
// their original order. The parts are independent content-addressed objects with
// no inter-part ordering dependency, so overlapping their Puts — especially
// byte-heavy attachments — is safe; only the resulting slice order matters, so
// each result is written back by index. Each Put goes through the importer's
// shared ingest semaphore, so parts and concurrent messages together never exceed
// the configured ingest concurrency.
func (importer *ArchiveImporter) ingestMessageParts(
	ctx context.Context,
	jobs []partIngestJob,
) ([]contracts.CompoundPart, error) {
	parts := make([]contracts.CompoundPart, len(jobs))
	errs := make([]error, len(jobs))
	var waitGroup sync.WaitGroup

	for index := range jobs {
		// Acquire the ingest slot under the context so a cancellation mid-import
		// unwinds promptly instead of blocking on a full semaphore (and so we stop
		// spawning goroutines that would only fail fast inside Ingest).
		if err := importer.acquireIngest(ctx); err != nil {
			errs[index] = err

			continue
		}
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			defer importer.releaseIngest()

			digest, _, err := importer.service.Ingest(ctx, jobs[index].object)
			if err != nil {
				errs[index] = err

				return
			}
			part := jobs[index].part
			part.Digest = digest
			parts[index] = part
		}(index)
	}
	waitGroup.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return parts, nil
}

func (importer *ArchiveImporter) acquireIngest(ctx context.Context) error {
	if importer.ingestSem == nil {
		return nil
	}

	select {
	case importer.ingestSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (importer *ArchiveImporter) releaseIngest() {
	if importer.ingestSem != nil {
		<-importer.ingestSem
	}
}

func (importer *ArchiveImporter) writeVariantPatches(
	ctx context.Context,
	sourceName string,
	canonical contracts.Manifest,
	message archiveMessage,
) (contracts.ObjectDigest, contracts.ObjectDigest, error) {
	canonicalHeaders, err := importer.canonicalPartContent(
		ctx,
		canonical,
		contracts.MailHeadersRole,
	)
	if err != nil {
		return "", "", err
	}
	canonicalBody, err := importer.canonicalPartContent(
		ctx,
		canonical,
		contracts.MailBodyRole,
	)
	if err != nil {
		return "", "", err
	}

	headerTarget := mustJSON(mailmessage.SortedHeaderRows(message.Headers))
	headerPatch := []byte(linePatch(string(canonicalHeaders), string(headerTarget)))
	bodyPatch := []byte(linePatch(string(canonicalBody), string(message.Body)))
	headerDigest, _, err := importer.service.Ingest(ctx, IngestObject{
		ObservedAt:   message.ObservedAt,
		Reader:       bytes.NewReader(headerPatch),
		MediaType:    "text/x-gmeow-patch",
		SourceKind:   contracts.MailArchiveSourceKind,
		SourceName:   sourceName,
		ExternalID:   message.ExternalID + ":header_patch",
		ExternalVer:  message.ExternalVersion,
		SourceHint:   "header patch",
		ContentRoles: []string{contracts.MailPatchDiffRole},
		Facets: []contracts.Facet{
			{Kind: contracts.VersionDeltaFacetKind, Metadata: map[string]any{
				"base_digest":   canonical.ObjectDigest,
				"target_sha256": sha256Hex(headerTarget),
				"codec":         "gmeow-patch-v1",
				"role":          contracts.MailHeadersRole,
			}},
		},
	})
	if err != nil {
		return "", "", err
	}
	bodyDigest, _, err := importer.service.Ingest(ctx, IngestObject{
		ObservedAt:   message.ObservedAt,
		Reader:       bytes.NewReader(bodyPatch),
		MediaType:    "text/x-gmeow-patch",
		SourceKind:   contracts.MailArchiveSourceKind,
		SourceName:   sourceName,
		ExternalID:   message.ExternalID + ":body_patch",
		ExternalVer:  message.ExternalVersion,
		SourceHint:   "body patch",
		ContentRoles: []string{contracts.MailPatchDiffRole},
		Facets: []contracts.Facet{
			{Kind: contracts.VersionDeltaFacetKind, Metadata: map[string]any{
				"base_digest":   canonical.ObjectDigest,
				"target_sha256": sha256Hex(message.Body),
				"codec":         "gmeow-patch-v1",
				"role":          contracts.MailBodyRole,
			}},
		},
	})

	return headerDigest, bodyDigest, err
}

func (importer *ArchiveImporter) writeVersionRecord(
	ctx context.Context,
	sourceName string,
	message archiveMessage,
	representation string,
	scale string,
	canonical bool,
	baseVersionID string,
	baseDigest contracts.ObjectDigest,
	deltaDigest contracts.ObjectDigest,
) (contracts.ObjectDigest, error) {
	metadata := contracts.VersionRecordMetadata{
		ObservedAt:              message.ObservedAt.Format(time.RFC3339Nano),
		Representation:          representation,
		VersionID:               message.Fingerprint,
		VersionSetID:            "mail_message:" + message.MessageID,
		DomainKind:              contracts.MailVersionSetDomain,
		LogicalID:               message.MessageID,
		BaseVersionID:           baseVersionID,
		BaseDigest:              baseDigest,
		DeltaDigest:             deltaDigest,
		TargetHash:              message.Fingerprint,
		CanonicalizationVersion: "mail-archive-v1",
		Scale:                   scale,
		Canonical:               canonical,
		Promoted:                canonical && baseVersionID != "",
		InputFingerprints: map[string]string{
			contracts.MailBodyLineFingerprint: message.BodyLineHash,
			contracts.MailSemanticFingerprint: message.Fingerprint,
		},
	}
	if representation == contracts.VersionRepresentationFull {
		metadata.FullDigest = baseDigest
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	digest, _, err := importer.service.Ingest(ctx, IngestObject{
		ObservedAt:   message.ObservedAt,
		Reader:       bytes.NewReader(payload),
		MediaType:    "application/vnd.gmeow.version-record+json",
		SourceKind:   contracts.MailArchiveSourceKind,
		SourceName:   sourceName,
		ExternalID:   message.ExternalID + ":version_record",
		ExternalVer:  message.ExternalVersion,
		SourceHint:   "version record",
		ContentRoles: []string{contracts.VersionRecordRole},
		Facets: []contracts.Facet{{
			Kind: contracts.VersionRecordFacetKind,
			Metadata: map[string]any{
				"version_id":       metadata.VersionID,
				"version_set_id":   metadata.VersionSetID,
				"domain_kind":      metadata.DomainKind,
				"logical_id":       metadata.LogicalID,
				"representation":   metadata.Representation,
				"scale":            metadata.Scale,
				"canonical":        metadata.Canonical,
				"promoted":         metadata.Promoted,
				"target_hash":      metadata.TargetHash,
				"body_line_hash":   message.BodyLineHash,
				"base_version_id":  metadata.BaseVersionID,
				"base_digest":      string(metadata.BaseDigest),
				"delta_digest":     string(metadata.DeltaDigest),
				"source_path":      message.SourcePath,
				"source_format":    message.Format,
				"source_mailbox":   message.Mailbox,
				"generated_msg_id": message.GeneratedMessage,
			},
		}},
	})

	return digest, err
}

func archiveVersionScale(metadata map[string]any, message archiveMessage) string {
	if stringValue(metadata["body_line_fingerprint"]) == message.BodyLineHash {
		return contracts.VersionScaleTrivial
	}
	if strings.EqualFold(stringValue(metadata["subject"]), message.Subject) &&
		strings.EqualFold(stringValue(metadata["from"]), message.From) &&
		strings.EqualFold(stringValue(metadata["to"]), message.To) {
		return contracts.VersionScaleMinor
	}

	return contracts.VersionScaleMajor
}

func shouldPromoteCanonical(
	ctx context.Context,
	store FilestoreClient,
	manifest contracts.Manifest,
	message archiveMessage,
) bool {
	for _, part := range manifest.Compound.Parts {
		if part.Role != contracts.MailBodyRole || part.Order != 1 {
			continue
		}
		bodyManifest, err := store.ReadManifest(ctx, part.Digest)
		return err == nil && int64(len(message.Body)) > bodyManifest.Size
	}
	return false
}

func promotedCanonicalVersionID(promote bool, message archiveMessage) string {
	if promote {
		return message.Fingerprint
	}
	return ""
}

func firstNonEmptyDigest(values ...contracts.ObjectDigest) contracts.ObjectDigest {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func versionCount(manifest contracts.Manifest) int {
	metadata := mailFacetMetadata(manifest)
	if count := intFromAny(metadata["version_count"]); count > 0 {
		return count
	}
	count := 0
	for _, part := range manifest.Compound.Parts {
		if part.Role == contracts.VersionRecordRole {
			count++
		}
	}
	if count == 0 {
		return 1
	}
	return count
}

func maxVersionScale(left, right string) string {
	rank := map[string]int{
		contracts.VersionScaleTrivial: 0,
		contracts.VersionScaleMinor:   1,
		contracts.VersionScaleMajor:   2,
	}
	if rank[right] > rank[left] {
		return right
	}
	if left != "" {
		return left
	}
	return right
}

func (importer *ArchiveImporter) canonicalPartContent(
	ctx context.Context,
	manifest contracts.Manifest,
	role string,
) ([]byte, error) {
	for _, part := range manifest.Compound.Parts {
		if part.Role != role {
			continue
		}
		reader, err := importer.store.Open(ctx, part.Digest)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		content, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		return content, nil
	}

	return nil, nil
}

func parseArchiveFile(path, root, format string, offset int) (archiveMessage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return archiveMessage{}, err
	}
	return parseArchiveMessage(raw, path, root, format, offset)
}

func parseArchiveMessage(
	raw []byte,
	path, root, format string,
	offset int,
) (archiveMessage, error) {
	canonical, err := mailmessage.Parse(raw, path)
	if err != nil {
		return archiveMessage{}, fmt.Errorf("parse archive mail message: %w", err)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		return archiveMessage{}, fmt.Errorf("resolve archive message path: %w", err)
	}

	message := archiveMessage{Message: canonical}
	message.ObservedAt = time.Now().UTC()
	message.SourcePath = filepath.Clean(path)
	message.Mailbox = archiveMailbox(rel, format)
	message.Format = format
	message.ExternalID = archiveExternalID(rel, offset)
	message.ExternalVersion = archiveExternalVersion(raw)

	return message, nil
}

func detectArchiveFileFormat(path, root, requested string) string {
	if requested != "" && requested != ArchiveImportFormatAuto {
		return requested
	}
	name := filepath.Base(path)
	if strings.HasPrefix(name, ".") {
		return ""
	}
	if strings.HasSuffix(strings.ToLower(name), ".eml") {
		return ArchiveImportFormatEMLDir
	}
	parent := filepath.Base(filepath.Dir(path))
	if parent == "cur" || parent == "new" {
		if hasSiblingDir(filepath.Dir(filepath.Dir(path)), "tmp") {
			return ArchiveImportFormatMaildir
		}
	}
	if numberedMailFilePattern.MatchString(name) {
		return ArchiveImportFormatNNML
	}
	if looksLikeMboxPath(path) {
		return ArchiveImportFormatMbox
	}
	if looksLikeRFC822File(path) {
		return ArchiveImportFormatEMLDir
	}
	_, _ = root, name
	return ""
}

func looksLikeMboxPath(path string) bool {
	name := filepath.Base(path)
	if strings.Contains(name, ".") || strings.HasSuffix(name, "~") {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	return err == nil && strings.HasPrefix(line, "From ")
}

func looksLikeRFC822File(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for range 20 {
		line, err := reader.ReadString('\n')
		if err != nil {
			return false
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "message-id:") ||
			strings.HasPrefix(lower, "from:") ||
			strings.HasPrefix(lower, "subject:") {
			return true
		}
		if strings.TrimSpace(line) == "" {
			return false
		}
	}
	return false
}

func shouldSkipArchiveDir(name string) bool {
	return name == "tmp" || name == ".git"
}

func shouldSkipArchiveFile(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(name, ".") ||
		strings.HasSuffix(lower, ".msf") ||
		strings.Contains(lower, ".ibex.") ||
		strings.HasSuffix(lower, ".cmeta") ||
		strings.HasSuffix(lower, ".ev-summary") ||
		strings.HasSuffix(lower, ".ev-summary-meta") ||
		strings.HasSuffix(lower, ".index") ||
		strings.HasSuffix(lower, ".index.ids") ||
		strings.HasSuffix(lower, ".index.sorted") ||
		strings.HasSuffix(lower, ".xml") {
		return true
	}
	return false
}

func hasSiblingDir(path, name string) bool {
	info, err := os.Stat(filepath.Join(path, name))
	return err == nil && info.IsDir()
}

func archiveProvenance(sourceName string, message archiveMessage) contracts.Provenance {
	return contracts.Provenance{
		SourceKind:      contracts.MailArchiveSourceKind,
		SourceName:      sourceName,
		ExternalID:      message.ExternalID,
		ExternalVersion: message.ExternalVersion,
		ObservedAt:      message.ObservedAt,
		CapabilitiesSeen: []string{
			contracts.MailMessageContentRole,
			contracts.MailHeadersRole,
			contracts.MailBodyRole,
		},
		Metadata: map[string]any{
			"format":      message.Format,
			"source_path": message.SourcePath,
			"mailbox":     message.Mailbox,
		},
	}
}

func identityProvenance(message archiveMessage) contracts.Provenance {
	return contracts.Provenance{
		SourceKind:      contracts.MailIdentitySourceKind,
		SourceName:      contracts.MailIdentitySourceName,
		ExternalID:      message.MessageID,
		ExternalVersion: "",
		ObservedAt:      message.ObservedAt,
		CapabilitiesSeen: []string{
			contracts.MailMessageIdentityRole,
		},
	}
}

func archiveMailMetadata(
	message archiveMessage,
	collision bool,
	maxScale string,
	versionCount int,
	canonicalVersionID string,
) map[string]any {
	metadata := mailmessage.Metadata(
		message.Message,
		collision,
		maxScale,
		versionCount,
		canonicalVersionID,
	)
	metadata["archive_format"] = message.Format
	metadata["archive_mailbox"] = message.Mailbox
	metadata["archive_source_path"] = message.SourcePath

	return metadata
}

func (importer *ArchiveImporter) canonicalBodyOrder(
	ctx context.Context,
	manifest contracts.Manifest,
	incomingSize int64,
) int {
	for _, part := range manifest.Compound.Parts {
		if part.Role != contracts.MailBodyRole || part.Order != 1 {
			continue
		}
		bodyManifest, err := importer.store.ReadManifest(ctx, part.Digest)
		if err == nil && incomingSize > bodyManifest.Size {
			return 1
		}

		return 1000
	}
	return 1
}

func mailFacetMetadata(manifest contracts.Manifest) map[string]any {
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == contracts.MailMessageFacetKind {
			return facet.Metadata
		}
	}
	return map[string]any{}
}

func cloneMetadata(metadata map[string]any) map[string]any {
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}

	return cloned
}

func appendSkippedMessageID(report *ArchiveImportReport, messageID string) {
	if len(report.SkippedMessageIDs) >= 20 {
		return
	}
	report.SkippedMessageIDs = append(report.SkippedMessageIDs, messageID)
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

func archiveExternalID(relative string, offset int) string {
	if offset > 0 {
		return fmt.Sprintf("%s#%d", filepath.ToSlash(relative), offset)
	}
	return filepath.ToSlash(relative)
}

func archiveExternalVersion(raw []byte) string {
	return sha256Hex(raw)
}

func archiveMailbox(relative, format string) string {
	clean := filepath.ToSlash(filepath.Clean(relative))
	switch format {
	case ArchiveImportFormatMaildir:
		dir := filepath.Dir(filepath.Dir(clean))
		if dir == "." {
			return "maildir"
		}
		return dir
	case ArchiveImportFormatMbox:
		return strings.TrimSuffix(clean, filepath.Ext(clean))
	default:
		dir := filepath.Dir(clean)
		if dir == "." {
			return format
		}
		return dir
	}
}

func sourceNameFromRoot(root string) string {
	base := filepath.Base(filepath.Clean(root))
	base = strings.TrimSpace(base)
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "archive"
	}
	return base
}

func collapseArchiveWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func linePatch(base, target string) string {
	baseLines := strings.Split(base, "\n")
	targetLines := strings.Split(target, "\n")
	var out strings.Builder
	out.WriteString("gmeow-patch-v1\n")
	out.WriteString("--- canonical\n")
	out.WriteString("+++ variant\n")
	for _, line := range baseLines {
		if line != "" {
			out.WriteString("-")
			out.WriteString(line)
			out.WriteString("\n")
		}
	}
	for _, line := range targetLines {
		if line != "" {
			out.WriteString("+")
			out.WriteString(line)
			out.WriteString("\n")
		}
	}
	return out.String()
}
