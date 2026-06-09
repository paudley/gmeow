// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"blackcat.ca/gmeow/internal/cache"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
)

const (
	CapabilityBackfill     = "backfill"
	CapabilityHydrate      = "hydrate"
	CapabilityLiveSearch   = "live_search"
	CapabilityLiveRetrieve = "live_retrieve"
	CapabilityActions      = "actions"
	CapabilityPush         = "push"
	CapabilityExport       = "export"
)

var (
	ErrUnsupportedOperation   = errors.New("unsupported source operation")
	ErrSourceIngestInProgress = errors.New("source ingest already in progress")
)

type FilestoreClient interface {
	LookupSourceObject(
		context.Context,
		contracts.SourceObjectRef,
	) (contracts.ObjectDigest, bool, error)
	TryAcquireSourceIngest(
		context.Context,
		contracts.SourceObjectRef,
	) (contracts.SourceIngestClaim, bool, error)
	ReleaseSourceIngest(context.Context, contracts.SourceIngestClaim) error
	Put(context.Context, rpc.PutRequest) (contracts.ObjectDigest, error)
	Open(context.Context, contracts.ObjectDigest) (io.ReadCloser, error)
	ReadManifest(context.Context, contracts.ObjectDigest) (contracts.Manifest, error)
	AttachProvenance(context.Context, contracts.ObjectDigest, []contracts.Provenance) error
	AttachProvenanceWithPriority(
		context.Context,
		contracts.ObjectDigest,
		[]contracts.Provenance,
		string,
	) error
	PutCompound(context.Context, rpc.CompoundPutRequest) (contracts.ObjectDigest, error)
	WriteSourceCursor(context.Context, contracts.SourceCursor) error
	ReadSourceCursor(
		context.Context,
		contracts.SourceCursorRef,
	) (contracts.SourceCursor, bool, error)
}

type ChangeNotifier interface {
	NotifyObjectsChanged(
		context.Context,
		contracts.ObjectChangeRequest,
	) (contracts.SchedulerScanResponse, error)
}

// PressureReporter exposes the scheduler's backpressure signal to the source.
// The scheduler owns the global pressure view (analysis queue depth); the
// source self-throttles against it, pausing backfill scheduling while pressured
// and resuming once the backlog drains. A nil reporter disables throttling.
type PressureReporter interface {
	Pressured(context.Context) (bool, error)
}

type Adapter interface {
	Name() string
	Kind() string
	Capabilities() []string
}

type PullAdapter interface {
	Adapter
	Pull(
		ctx context.Context,
		service IngestService,
		request PullRequest,
	) ([]IngestObject, contracts.SourceCursor, error)
}

type HydrateAdapter interface {
	Adapter
	Hydrate(ctx context.Context, externalID string) (IngestObject, error)
}

type LiveSearchAdapter interface {
	Adapter
	LiveSearch(ctx context.Context, request LiveSearchRequest) ([]LiveSearchResult, error)
}

type HydratingLiveSearchAdapter interface {
	LiveSearchAdapter
	SearchAndHydrate(
		ctx context.Context,
		service IngestService,
		request LiveSearchRequest,
	) ([]LiveSearchResult, error)
}

type LiveRetrieveAdapter interface {
	Adapter
	LiveRetrieve(ctx context.Context, externalID string) (IngestObject, error)
}

type ActionAdapter interface {
	Adapter
	ApplyAction(ctx context.Context, request ActionRequest) (ActionResult, error)
}

type PullRequest struct {
	Cursor map[string]any
	Limit  int
}

type BackfillRequest struct {
	Cursor map[string]any
	// PriorityClass tags the analysis priority of objects this run ingests and
	// also selects throttling: only low-priority background backfill pauses under
	// analysis backpressure. High-priority inbox refresh runs at full speed.
	PriorityClass string
	CursorKey     string
	PageSize      int
	MaxPages      int
	Concurrency   int
	Resume        bool
}

type BackfillReport struct {
	FinalCursor      contracts.SourceCursor `json:"final_cursor"`
	FailedMessageIDs []string               `json:"failed_message_ids,omitempty"`
	Processed        int                    `json:"processed"`
	Created          int                    `json:"created"`
	Skipped          int                    `json:"skipped"`
	Failed           int                    `json:"failed"`
	Pages            int                    `json:"pages"`
	Completed        bool                   `json:"completed"`
}

type LiveSearchRequest struct {
	Query string
	Limit int
}

type LiveSearchResult struct {
	ObjectDigest    contracts.ObjectDigest
	ExternalID      string
	ExternalVersion string
	Hydrated        bool
}

type ActionRequest struct {
	Parameters   map[string]any
	ObjectDigest contracts.ObjectDigest
	Action       string
}

type ActionResult struct {
	Attributes map[string]any
	Action     string
	Applied    bool
}

type IngestObject struct {
	ObservedAt  time.Time
	Reader      io.Reader
	Compound    *CompoundObject
	MediaType   string
	SourceKind  string
	SourceName  string
	ExternalID  string
	ExternalVer string
	SourceHint  string
	// PriorityClass tags the analysis priority the scheduler should assign to work
	// derived from this object (e.g. fresh_ingest for inbox, background for
	// backfill, repair for imports). Empty defers to the scheduler's reason-based
	// default.
	PriorityClass string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
}

// CompoundObject mirrors rpc.CompoundPutRequest field-for-field so it converts
// directly; keep PriorityClass in the same position as that struct.
type CompoundObject struct {
	ObjectID      string
	MediaType     string
	SourceHint    string
	PriorityClass string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
	Parts         []contracts.CompoundPart
}

type Service struct {
	store    FilestoreClient
	notifier ChangeNotifier
	pressure PressureReporter
	// sourceLookup caches positive (source-ref -> digest) answers. The mapping is
	// permanent: once an exact source/name/external-id/version maps to a digest it
	// never changes, so a duplicate-heavy re-ingest resolves known objects from
	// memory instead of a FILESTORE round-trip. Negatives are never cached.
	sourceLookup *cache.LRU[string, contracts.ObjectDigest]
}

// pressurePollInterval bounds how often a paused backfill re-checks the
// scheduler's pressure signal before resuming.
const pressurePollInterval = 5 * time.Second

// sourceLookupCacheSize bounds the in-process source-ref resolution cache.
const sourceLookupCacheSize = 200000

type IngestService interface {
	Ingest(ctx context.Context, object IngestObject) (contracts.ObjectDigest, bool, error)
	LookupSourceObject(
		ctx context.Context,
		ref contracts.SourceObjectRef,
	) (contracts.ObjectDigest, bool, error)
}

func NewService(store FilestoreClient) (*Service, error) {
	return NewServiceWithNotifier(store, nil)
}

func NewServiceWithNotifier(
	store FilestoreClient,
	notifier ChangeNotifier,
) (*Service, error) {
	if store == nil {
		return nil, errors.New("source filestore client is required")
	}

	return &Service{
		store:        store,
		notifier:     notifier,
		sourceLookup: cache.NewLRU[string, contracts.ObjectDigest](sourceLookupCacheSize),
	}, nil
}

func sourceRefKey(ref contracts.SourceObjectRef) string {
	return ref.SourceKind + "\x00" +
		ref.SourceName + "\x00" +
		ref.ExternalID + "\x00" +
		ref.ExternalVersion
}

// lookupSourceObject resolves a source ref to its object digest, serving and
// populating the permanent positive cache so repeated lookups of an already-seen
// ref (the dominant case in a duplicate-heavy re-ingest) avoid a FILESTORE call.
func (service *Service) lookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	key := sourceRefKey(ref)
	if digest, ok := service.sourceLookup.Get(key); ok {
		return digest, true, nil
	}

	digest, found, err := service.store.LookupSourceObject(ctx, ref)
	if err != nil {
		return "", false, err
	}

	if found {
		service.sourceLookup.Put(key, digest)
	}

	return digest, found, nil
}

// SetPressureReporter wires the scheduler backpressure signal that gates backfill
// scheduling. It is optional: with no reporter set, backfill never pauses.
func (service *Service) SetPressureReporter(reporter PressureReporter) {
	service.pressure = reporter
}

// awaitPressureRelief blocks while the scheduler reports backpressure, so the
// source pauses fetching new backfill pages until the analysis backlog drains
// below the scheduler's low-water mark. It fails open: a pressure-check error
// must not wedge ingest, so it returns and lets the page proceed. It honours
// context cancellation so a shutdown is never delayed by a pause.
func (service *Service) awaitPressureRelief(ctx context.Context) error {
	if service.pressure == nil {
		return nil
	}

	paused := false
	for {
		pressured, err := service.pressure.Pressured(ctx)
		if err != nil {
			log.Printf("source backfill pressure check failed: %v", err)

			return nil
		}
		if !pressured {
			if paused {
				log.Printf("source backfill resumed: analysis backpressure released")
			}

			return nil
		}
		if !paused {
			paused = true
			log.Printf("source backfill paused: analysis backpressure engaged")
		}

		timer := time.NewTimer(pressurePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (service *Service) Ingest(
	ctx context.Context,
	object IngestObject,
) (contracts.ObjectDigest, bool, error) {
	ref, hasRef, err := sourceRef(object)
	if err != nil {
		return "", false, err
	}

	provenance := provenanceFor(object)
	if hasRef {
		if digest, found, err := service.lookupSourceObject(ctx, ref); err != nil {
			return "", false, err
		} else if found {
			return digest, false, nil
		}
	}

	var claim contracts.SourceIngestClaim
	claimAcquired := false
	if hasRef {
		var acquired bool
		claim, acquired, err = service.store.TryAcquireSourceIngest(ctx, ref)
		if err != nil {
			return "", false, err
		}

		if !acquired {
			return "", false, fmt.Errorf(
				"%w for %s/%s/%s",
				ErrSourceIngestInProgress,
				ref.SourceKind,
				ref.SourceName,
				ref.ExternalID,
			)
		}

		claimAcquired = true
		defer func() {
			if claimAcquired {
				service.releaseClaim(ctx, claim)
			}
		}()
		if digest, found, err := service.lookupSourceObject(ctx, ref); err != nil {
			return "", false, err
		} else if found {
			if err := service.store.ReleaseSourceIngest(ctx, claim); err != nil {
				return "", false, err
			}
			claimAcquired = false

			return digest, false, nil
		}
	}

	if object.Compound != nil {
		compound := *object.Compound
		compound.Provenance = mergeProvenance(compound.Provenance, provenance)
		if object.PriorityClass != "" {
			compound.PriorityClass = object.PriorityClass
		}
		digest, err := service.store.PutCompound(ctx, rpc.CompoundPutRequest(compound))
		if err != nil {
			if digest, found, lookupErr := service.lookupAfterWriteError(
				ctx,
				ref,
				hasRef,
			); lookupErr != nil {
				return "", false, lookupErr
			} else if found {
				return digest, false, nil
			}

			return "", false, err
		}
		if claimAcquired {
			if err := service.store.ReleaseSourceIngest(ctx, claim); err != nil {
				return "", false, err
			}
			claimAcquired = false
		}

		return digest, true, service.notifyChanged(ctx, digest)
	}

	if object.Reader == nil {
		return "", false, errors.New("source ingest object reader is required")
	}
	if closer, ok := object.Reader.(io.Closer); ok {
		defer closer.Close()
	}

	digest, err := service.store.Put(ctx, rpc.PutRequest{
		Reader:        object.Reader,
		MediaType:     object.MediaType,
		SourceHint:    object.SourceHint,
		PriorityClass: object.PriorityClass,
		ContentRoles:  append([]string{}, object.ContentRoles...),
		Facets:        append([]contracts.Facet{}, object.Facets...),
		Provenance:    mergeProvenance(object.Provenance, provenance),
		Relationships: append([]contracts.Relationship{}, object.Relationships...),
	})
	if err != nil {
		if digest, found, lookupErr := service.lookupAfterWriteError(
			ctx,
			ref,
			hasRef,
		); lookupErr != nil {
			return "", false, lookupErr
		} else if found {
			return digest, false, nil
		}

		return "", false, err
	}
	if claimAcquired {
		if err := service.store.ReleaseSourceIngest(ctx, claim); err != nil {
			return "", false, err
		}
		claimAcquired = false
	}

	return digest, true, service.notifyChanged(ctx, digest)
}

func (service *Service) lookupAfterWriteError(
	ctx context.Context,
	ref contracts.SourceObjectRef,
	hasRef bool,
) (contracts.ObjectDigest, bool, error) {
	if !hasRef {
		return "", false, nil
	}

	return service.lookupSourceObject(ctx, ref)
}

func (service *Service) notifyChanged(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	if service.notifier == nil {
		return nil
	}

	_, err := service.notifier.NotifyObjectsChanged(ctx, contracts.ObjectChangeRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigests: []contracts.ObjectDigest{digest},
	})
	if err != nil {
		log.Printf("source notification failed digest=%s error=%v", digest, err)
	}

	return nil
}

func (service *Service) LookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	return service.lookupSourceObject(ctx, ref)
}

func (service *Service) WriteCursor(
	ctx context.Context,
	adapter Adapter,
	cursor contracts.SourceCursor,
) error {
	if cursor.SourceKind == "" {
		cursor.SourceKind = adapter.Kind()
	}

	if cursor.SourceName == "" {
		cursor.SourceName = adapter.Name()
	}

	if cursor.SchemaVersion == 0 {
		cursor.SchemaVersion = contracts.SchemaVersionPhase00
	}

	if cursor.UpdatedAt.IsZero() {
		cursor.UpdatedAt = time.Now().UTC()
	}

	return service.store.WriteSourceCursor(ctx, cursor)
}

func (service *Service) ReadCursor(
	ctx context.Context,
	adapter Adapter,
) (contracts.SourceCursor, bool, error) {
	if adapter == nil {
		return contracts.SourceCursor{}, false, errors.New("source adapter is required")
	}

	return service.store.ReadSourceCursor(ctx, contracts.SourceCursorRef{
		SourceKind: adapter.Kind(),
		SourceName: adapter.Name(),
	})
}

func (service *Service) RunBackfill(
	ctx context.Context,
	adapter PullAdapter,
	request BackfillRequest,
) (BackfillReport, error) {
	if adapter == nil {
		return BackfillReport{}, errors.New("source pull adapter is required")
	}
	if !hasCapability(adapter, CapabilityBackfill) {
		return BackfillReport{}, fmt.Errorf(
			"%w: source %s/%s does not support backfill",
			ErrUnsupportedOperation,
			adapter.Kind(),
			adapter.Name(),
		)
	}

	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	workerCount := request.Concurrency
	if workerCount <= 0 {
		workerCount = 4
	}

	cursor := cloneCursor(request.Cursor)
	storedCursor := contracts.SourceCursor{}
	if request.CursorKey != "" {
		stored, found, err := service.ReadCursor(ctx, adapter)
		if err != nil {
			return BackfillReport{}, err
		}
		if found {
			storedCursor = stored
		}
		if found && request.Resume {
			nested := mapCursorValue(stored.Cursor, request.CursorKey)
			if nested != nil {
				cursor = nested
			}
		}
	}
	report := BackfillReport{}
	if boolCursorValue(cursor, "completed") {
		report.Completed = true
		report.FinalCursor = contracts.SourceCursor{
			SchemaVersion: contracts.SchemaVersionPhase00,
			SourceKind:    adapter.Kind(),
			SourceName:    adapter.Name(),
			Cursor:        cursor,
			UpdatedAt:     time.Now().UTC(),
		}
		if request.CursorKey != "" {
			report.FinalCursor = namespacedCursor(
				storedCursor,
				report.FinalCursor,
				request.CursorKey,
			)
		}

		return report, nil
	}

	throttled := request.PriorityClass == contracts.PriorityBackground
	for report.MaxPagesNotReached(request.MaxPages) {
		// Only low-priority background backfill gates on backpressure: a fast
		// historical backfill must not outrun analysis and grow the work queue
		// without bound. High-priority inbox sync, search, and imports run at full
		// speed and rely on priority ordering to preempt backfill's analysis.
		if throttled {
			if err := service.awaitPressureRelief(ctx); err != nil {
				return report, err
			}
		}

		objects, nextCursor, err := adapter.Pull(ctx, service, PullRequest{
			Cursor: cursor,
			Limit:  pageSize,
		})
		if err != nil {
			report.FinalCursor = nextCursor
			if nextCursor.SourceKind != "" && nextCursor.SourceName != "" {
				_ = service.writeBackfillCursor(
					ctx,
					adapter,
					storedCursor,
					nextCursor,
					request.CursorKey,
				)
			}

			return report, err
		}
		if len(objects) == 0 && boolCursorValue(nextCursor.Cursor, "completed") {
			report.Completed = true
			report.FinalCursor = nextCursor
			if err := service.writeBackfillCursor(
				ctx,
				adapter,
				storedCursor,
				nextCursor,
				request.CursorKey,
			); err != nil {
				return report, err
			}

			return report, nil
		}

		pageReport := service.ingestBackfillPage(
			ctx,
			objects,
			workerCount,
			request.PriorityClass,
		)
		report.Processed += pageReport.Processed
		report.Created += pageReport.Created
		report.Skipped += pageReport.Skipped
		report.Failed += pageReport.Failed
		report.FailedMessageIDs = append(
			report.FailedMessageIDs,
			pageReport.FailedMessageIDs...,
		)
		report.Pages++

		nextCursor.Cursor = mergeBackfillCounts(nextCursor.Cursor, report)
		report.Completed = boolCursorValue(nextCursor.Cursor, "completed")
		report.FinalCursor = nextCursor
		if err := service.writeBackfillCursor(
			ctx,
			adapter,
			storedCursor,
			nextCursor,
			request.CursorKey,
		); err != nil {
			return report, err
		}
		if pageReport.Failed > 0 {
			return report, fmt.Errorf(
				"source backfill failed for %d message(s)",
				pageReport.Failed,
			)
		}
		if report.Completed {
			return report, nil
		}

		cursor = cloneCursor(nextCursor.Cursor)
	}

	return report, nil
}

func (service *Service) writeBackfillCursor(
	ctx context.Context,
	adapter Adapter,
	stored contracts.SourceCursor,
	next contracts.SourceCursor,
	key string,
) error {
	if key == "" {
		return service.WriteCursor(ctx, adapter, next)
	}

	latest, found, err := service.ReadCursor(ctx, adapter)
	if err != nil {
		return err
	}
	if found {
		stored = latest
	}

	return service.WriteCursor(ctx, adapter, namespacedCursor(stored, next, key))
}

func (report BackfillReport) MaxPagesNotReached(maxPages int) bool {
	return maxPages <= 0 || report.Pages < maxPages
}

func (service *Service) ingestBackfillPage(
	ctx context.Context,
	objects []IngestObject,
	workerCount int,
	priorityClass string,
) BackfillReport {
	jobs := make(chan IngestObject)
	results := make(chan backfillIngestResult)
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for object := range jobs {
				object.PriorityClass = priorityClass
				_, created, err := service.Ingest(ctx, object)
				results <- backfillIngestResult{
					messageID: object.ExternalID,
					created:   created,
					err:       err,
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, object := range objects {
			select {
			case <-ctx.Done():
				return
			case jobs <- object:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(results)
	}()

	report := BackfillReport{}
	for result := range results {
		report.Processed++
		if result.err != nil {
			if errors.Is(result.err, ErrSourceIngestInProgress) {
				report.Skipped++

				continue
			}
			report.Failed++
			report.FailedMessageIDs = append(
				report.FailedMessageIDs,
				result.messageID,
			)

			continue
		}
		if result.created {
			report.Created++
		} else {
			report.Skipped++
		}
	}

	return report
}

type backfillIngestResult struct {
	messageID string
	err       error
	created   bool
}

func cloneCursor(cursor map[string]any) map[string]any {
	cloned := map[string]any{}
	for key, value := range cursor {
		cloned[key] = value
	}

	return cloned
}

func namespacedCursor(
	stored contracts.SourceCursor,
	next contracts.SourceCursor,
	key string,
) contracts.SourceCursor {
	wrapped := cloneCursor(stored.Cursor)
	wrapped[key] = cloneCursor(next.Cursor)
	wrapped[key+"_updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	next.Cursor = wrapped

	return next
}

func mapCursorValue(cursor map[string]any, key string) map[string]any {
	value, ok := cursor[key]
	if !ok {
		return nil
	}
	if typed, ok := value.(map[string]any); ok {
		return cloneCursor(typed)
	}
	typed, ok := value.(map[string]string)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(typed))
	for mapKey, mapValue := range typed {
		out[mapKey] = mapValue
	}

	return out
}

func mergeBackfillCounts(
	cursor map[string]any,
	report BackfillReport,
) map[string]any {
	merged := cloneCursor(cursor)
	merged["processed_count"] = report.Processed
	merged["created_count"] = report.Created
	merged["skipped_count"] = report.Skipped
	merged["failed_count"] = report.Failed
	merged["failed_message_ids"] = append([]string{}, report.FailedMessageIDs...)

	return merged
}

func boolCursorValue(cursor map[string]any, key string) bool {
	value, _ := cursor[key].(bool)

	return value
}

func (service *Service) ApplyAction(
	ctx context.Context,
	adapter ActionAdapter,
	request ActionRequest,
) (ActionResult, error) {
	if adapter == nil {
		return ActionResult{}, errors.New("source action adapter is required")
	}

	if !hasCapability(adapter, CapabilityActions) {
		return ActionResult{}, fmt.Errorf(
			"%w: source %s/%s does not support actions",
			ErrUnsupportedOperation,
			adapter.Kind(),
			adapter.Name(),
		)
	}

	if strings.TrimSpace(string(request.ObjectDigest)) == "" {
		return ActionResult{}, errors.New("source action object_digest is required")
	}

	manifest, err := service.store.ReadManifest(ctx, request.ObjectDigest)
	if err != nil {
		return ActionResult{}, err
	}

	provenance, ok := actionProvenance(manifest, adapter)
	if !ok {
		return ActionResult{}, fmt.Errorf(
			"%w: object %s has no provenance for source %s/%s",
			ErrUnsupportedOperation,
			request.ObjectDigest,
			adapter.Kind(),
			adapter.Name(),
		)
	}

	if !actionFacetAllowed(manifest, adapter) {
		return ActionResult{}, fmt.Errorf(
			"%w: object %s facets do not allow %s actions",
			ErrUnsupportedOperation,
			request.ObjectDigest,
			adapter.Kind(),
		)
	}

	parameters := map[string]any{}
	for key, value := range request.Parameters {
		parameters[key] = value
	}
	if adapter.Kind() == "gmail" &&
		strings.TrimSpace(stringValue(parameters["message_id"])) == "" {
		parameters["message_id"] = provenance.ExternalID
	}

	request.Parameters = parameters

	return adapter.ApplyAction(ctx, request)
}

func (service *Service) releaseClaim(
	ctx context.Context,
	claim contracts.SourceIngestClaim,
) {
	_ = service.store.ReleaseSourceIngest(ctx, claim)
}

func sourceRef(object IngestObject) (contracts.SourceObjectRef, bool, error) {
	ref := contracts.SourceObjectRef{
		SourceKind:      strings.TrimSpace(object.SourceKind),
		SourceName:      strings.TrimSpace(object.SourceName),
		ExternalID:      strings.TrimSpace(object.ExternalID),
		ExternalVersion: strings.TrimSpace(object.ExternalVer),
	}
	if ref.SourceKind == "" && ref.SourceName == "" && ref.ExternalID == "" {
		return contracts.SourceObjectRef{}, false, nil
	}

	if ref.SourceKind == "" || ref.SourceName == "" || ref.ExternalID == "" {
		return contracts.SourceObjectRef{}, false, errors.New(
			"source identity requires source_kind, source_name, and external_id",
		)
	}

	return ref, true, nil
}

func provenanceFor(object IngestObject) []contracts.Provenance {
	ref, hasRef, _ := sourceRef(object)
	if !hasRef {
		return nil
	}

	observed := object.ObservedAt
	if observed.IsZero() {
		observed = time.Now().UTC()
	}

	return []contracts.Provenance{{
		SourceKind:       ref.SourceKind,
		SourceName:       ref.SourceName,
		ExternalID:       ref.ExternalID,
		ExternalVersion:  ref.ExternalVersion,
		ObservedAt:       observed,
		CapabilitiesSeen: append([]string{}, object.ContentRoles...),
	}}
}

func mergeProvenance(
	first []contracts.Provenance,
	second []contracts.Provenance,
) []contracts.Provenance {
	merged := append([]contracts.Provenance{}, first...)
	seen := map[string]bool{}
	for _, item := range merged {
		seen[provenanceKey(item)] = true
	}

	for _, item := range second {
		key := provenanceKey(item)
		if !seen[key] {
			merged = append(merged, item)
			seen[key] = true
		}
	}

	return merged
}

func provenanceKey(item contracts.Provenance) string {
	return item.SourceKind + "\x00" +
		item.SourceName + "\x00" +
		item.ExternalID + "\x00" +
		item.ExternalVersion
}

func hasCapability(adapter Adapter, capability string) bool {
	for _, available := range adapter.Capabilities() {
		if available == capability {
			return true
		}
	}

	return false
}

func actionProvenance(
	manifest contracts.Manifest,
	adapter Adapter,
) (contracts.Provenance, bool) {
	for _, provenance := range manifest.Provenance {
		if provenance.SourceKind == adapter.Kind() &&
			provenance.SourceName == adapter.Name() &&
			strings.TrimSpace(provenance.ExternalID) != "" {
			return provenance, true
		}
	}

	return contracts.Provenance{}, false
}

func actionFacetAllowed(manifest contracts.Manifest, adapter Adapter) bool {
	if adapter.Kind() != "gmail" {
		return len(manifest.Facets) > 0
	}

	for _, facet := range manifest.Facets {
		if facet.FacetKind() == "mail_message" {
			return true
		}
	}

	return false
}
