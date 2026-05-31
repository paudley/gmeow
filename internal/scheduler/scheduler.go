// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

const (
	objectChangeProcessLimit = 100
	schedulerWorkerCount     = 3
	failureProcessLimit      = 100
)

var errSweepChunkDone = errors.New("sweep chunk limit reached")

type Config struct {
	ScanInterval            time.Duration
	RetryBackoff            time.Duration
	SelfHealIdleThreshold   time.Duration
	Priorities              config.SchedulerPriority
	BackpressureHighWater   int
	BackpressureLowWater    int
	SelfHealChunkSize       int
	FullyAnnotatedCacheSize int
}

// Store is the narrow slice of the filestore the scheduler reads. The scheduler
// never writes to FILESTORE — it is a read-only coordinator. Both the in-process
// FilesystemStore and the filestore gRPC client satisfy it.
type Store interface {
	WalkProjection(ctx context.Context, fn filestore.ProjectionFunc) error
	ProjectionObject(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (filestore.ProjectionObject, bool, error)
	HasAnalysisAnnotation(
		ctx context.Context,
		digest contracts.ObjectDigest,
		analyzerName string,
		analyzerVersion string,
	) (bool, error)
}

type Service struct {
	store     Store
	broker    Broker
	projector ProjectionRefresher
	now       func() time.Time
	specs     []contracts.AnalyzerSpec
	config    Config

	fullyAnnotated *lruCache

	mu        sync.Mutex
	pressured bool
	writeSeen bool
	sweepPos  string
}

type Option func(*Service)

func NewService(
	store Store,
	broker Broker,
	specs []contracts.AnalyzerSpec,
	cfg Config,
	options ...Option,
) (*Service, error) {
	if store == nil {
		return nil, errors.New("scheduler filestore is required")
	}

	if broker == nil {
		return nil, errors.New("scheduler broker is required")
	}

	normalized, err := NormalizeSpecs(specs)
	if err != nil {
		return nil, err
	}

	cacheSize := cfg.FullyAnnotatedCacheSize
	if cacheSize <= 0 {
		cacheSize = 100000
	}

	service := &Service{
		store:          store,
		broker:         broker,
		specs:          normalized,
		config:         cfg,
		now:            func() time.Time { return time.Now().UTC() },
		fullyAnnotated: newLRUCache(cacheSize),
	}

	if service.config.ScanInterval <= 0 {
		service.config.ScanInterval = 30 * time.Second
	}

	if service.config.RetryBackoff <= 0 {
		service.config.RetryBackoff = 30 * time.Second
	}

	if service.config.BackpressureHighWater <= 0 {
		service.config.BackpressureHighWater = 10000
	}

	if service.config.BackpressureLowWater <= 0 {
		service.config.BackpressureLowWater = 5000
	}

	if service.config.BackpressureLowWater >= service.config.BackpressureHighWater {
		return nil, fmt.Errorf(
			"backpressure low-water (%d) must be below high-water (%d)",
			service.config.BackpressureLowWater,
			service.config.BackpressureHighWater,
		)
	}

	if service.config.SelfHealChunkSize <= 0 {
		service.config.SelfHealChunkSize = 500
	}

	if service.config.SelfHealIdleThreshold <= 0 {
		service.config.SelfHealIdleThreshold = 5 * time.Minute
	}

	for _, option := range options {
		option(service)
	}

	return service, nil
}

func WithProjector(projector ProjectionRefresher) Option {
	return func(service *Service) {
		service.projector = projector
	}
}

func WithClock(now func() time.Time) Option {
	return func(service *Service) {
		if now != nil {
			service.now = now
		}
	}
}

func SpecsFromConfig(analyzers []config.AnalyzerConfig) []contracts.AnalyzerSpec {
	specs := make([]contracts.AnalyzerSpec, 0, len(analyzers))
	for _, analyzer := range analyzers {
		spec := contracts.AnalyzerSpec{
			Name:          analyzer.Name,
			Version:       analyzer.Version,
			WorkerKind:    analyzer.WorkerKind,
			MediaTypes:    append([]string(nil), analyzer.MediaTypes...),
			Deterministic: true,
		}
		specs = append(specs, withKnownAnalyzerConstraints(spec))
	}

	return specs
}

func withKnownAnalyzerConstraints(spec contracts.AnalyzerSpec) contracts.AnalyzerSpec {
	if len(spec.MediaTypes) > 0 {
		return spec
	}

	switch spec.Name {
	case "text.extract":
		spec.MediaTypes = []string{
			"text/plain",
			"text/html",
			"message/rfc822",
			"application/json",
		}
	case "rfc822.headers":
		spec.MediaTypes = []string{"message/rfc822", "text/rfc822-headers"}
	}

	return spec
}

func NormalizeSpecs(specs []contracts.AnalyzerSpec) ([]contracts.AnalyzerSpec, error) {
	normalized := make([]contracts.AnalyzerSpec, 0, len(specs))
	seen := map[string]bool{}

	for _, spec := range specs {
		spec.Name = strings.TrimSpace(spec.Name)

		spec.Version = strings.TrimSpace(spec.Version)
		if spec.Name == "" {
			return nil, errors.New("analyzer spec name is required")
		}

		if spec.Version == "" {
			return nil, fmt.Errorf("analyzer %q version is required", spec.Name)
		}

		key := spec.Name + "\x00" + spec.Version
		if seen[key] {
			continue
		}

		seen[key] = true

		normalized = append(normalized, spec)
	}

	sort.SliceStable(normalized, func(left, right int) bool {
		return normalized[left].Name < normalized[right].Name
	})

	return normalized, nil
}

// Scan walks the entire projection and enqueues missing analysis work. This is
// an explicit operator/admin operation or the bounded self-heal backstop — not
// the normal driver of analysis work.
func (service *Service) Scan(
	ctx context.Context,
	request contracts.SchedulerScanRequest,
) (contracts.SchedulerScanResponse, error) {
	response := contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}

	err := service.broker.Declare(ctx)
	if err != nil {
		return response, err
	}

	_, err = service.broker.ProcessFailures(ctx, 100)
	if err != nil {
		return response, err
	}

	err = service.store.WalkProjection(
		ctx,
		func(object filestore.ProjectionObject) error {
			response.Scanned++
			if len(object.Findings) > 0 {
				response.Failed++

				return nil
			}

			jobs := service.jobsForObject(object, request)
			if len(jobs) == 0 {
				service.fullyAnnotated.Add(string(object.Manifest.ObjectDigest))
				response.Skipped++

				return nil
			}

			for _, job := range jobs {
				job = service.normalizeJob(job)

				err := service.broker.Publish(ctx, job)
				if err != nil {
					response.Failed++

					return err
				}

				response.Enqueued++
			}

			err := service.broker.PublishProjectionRefresh(
				ctx,
				object.Manifest.ObjectDigest,
			)
			if err != nil {
				response.Failed++

				return err
			}

			return nil
		},
	)

	return response, err
}

func (service *Service) Enqueue(ctx context.Context, job contracts.AnalyzerJob) error {
	job = service.normalizeJob(job)

	return service.broker.Publish(ctx, job)
}

func (service *Service) normalizeJob(job contracts.AnalyzerJob) contracts.AnalyzerJob {
	if job.SchemaVersion == 0 {
		job.SchemaVersion = contracts.SchemaVersionPhase00
	}

	if job.CreatedAt.IsZero() {
		job.CreatedAt = service.now()
	}

	if job.IdempotencyKey == "" {
		job.IdempotencyKey = IdempotencyKey(job)
	}

	if job.JobID == "" {
		job.JobID = job.IdempotencyKey
	}

	if job.PriorityClass == "" {
		job.PriorityClass = contracts.PriorityBackground
	}

	if job.Priority == 0 {
		job.Priority = service.priorityValue(job.PriorityClass)
	}

	return job
}

func (service *Service) Force(
	ctx context.Context,
	digest contracts.ObjectDigest,
	analyzerNames []string,
	requestedBy string,
	traceID string,
) (contracts.SchedulerScanResponse, error) {
	object, found, err := service.store.ProjectionObject(ctx, digest)
	if err != nil {
		return contracts.SchedulerScanResponse{}, err
	}

	if !found {
		return contracts.SchedulerScanResponse{}, fmt.Errorf(
			"object %s not found",
			digest,
		)
	}

	filter := map[string]bool{}
	for _, name := range analyzerNames {
		filter[name] = true
	}

	response := contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Scanned:       1,
	}
	request := contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityForced,
		RequestedBy:   requestedBy,
		Reason:        "forced",
		Forced:        true,
		TraceID:       traceID,
	}

	service.fullyAnnotated.Remove(string(digest))

	for _, job := range service.jobsForObject(object, request) {
		if len(filter) > 0 && !filter[job.Analyzer.Name] {
			continue
		}

		err = service.broker.Publish(ctx, service.normalizeJob(job))
		if err != nil {
			response.Failed++

			return response, err
		}

		response.Enqueued++
	}

	if response.Enqueued == 0 && response.Skipped == 0 {
		response.Skipped = 1
	}

	return response, nil
}

func (service *Service) NotifyObjectsChanged(
	ctx context.Context,
	request contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	response := contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}

	normalized := request
	normalized.SchemaVersion = contracts.SchemaVersionPhase00
	normalized.ObjectDigests = make(
		[]contracts.ObjectDigest,
		0,
		len(request.ObjectDigests),
	)

	for _, digest := range request.ObjectDigests {
		if strings.TrimSpace(string(digest)) == "" {
			continue
		}

		normalized.ObjectDigests = append(normalized.ObjectDigests, digest)
		response.Scanned++
	}

	if len(normalized.ObjectDigests) == 0 {
		return response, nil
	}

	err := service.broker.PublishObjectChanges(ctx, normalized)
	if err != nil {
		response.Failed = response.Scanned

		return response, fmt.Errorf("publish object change notification: %w", err)
	}

	response.Enqueued = len(normalized.ObjectDigests)

	service.noteWrite()

	return response, nil
}

func (service *Service) Requeue(
	ctx context.Context,
	request contracts.RequeueRequest,
) (contracts.RequeueResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	requeued, err := service.broker.RequeueDeadLetters(ctx, limit)

	return contracts.RequeueResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Requeued:      requeued,
	}, err
}

func (service *Service) ProcessFailures(
	ctx context.Context,
	request contracts.RequeueRequest,
) (contracts.RequeueResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	processed, err := service.broker.ProcessFailures(ctx, limit)

	return contracts.RequeueResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Requeued:      processed,
	}, err
}

func (service *Service) DeadLetters(
	ctx context.Context,
	request contracts.DeadLetterRequest,
) (contracts.DeadLetterResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	jobs, err := service.broker.DeadLetters(ctx, limit)

	return contracts.DeadLetterResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Jobs:          jobs,
	}, err
}

func (service *Service) FailedJobs(
	ctx context.Context,
	request contracts.DeadLetterRequest,
) (contracts.DeadLetterResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	jobs, err := service.broker.FailedJobs(ctx, limit)

	return contracts.DeadLetterResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Jobs:          jobs,
	}, err
}

func (service *Service) PendingJobs(
	ctx context.Context,
	request contracts.DeadLetterRequest,
) (contracts.DeadLetterResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	jobs, err := service.broker.PendingJobs(ctx, limit)

	return contracts.DeadLetterResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Jobs:          jobs,
	}, err
}

func (service *Service) ReconcilePending(
	ctx context.Context,
	request contracts.RequeueRequest,
) (contracts.ReconcilePendingResponse, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}

	response, err := service.broker.ReconcilePending(
		ctx,
		limit,
		func(job contracts.AnalyzerJob) (bool, error) {
			return service.store.HasAnalysisAnnotation(
				ctx,
				job.ObjectDigest,
				job.Analyzer.Name,
				job.Analyzer.Version,
			)
		},
	)
	response.SchemaVersion = contracts.SchemaVersionPhase00

	return response, err
}

func (service *Service) Status(ctx context.Context) (contracts.SchedulerStatus, error) {
	return service.broker.Status(ctx)
}

// AnalyzerStatus reports the work and retry depth of each analyzer's own queue,
// so an operator can see which analyzer is backed up rather than only the total.
func (service *Service) AnalyzerStatus(
	ctx context.Context,
) ([]contracts.AnalyzerQueueDepth, error) {
	return service.broker.PerAnalyzerStatus(ctx)
}

func (service *Service) PurgeQueues(ctx context.Context) (int, error) {
	return service.broker.PurgeAll(ctx)
}

// Pressured reports whether the scheduler's analysis queue depth exceeds the
// backpressure high-water mark. Sources should pause backfill scheduling when
// this returns true and resume when it returns false (below low-water).
func (service *Service) Pressured() bool {
	service.mu.Lock()
	defer service.mu.Unlock()

	return service.pressured
}

func (service *Service) updatePressure(ctx context.Context) {
	status, err := service.broker.Status(ctx)
	if err != nil {
		slog.Error("scheduler pressure check failed", "error", err)

		return
	}

	depth := status.Pending + status.Retry

	service.mu.Lock()
	defer service.mu.Unlock()

	if service.pressured && depth <= service.config.BackpressureLowWater {
		service.pressured = false
		slog.Info("scheduler backpressure released",
			"depth", depth,
			"low_water", service.config.BackpressureLowWater,
		)
	} else if !service.pressured && depth >= service.config.BackpressureHighWater {
		service.pressured = true
		slog.Warn("scheduler backpressure engaged",
			"depth", depth,
			"high_water", service.config.BackpressureHighWater,
		)
	}
}

func (service *Service) noteWrite() {
	service.mu.Lock()
	defer service.mu.Unlock()

	service.writeSeen = true
}

func (service *Service) consumeWriteSeen() bool {
	service.mu.Lock()
	defer service.mu.Unlock()

	seen := service.writeSeen
	service.writeSeen = false

	return seen
}

// Run starts the scheduler's background workers. Queue workers block on RabbitMQ
// delivery streams and process work as fast as RabbitMQ and downstream IO allow.
// The only periodic worker is the low-priority self-heal backstop.
func (service *Service) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, schedulerWorkerCount)

	go func() {
		errc <- service.runFailureQueue(runCtx)
	}()
	go func() {
		errc <- service.runObjectChangeQueue(runCtx)
	}()
	go func() {
		errc <- service.runSelfHeal(runCtx)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("run scheduler: %w", ctx.Err())
	case err := <-errc:
		cancel()

		ctxErr := ctx.Err()
		if ctxErr != nil {
			return fmt.Errorf("run scheduler: %w", ctxErr)
		}

		return err
	}
}

// SelfHealSweep runs a full self-heal sweep for admin/operator use. It resets
// the cursor and walks the entire projection in bounded chunks.
func (service *Service) SelfHealSweep(
	ctx context.Context,
) (contracts.SchedulerScanResponse, error) {
	service.mu.Lock()
	service.sweepPos = ""
	service.mu.Unlock()

	return service.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityRepair,
		RequestedBy:   "scheduler",
		Reason:        "self_heal_sweep",
	})
}

// selfHealChunk walks a bounded chunk of the projection from the current cursor
// position, enqueuing missing analysis work. The cursor advances through the
// corpus in chunks and restarts from the beginning once it reaches the end, so
// every object is visited each cycle (re-visits are cheap: the annotation cache
// and worker idempotency make an already-analyzed object a no-op).
func (service *Service) selfHealChunk(ctx context.Context) {
	service.mu.Lock()
	startPos := service.sweepPos
	service.mu.Unlock()

	count := 0
	chunkSize := service.config.SelfHealChunkSize
	started := false
	lastDigest := ""

	scanRequest := contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityRepair,
		RequestedBy:   "scheduler",
		Reason:        "self_heal",
	}

	walkErr := service.store.WalkProjection(
		ctx,
		func(object filestore.ProjectionObject) error {
			digest := string(object.Manifest.ObjectDigest)

			if !started {
				if digest >= startPos {
					started = true
				} else {
					return nil
				}
			}

			if count >= chunkSize {
				return errSweepChunkDone
			}

			count++
			lastDigest = digest

			if len(object.Findings) > 0 {
				return nil
			}

			jobs := service.jobsForObject(object, scanRequest)
			if len(jobs) == 0 {
				service.fullyAnnotated.Add(digest)

				return nil
			}

			for _, job := range jobs {
				job = service.normalizeJob(job)

				publishErr := service.broker.Publish(ctx, job)
				if publishErr != nil {
					return publishErr
				}
			}

			return service.broker.PublishProjectionRefresh(ctx, object.Manifest.ObjectDigest)
		},
	)

	if walkErr != nil && !errors.Is(walkErr, errSweepChunkDone) {
		slog.Error("self-heal chunk failed", "error", walkErr)
	}

	service.mu.Lock()
	if count < chunkSize {
		// Fewer than a full chunk means the walk reached the end of the corpus;
		// reset the cursor so the next sweep restarts from a fresh random position.
		service.sweepPos = ""
	} else {
		service.sweepPos = lastDigest
	}
	service.mu.Unlock()

	if count > 0 {
		slog.Info("self-heal chunk completed", "objects", count, "cursor", lastDigest)
	}
}

func (service *Service) ProcessObjectChanges(
	ctx context.Context,
	limit int,
) (int, error) {
	processed, err := service.broker.ProcessObjectChanges(
		ctx,
		limit,
		service.processObjectChanges,
	)
	if err != nil {
		return processed, fmt.Errorf("process object changes: %w", err)
	}

	return processed, nil
}

func (service *Service) runFailureQueue(ctx context.Context) error {
	for {
		_, err := service.broker.ProcessFailures(ctx, failureProcessLimit)
		if err != nil {
			return fmt.Errorf("process failure queue: %w", err)
		}
	}
}

func (service *Service) runObjectChangeQueue(ctx context.Context) error {
	for {
		_, err := service.ProcessObjectChanges(ctx, objectChangeProcessLimit)
		if err != nil {
			return fmt.Errorf("process object change queue: %w", err)
		}
	}
}

func (service *Service) runSelfHeal(ctx context.Context) error {
	ticker := time.NewTicker(service.config.ScanInterval)
	defer ticker.Stop()

	idleSince := time.Time{}
	sweepArmed := false

	for {
		service.updatePressure(ctx)

		if service.consumeWriteSeen() {
			sweepArmed = true
			idleSince = time.Time{}
		}

		if sweepArmed && !service.Pressured() {
			if idleSince.IsZero() {
				idleSince = service.now()
			}

			if service.now().Sub(idleSince) >= service.config.SelfHealIdleThreshold {
				service.selfHealChunk(ctx)

				sweepArmed = false
				idleSince = time.Time{}
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("run self-heal: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (service *Service) processObjectChanges(
	ctx context.Context,
	requests []contracts.ObjectChangeRequest,
) error {
	projected := map[contracts.ObjectDigest]bool{}

	for _, request := range requests {
		err := service.processObjectChangeRequest(ctx, request, projected)
		if err != nil {
			return err
		}
	}

	return nil
}

func (service *Service) processObjectChangeRequest(
	ctx context.Context,
	request contracts.ObjectChangeRequest,
	projected map[contracts.ObjectDigest]bool,
) error {
	scanRequest := objectChangeScanRequest(request)

	for _, digest := range request.ObjectDigests {
		if strings.TrimSpace(string(digest)) == "" {
			continue
		}

		err := service.processObjectChangeDigest(
			ctx,
			digest,
			request,
			scanRequest,
			projected,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func objectChangeScanRequest(
	request contracts.ObjectChangeRequest,
) contracts.SchedulerScanRequest {
	return contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: request.PriorityClass,
		RequestedBy:   firstNonEmpty(request.RequestedBy, "filestore"),
		Reason:        firstNonEmpty(request.Reason, "object_changed"),
		TraceID:       request.TraceID,
	}
}

func (service *Service) processObjectChangeDigest(
	ctx context.Context,
	digest contracts.ObjectDigest,
	request contracts.ObjectChangeRequest,
	scanRequest contracts.SchedulerScanRequest,
	projected map[contracts.ObjectDigest]bool,
) error {
	object, found, err := service.store.ProjectionObject(ctx, digest)
	if err != nil {
		return fmt.Errorf("load changed object %s: %w", digest, err)
	}

	if !found || len(object.Findings) > 0 {
		return nil
	}

	err = service.projectObjectChange(ctx, object, projected)
	if err != nil {
		return err
	}

	if request.ProjectionOnly || service.fullyAnnotated.Contains(string(digest)) {
		return nil
	}

	return service.enqueueObjectChangeAnalysis(ctx, object, scanRequest)
}

func (service *Service) projectObjectChange(
	ctx context.Context,
	object filestore.ProjectionObject,
	projected map[contracts.ObjectDigest]bool,
) error {
	digest := object.Manifest.ObjectDigest
	if service.projector == nil || projected[digest] {
		return nil
	}

	err := service.projector.ProjectObject(ctx, object)
	if err != nil {
		return fmt.Errorf("project changed object %s: %w", digest, err)
	}

	projected[digest] = true

	return nil
}

func (service *Service) enqueueObjectChangeAnalysis(
	ctx context.Context,
	object filestore.ProjectionObject,
	scanRequest contracts.SchedulerScanRequest,
) error {
	digest := object.Manifest.ObjectDigest

	jobs := service.jobsForObject(object, scanRequest)
	if len(jobs) == 0 {
		service.fullyAnnotated.Add(string(digest))

		return nil
	}

	for _, job := range jobs {
		err := service.broker.Publish(ctx, service.normalizeJob(job))
		if err != nil {
			return fmt.Errorf(
				"publish analysis job for changed object %s: %w",
				digest,
				err,
			)
		}
	}

	service.noteWrite()

	return nil
}

func (service *Service) jobsForObject(
	object filestore.ProjectionObject,
	request contracts.SchedulerScanRequest,
) []contracts.AnalyzerJob {
	jobs := []contracts.AnalyzerJob{}

	annotations := analysisAnnotationsByName(object.Annotations)
	for _, spec := range service.specs {
		if !specMatchesObject(spec, object.Manifest) {
			continue
		}

		reason := analysisReason(spec, annotations[spec.Name], request)
		if reason == "" {
			continue
		}

		job := contracts.AnalyzerJob{
			SchemaVersion: contracts.SchemaVersionPhase00,
			Analyzer:      spec,
			ObjectDigest:  object.Manifest.ObjectDigest,
			PriorityClass: firstNonEmpty(request.PriorityClass, priorityClassForReason(reason)),
			RequestedBy:   firstNonEmpty(request.RequestedBy, "scheduler"),
			Reason:        firstNonEmpty(request.Reason, reason),
			Forced:        request.Forced,
			TraceID:       request.TraceID,
			CreatedAt:     service.now(),
		}
		job.Priority = service.priorityValue(job.PriorityClass)
		job.IdempotencyKey = IdempotencyKey(job)
		job.JobID = job.IdempotencyKey
		jobs = append(jobs, job)
	}

	return jobs
}

func analysisAnnotationsByName(
	annotations []contracts.Annotation,
) map[string]contracts.Annotation {
	result := map[string]contracts.Annotation{}

	for _, annotation := range annotations {
		if annotation.Kind == "analysis" && annotation.AnalyzerName != "" {
			result[annotation.AnalyzerName] = annotation
		}
	}

	return result
}

func analysisReason(
	spec contracts.AnalyzerSpec,
	annotation contracts.Annotation,
	request contracts.SchedulerScanRequest,
) string {
	if request.Forced {
		return "forced"
	}

	if annotation.AnalyzerName == "" {
		return "missing"
	}

	if annotation.AnalyzerVer != spec.Version {
		return "stale"
	}

	if status, ok := annotation.Data["status"].(string); ok {
		switch status {
		case "failed", "error", "invalid":
			return "repair"
		}
	}

	return ""
}

func specMatchesObject(spec contracts.AnalyzerSpec, manifest contracts.Manifest) bool {
	if len(spec.MediaTypes) > 0 && !containsString(spec.MediaTypes, manifest.MediaType) {
		return false
	}

	if len(spec.ContentRoles) > 0 &&
		!containsAny(spec.ContentRoles, manifest.ContentRoles) {
		return false
	}

	return true
}

func IdempotencyKey(job contracts.AnalyzerJob) string {
	parts := []string{
		string(job.ObjectDigest),
		job.Analyzer.Name,
		job.Analyzer.Version,
	}
	if job.Forced {
		parts = append(parts, "forced", firstNonEmpty(job.TraceID, "forced"))
	}

	input := strings.Join(parts, "\x00")
	sum := sha256.Sum256([]byte(input))

	return hex.EncodeToString(sum[:])
}

func priorityClassForReason(reason string) string {
	switch reason {
	case "forced":
		return contracts.PriorityForced
	case "repair":
		return contracts.PriorityRepair
	default:
		return contracts.PriorityBackground
	}
}

func (service *Service) priorityValue(priorityClass string) int {
	switch priorityClass {
	case contracts.PriorityInteractive:
		return service.config.Priorities.Interactive
	case contracts.PriorityForced:
		return service.config.Priorities.Forced
	case contracts.PriorityFreshIngest:
		return service.config.Priorities.FreshIngest
	case contracts.PriorityRepair:
		return service.config.Priorities.Repair
	default:
		return service.config.Priorities.Background
	}
}

func containsAny(wanted, available []string) bool {
	for _, value := range wanted {
		if containsString(available, value) {
			return true
		}
	}

	return false
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
			return strings.TrimSpace(value)
		}
	}

	return ""
}

var _ Scheduler = (*Service)(nil)
