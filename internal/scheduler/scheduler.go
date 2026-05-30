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
	"time"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

type Config struct {
	ScanInterval time.Duration
	RetryBackoff time.Duration
	Priorities   config.SchedulerPriority
}

// Store is the narrow slice of the filestore the scheduler depends on: reading
// projections and writing analysis annotations. Both the in-process
// FilesystemStore and the filestore gRPC client satisfy it, so the scheduler can
// run against the filestore server (the only safe option while filestore-serve
// holds Pebble's exclusive lock) without depending on the whole Store surface.
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
	WriteAnnotation(ctx context.Context, annotation contracts.Annotation) error
}

type Service struct {
	store     Store
	broker    Broker
	projector ProjectionRefresher
	now       func() time.Time
	specs     []contracts.AnalyzerSpec
	config    Config
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

	service := &Service{
		store:  store,
		broker: broker,
		specs:  normalized,
		config: cfg,
		now:    func() time.Time { return time.Now().UTC() },
	}
	if service.config.ScanInterval <= 0 {
		service.config.ScanInterval = 30 * time.Second
	}

	if service.config.RetryBackoff <= 0 {
		service.config.RetryBackoff = 30 * time.Second
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

func (service *Service) Scan(
	ctx context.Context,
	request contracts.SchedulerScanRequest,
) (contracts.SchedulerScanResponse, error) {
	response := contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}
	if err := service.broker.Declare(ctx); err != nil {
		return response, err
	}

	if _, err := service.broker.ProcessFailures(ctx, 100); err != nil {
		return response, err
	}
	activeKeys, err := service.broker.ActiveJobKeys(ctx, 0)
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
				response.Skipped++

				return nil
			}

			schedulerAnnotation := schedulerAnnotationFor(object)
			for _, job := range jobs {
				enqueued, err := service.enqueueForObject(
					ctx,
					job,
					schedulerAnnotation,
					activeKeys,
				)
				if err != nil {
					response.Failed++

					return err
				}

				if enqueued {
					response.Enqueued++
				} else {
					response.Skipped++
				}
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

func (service *Service) enqueueForObject(
	ctx context.Context,
	job contracts.AnalyzerJob,
	schedulerAnnotation *contracts.Annotation,
	activeKeys map[string]bool,
) (bool, error) {
	job = service.normalizeJob(job)
	if activeKeys[job.IdempotencyKey] {
		return false, nil
	}
	if schedulerMarkerActive(
		*schedulerAnnotation,
		job,
		service.now(),
		service.config.RetryBackoff,
	) {
		return false, nil
	}

	updated := withSchedulerMarker(*schedulerAnnotation, job, "publishing", service.now())
	err := service.store.WriteAnnotation(ctx, updated)
	if err != nil {
		return false, err
	}

	*schedulerAnnotation = updated

	err = service.broker.Publish(ctx, job)
	if err != nil {
		return false, err
	}
	activeKeys[job.IdempotencyKey] = true

	updated = withSchedulerMarker(*schedulerAnnotation, job, "enqueued", service.now())
	err = service.store.WriteAnnotation(ctx, updated)
	if err != nil {
		return false, err
	}

	*schedulerAnnotation = updated

	return true, nil
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

	schedulerAnnotation := schedulerAnnotationFor(object)
	activeKeys, err := service.broker.ActiveJobKeys(ctx, 0)
	if err != nil {
		response.Failed++

		return response, err
	}
	for _, job := range service.jobsForObject(object, request) {
		if len(filter) > 0 && !filter[job.Analyzer.Name] {
			continue
		}

		enqueued, err := service.enqueueForObject(
			ctx,
			job,
			schedulerAnnotation,
			activeKeys,
		)
		if err != nil {
			response.Failed++

			return response, err
		}

		if enqueued {
			response.Enqueued++
		} else {
			response.Skipped++
		}
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
	for _, digest := range request.ObjectDigests {
		if strings.TrimSpace(string(digest)) == "" {
			continue
		}

		response.Scanned++
		if err := service.broker.PublishProjectionRefresh(ctx, digest); err != nil {
			response.Failed++

			return response, err
		}

		if request.ProjectionOnly {
			if err := service.markSatisfiedAnalysis(ctx, digest); err != nil {
				response.Failed++

				return response, err
			}
			response.Skipped++

			continue
		}

		object, found, err := service.projectionObjectForDigest(ctx, digest)
		if err != nil {
			response.Failed++

			return response, err
		}
		if !found {
			response.Failed++

			return response, fmt.Errorf("changed object %s not found in filestore", digest)
		}
		if len(object.Findings) > 0 {
			response.Failed++

			continue
		}

		schedulerAnnotation := schedulerAnnotationFor(object)
		activeKeys, err := service.broker.ActiveJobKeys(ctx, 0)
		if err != nil {
			response.Failed++

			return response, err
		}
		scanRequest := contracts.SchedulerScanRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			PriorityClass: request.PriorityClass,
			RequestedBy:   firstNonEmpty(request.RequestedBy, "filestore"),
			Reason:        firstNonEmpty(request.Reason, "object_changed"),
			TraceID:       request.TraceID,
		}
		for _, job := range service.jobsForObject(object, scanRequest) {
			enqueued, err := service.enqueueForObject(
				ctx,
				job,
				schedulerAnnotation,
				activeKeys,
			)
			if err != nil {
				response.Failed++

				return response, err
			}
			if enqueued {
				response.Enqueued++
			} else {
				response.Skipped++
			}
		}
	}

	return response, nil
}

func (service *Service) projectionObjectForDigest(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (filestore.ProjectionObject, bool, error) {
	return service.store.ProjectionObject(ctx, digest)
}

func (service *Service) markSatisfiedAnalysis(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	object, found, err := service.projectionObjectForDigest(ctx, digest)
	if err != nil {
		return err
	}
	if !found || len(object.Findings) > 0 {
		return nil
	}

	updated, changed := withCompletedSchedulerMarkers(
		*schedulerAnnotationFor(object),
		object,
		service.now(),
	)
	if !changed {
		return nil
	}

	return service.store.WriteAnnotation(ctx, updated)
}

func (service *Service) markQueuedJobs(
	ctx context.Context,
	jobs []contracts.AnalyzerJob,
) error {
	byDigest := map[contracts.ObjectDigest][]contracts.AnalyzerJob{}
	for _, job := range jobs {
		if job.ObjectDigest == "" || job.IdempotencyKey == "" {
			continue
		}
		byDigest[job.ObjectDigest] = append(byDigest[job.ObjectDigest], job)
	}

	for digest, digestJobs := range byDigest {
		object, found, err := service.projectionObjectForDigest(ctx, digest)
		if err != nil {
			return err
		}
		if !found || len(object.Findings) > 0 {
			continue
		}

		annotation := schedulerAnnotationFor(object)
		for _, job := range digestJobs {
			*annotation = withSchedulerMarker(
				*annotation,
				service.normalizeJob(job),
				"enqueued",
				service.now(),
			)
		}
		if err := service.store.WriteAnnotation(ctx, *annotation); err != nil {
			return err
		}
	}

	return nil
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
	if err != nil {
		return response, err
	}
	if err := service.markQueuedJobs(ctx, response.KeptJobs); err != nil {
		return response, err
	}

	return response, nil
}

func (service *Service) Status(ctx context.Context) (contracts.SchedulerStatus, error) {
	return service.broker.Status(ctx)
}

func (service *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(service.config.ScanInterval)
	defer ticker.Stop()

	for {
		if _, err := service.broker.ProcessFailures(ctx, 100); err != nil {
			return err
		}

		if service.projector != nil {
			if _, err := service.broker.ProcessProjectionRefreshes(
				ctx,
				100,
				service.refreshProjectionDigests,
			); err != nil {
				return err
			}
		}

		// Self-healing reconciliation: sweep the projection and enqueue analyzer
		// work for every object still missing it. This is what makes analysis
		// converge without operator intervention — an object whose change
		// notification was dropped (e.g. a notify failure during a bulk import)
		// is picked up on the next sweep. Scan only enqueues missing work and
		// dedups against in-flight jobs, so re-running it is cheap and idempotent.
		// A transient scan error is logged, not fatal, so reconciliation keeps
		// running.
		if _, err := service.Scan(ctx, contracts.SchedulerScanRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			RequestedBy:   "scheduler",
			Reason:        "periodic_reconcile",
		}); err != nil {
			slog.Error("scheduler reconcile scan failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (service *Service) refreshProjectionDigests(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) error {
	seen := map[contracts.ObjectDigest]bool{}
	for _, digest := range digests {
		if digest == "" || seen[digest] {
			continue
		}
		seen[digest] = true

		object, found, err := service.projectionObjectForDigest(ctx, digest)
		if err != nil {
			return err
		}
		if !found || len(object.Findings) > 0 {
			continue
		}
		if err := service.projector.ProjectObject(ctx, object); err != nil {
			return err
		}
	}

	return nil
}

func schedulerAnnotationFor(object filestore.ProjectionObject) *contracts.Annotation {
	for _, annotation := range object.Annotations {
		if annotation.Kind == "scheduler" {
			return &contracts.Annotation{
				SchemaVersion: annotation.SchemaVersion,
				ObjectDigest:  object.Manifest.ObjectDigest,
				Kind:          "scheduler",
				GeneratedAt:   annotation.GeneratedAt,
				Data:          copyMap(annotation.Data),
			}
		}
	}

	return &contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  object.Manifest.ObjectDigest,
		Kind:          "scheduler",
		Data:          map[string]any{},
	}
}

func schedulerMarkerActive(
	annotation contracts.Annotation,
	job contracts.AnalyzerJob,
	now time.Time,
	lease time.Duration,
) bool {
	marker, ok := schedulerMarkers(annotation)[job.IdempotencyKey]
	if !ok {
		return false
	}

	status, _ := marker["status"].(string)
	if job.Reason == "repair" && status != "publishing" {
		reason, _ := marker["reason"].(string)

		return reason == "repair"
	}

	switch status {
	case "complete", "dead", "enqueued", "inflight", "retry":
		return true
	case "publishing":
		updatedAt, _ := marker["updated_at"].(string)

		parsed, err := time.Parse(time.RFC3339Nano, updatedAt)
		if err != nil {
			return false
		}

		return now.Sub(parsed) <= lease
	default:
		return false
	}
}

func withSchedulerMarker(
	annotation contracts.Annotation,
	job contracts.AnalyzerJob,
	status string,
	now time.Time,
) contracts.Annotation {
	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	annotation.ObjectDigest = job.ObjectDigest
	annotation.Kind = "scheduler"
	annotation.GeneratedAt = now
	data := copyMap(annotation.Data)
	markers := schedulerMarkers(annotation)
	markers[job.IdempotencyKey] = map[string]any{
		"status":           status,
		"job_id":           job.JobID,
		"analyzer_name":    job.Analyzer.Name,
		"analyzer_version": job.Analyzer.Version,
		"reason":           job.Reason,
		"attempt":          job.Attempt,
		"priority_class":   job.PriorityClass,
		"priority":         job.Priority,
		"updated_at":       now.Format(time.RFC3339Nano),
	}
	data["scheduled_jobs"] = markers
	annotation.Data = data

	return annotation
}

func withCompletedSchedulerMarkers(
	annotation contracts.Annotation,
	object filestore.ProjectionObject,
	now time.Time,
) (contracts.Annotation, bool) {
	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	annotation.ObjectDigest = object.Manifest.ObjectDigest
	annotation.Kind = "scheduler"
	annotation.GeneratedAt = now
	data := copyMap(annotation.Data)
	markers := schedulerMarkers(annotation)
	changed := false
	for _, analysis := range object.Annotations {
		if analysis.Kind != "analysis" ||
			analysis.AnalyzerName == "" ||
			analysis.AnalyzerVer == "" ||
			!analysisOutputSatisfied(analysis) {
			continue
		}

		job := contracts.AnalyzerJob{
			ObjectDigest: object.Manifest.ObjectDigest,
			Analyzer: contracts.AnalyzerSpec{
				Name:    analysis.AnalyzerName,
				Version: analysis.AnalyzerVer,
			},
		}
		key := IdempotencyKey(job)
		marker := markers[key]
		if marker != nil && marker["status"] == "complete" {
			continue
		}

		markers[key] = map[string]any{
			"status":           "complete",
			"job_id":           key,
			"analyzer_name":    analysis.AnalyzerName,
			"analyzer_version": analysis.AnalyzerVer,
			"updated_at":       now.Format(time.RFC3339Nano),
		}
		changed = true
	}
	if !changed {
		return annotation, false
	}

	data["scheduled_jobs"] = markers
	annotation.Data = data

	return annotation, true
}

func analysisOutputSatisfied(annotation contracts.Annotation) bool {
	status, _ := annotation.Data["status"].(string)

	switch status {
	case "complete", "skipped":
		return true
	default:
		return false
	}
}

func schedulerMarkers(annotation contracts.Annotation) map[string]map[string]any {
	result := map[string]map[string]any{}

	raw, ok := annotation.Data["scheduled_jobs"].(map[string]any)
	if !ok {
		return result
	}

	for key, value := range raw {
		if marker, ok := value.(map[string]any); ok {
			result[key] = copyMap(marker)
		}
	}

	return result
}

func copyMap(input map[string]any) map[string]any {
	output := map[string]any{}
	for key, value := range input {
		output[key] = value
	}

	return output
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
