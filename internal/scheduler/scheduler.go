// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

type Service struct {
	store     filestore.Store
	broker    Broker
	projector ProjectionRefresher
	now       func() time.Time
	specs     []contracts.AnalyzerSpec
	config    Config
}

type Option func(*Service)

func NewService(
	store filestore.Store,
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
		specs = append(specs, contracts.AnalyzerSpec{
			Name:          analyzer.Name,
			Version:       analyzer.Version,
			WorkerKind:    analyzer.WorkerKind,
			MediaTypes:    append([]string(nil), analyzer.MediaTypes...),
			Deterministic: true,
		})
	}

	return specs
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

	err := service.store.WalkProjection(
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
				enqueued, err := service.enqueueForObject(ctx, job, schedulerAnnotation)
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
) (bool, error) {
	job = service.normalizeJob(job)
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
	manifest, err := service.store.ReadManifest(ctx, digest)
	if err != nil {
		return contracts.SchedulerScanResponse{}, err
	}

	object := filestore.ProjectionObject{
		Digest:   digest,
		Manifest: manifest,
	}

	if err := service.store.WalkProjection(
		ctx,
		func(candidate filestore.ProjectionObject) error {
			if candidate.Manifest.ObjectDigest == digest {
				object = candidate
			}

			return nil
		},
	); err != nil {
		return contracts.SchedulerScanResponse{}, err
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
	for _, job := range service.jobsForObject(object, request) {
		if len(filter) > 0 && !filter[job.Analyzer.Name] {
			continue
		}

		enqueued, err := service.enqueueForObject(
			ctx,
			job,
			schedulerAnnotation,
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

func (service *Service) Status(ctx context.Context) (contracts.SchedulerStatus, error) {
	return service.broker.Status(ctx)
}

func (service *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(service.config.ScanInterval)
	defer ticker.Stop()

	for {
		if _, err := service.Scan(ctx, contracts.SchedulerScanRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			PriorityClass: contracts.PriorityBackground,
			RequestedBy:   "scheduler",
			Reason:        "background_scan",
		}); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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

	if job.Priority > markerPriority(marker, job.Priority) {
		return false
	}

	status, _ := marker["status"].(string)
	switch status {
	case "enqueued":
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

func markerPriority(marker map[string]any, fallback int) int {
	switch value := marker["priority"].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return fallback
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
	input := strings.Join([]string{
		string(job.ObjectDigest),
		job.Analyzer.Name,
		job.Analyzer.Version,
	}, "\x00")
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
