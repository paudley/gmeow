// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/source"
)

const MailMessageFacet = "mail_message"

type Services struct {
	query     QueryReader
	objects   ObjectReader
	scheduler SchedulerClient
	sources   SourceRegistry
	ingest    SourceIngestService
}

type Options struct {
	Query     QueryReader
	Objects   ObjectReader
	Scheduler SchedulerClient
	Sources   SourceRegistry
	Ingest    SourceIngestService
}

type SearchOptions struct {
	Query         string   `json:"query"`
	Facets        []string `json:"facets,omitempty"`
	AnalyzerNames []string `json:"analyzer_names,omitempty"`
	MediaTypes    []string `json:"media_types,omitempty"`
	Limit         int      `json:"limit,omitempty"`
	Offset        int      `json:"offset,omitempty"`
	FullMessage   bool     `json:"full_msg,omitempty"`
}

type ObjectSearchResponse struct {
	Results []ObjectSearchResult `json:"results"`
	Total   int                  `json:"total"`
}

type ObjectSearchResult struct {
	Attributes        map[string]any         `json:"attributes,omitempty"`
	Structure         *contracts.Structure   `json:"structure,omitempty"`
	Provenance        []contracts.Provenance `json:"provenance,omitempty"`
	ObjectDigest      contracts.ObjectDigest `json:"object_digest"`
	Title             string                 `json:"title,omitempty"`
	Snippet           string                 `json:"snippet,omitempty"`
	Facets            []string               `json:"facets,omitempty"`
	AnalysisPending   bool                   `json:"analysis_pending,omitempty"`
	ProjectionPending bool                   `json:"projection_pending,omitempty"`
	Score             float64                `json:"score,omitempty"`
}

type RetrieveResponse struct {
	Manifest contracts.Manifest `json:"manifest"`
	Content  string             `json:"content,omitempty"`
}

type ForceAnalysisRequest struct {
	Digest      contracts.ObjectDigest `json:"digest"`
	Analyzers   []string               `json:"analyzers,omitempty"`
	RequestedBy string                 `json:"requested_by,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
}

type SourceActionRequest struct {
	Parameters map[string]any         `json:"parameters,omitempty"`
	Digest     contracts.ObjectDigest `json:"digest"`
	Action     string                 `json:"action"`
}

type OpsStatusResponse struct {
	Scheduler contracts.SchedulerStatus      `json:"scheduler"`
	Cursors   contracts.SourceCursorResponse `json:"source_cursors"`
	Errors    map[string]string              `json:"errors,omitempty"`
	Counts    map[string]int                 `json:"counts,omitempty"`
	Metadata  map[string]any                 `json:"metadata,omitempty"`
}

type StaticSourceRegistry struct {
	adapters []source.Adapter
}

func New(options Options) (*Services, error) {
	if options.Query == nil {
		return nil, errors.New("appsvc query reader is required")
	}
	if options.Objects == nil {
		return nil, errors.New("appsvc object reader is required")
	}

	return &Services{
		query:     options.Query,
		objects:   options.Objects,
		scheduler: options.Scheduler,
		sources:   options.Sources,
		ingest:    options.Ingest,
	}, nil
}

func NewStaticSourceRegistry(adapters ...source.Adapter) *StaticSourceRegistry {
	return &StaticSourceRegistry{adapters: append([]source.Adapter{}, adapters...)}
}

func (registry *StaticSourceRegistry) LiveSearchBackends(facet string) []source.LiveSearchAdapter {
	if registry == nil {
		return nil
	}

	backends := []source.LiveSearchAdapter{}
	for _, adapter := range registry.adapters {
		live, ok := adapter.(source.LiveSearchAdapter)
		if !ok || !hasCapability(adapter, source.CapabilityLiveSearch) {
			continue
		}
		if facet == MailMessageFacet && adapter.Kind() != "gmail" {
			continue
		}

		backends = append(backends, live)
	}

	return backends
}

func (registry *StaticSourceRegistry) ActionBackend(kind, name string) (source.ActionAdapter, bool) {
	if registry == nil {
		return nil, false
	}

	for _, adapter := range registry.adapters {
		action, ok := adapter.(source.ActionAdapter)
		if !ok || !hasCapability(adapter, source.CapabilityActions) {
			continue
		}
		if adapter.Kind() == kind && adapter.Name() == name {
			return action, true
		}
	}

	return nil, false
}

func (services *Services) ObjectSearch(
	ctx context.Context,
	options SearchOptions,
) (ObjectSearchResponse, error) {
	response, err := services.query.Search(ctx, searchRequest(options))
	if err != nil {
		return ObjectSearchResponse{}, err
	}

	results, err := services.expandSearchResults(ctx, response.Results, options.FullMessage)
	if err != nil {
		return ObjectSearchResponse{}, err
	}

	return ObjectSearchResponse{Results: results, Total: response.Total}, nil
}

func (services *Services) MailSearch(
	ctx context.Context,
	options SearchOptions,
) (ObjectSearchResponse, error) {
	options.Facets = ensureFacet(options.Facets, MailMessageFacet)

	type backendResult struct {
		results []ObjectSearchResult
		err     error
	}

	channel := make(chan backendResult, 2)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		response, err := services.ObjectSearch(ctx, options)
		channel <- backendResult{results: response.Results, err: err}
	}()

	if services.sources != nil {
		for _, backend := range services.sources.LiveSearchBackends(MailMessageFacet) {
			wait.Add(1)
			go func(backend source.LiveSearchAdapter) {
				defer wait.Done()
				results, err := services.liveMailSearch(ctx, backend, options)
				channel <- backendResult{results: results, err: err}
			}(backend)
		}
	}

	go func() {
		wait.Wait()
		close(channel)
	}()

	fused := map[contracts.ObjectDigest]ObjectSearchResult{}
	var firstErr error
	for item := range channel {
		if item.err != nil && firstErr == nil {
			firstErr = item.err
			continue
		}
		for _, result := range item.results {
			existing, ok := fused[result.ObjectDigest]
			if !ok || result.Score > existing.Score {
				fused[result.ObjectDigest] = mergeSearchResult(existing, result)
			}
		}
	}
	if firstErr != nil && len(fused) == 0 {
		return ObjectSearchResponse{}, firstErr
	}

	results := make([]ObjectSearchResult, 0, len(fused))
	for _, result := range fused {
		results = append(results, result)
	}
	sort.SliceStable(results, func(left, right int) bool {
		return results[left].Score > results[right].Score
	})
	if options.Limit > 0 && len(results) > options.Limit {
		results = results[:options.Limit]
	}

	return ObjectSearchResponse{Results: results, Total: len(results)}, nil
}

func (services *Services) Retrieve(
	ctx context.Context,
	digest contracts.ObjectDigest,
	includeContent bool,
) (RetrieveResponse, error) {
	manifest, err := services.objects.ReadManifest(ctx, digest)
	if err != nil {
		return RetrieveResponse{}, err
	}

	response := RetrieveResponse{Manifest: manifest}
	if includeContent {
		content, err := readObjectContent(ctx, services.objects, digest)
		if err != nil {
			return RetrieveResponse{}, err
		}
		response.Content = content
	}

	return response, nil
}

func (services *Services) Structure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	structure, err := services.objects.GetStructure(ctx, digest)
	if err == nil {
		return structure, nil
	}

	return services.query.Structure(ctx, digest)
}

func (services *Services) Provenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
) ([]contracts.Provenance, error) {
	manifest, err := services.objects.ReadManifest(ctx, digest)
	if err != nil {
		return nil, err
	}

	return append([]contracts.Provenance{}, manifest.Provenance...), nil
}

func (services *Services) Facets(
	ctx context.Context,
	digest contracts.ObjectDigest,
) ([]contracts.Facet, error) {
	manifest, err := services.objects.ReadManifest(ctx, digest)
	if err != nil {
		return nil, err
	}

	return append([]contracts.Facet{}, manifest.Facets...), nil
}

func (services *Services) Compound(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Compound, error) {
	manifest, err := services.objects.ReadManifest(ctx, digest)
	if err != nil {
		return contracts.Compound{}, err
	}

	return manifest.Compound, nil
}

func (services *Services) GraphExplore(
	ctx context.Context,
	request contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return services.query.Graph(ctx, request)
}

func (services *Services) AnalysisStatus(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return services.query.AnalysisStatus(ctx, request)
}

func (services *Services) ForceAnalysis(
	ctx context.Context,
	request ForceAnalysisRequest,
) (contracts.SchedulerScanResponse, error) {
	if services.scheduler == nil {
		return contracts.SchedulerScanResponse{}, errors.New("scheduler client is not configured")
	}

	requestedBy := strings.TrimSpace(request.RequestedBy)
	if requestedBy == "" {
		requestedBy = "interface"
	}

	return services.scheduler.Force(ctx, request.Digest, request.Analyzers, requestedBy, request.TraceID)
}

func (services *Services) SourceAction(
	ctx context.Context,
	request SourceActionRequest,
) (source.ActionResult, error) {
	if services.sources == nil {
		return source.ActionResult{}, errors.New("source registry is not configured")
	}

	manifest, err := services.objects.ReadManifest(ctx, request.Digest)
	if err != nil {
		return source.ActionResult{}, err
	}
	if !manifestHasFacet(manifest, MailMessageFacet) {
		return source.ActionResult{}, fmt.Errorf("object %s is not a mail message", request.Digest)
	}

	for _, provenance := range manifest.Provenance {
		backend, ok := services.sources.ActionBackend(provenance.SourceKind, provenance.SourceName)
		if !ok {
			continue
		}

		parameters := map[string]any{}
		for key, value := range request.Parameters {
			parameters[key] = value
		}
		if provenance.ExternalID != "" {
			parameters["message_id"] = provenance.ExternalID
		}

		return backend.ApplyAction(ctx, source.ActionRequest{
			ObjectDigest: request.Digest,
			Action:       request.Action,
			Parameters:   parameters,
		})
	}

	return source.ActionResult{}, fmt.Errorf("no action-capable source for object %s", request.Digest)
}

func (services *Services) OpsStatus(ctx context.Context) (OpsStatusResponse, error) {
	response := OpsStatusResponse{Errors: map[string]string{}, Counts: map[string]int{}}
	if services.scheduler != nil {
		status, err := services.scheduler.Status(ctx)
		if err != nil {
			response.Errors["scheduler"] = err.Error()
		} else {
			response.Scheduler = status
			response.Counts["scheduler_pending"] = status.Pending
		}
	}

	cursors, err := services.query.SourceCursors(ctx, contracts.SourceCursorRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
	})
	if err != nil {
		response.Errors["source_cursors"] = err.Error()
	} else {
		response.Cursors = cursors
		response.Counts["source_cursors"] = len(cursors.Cursors)
	}

	if len(response.Errors) == 0 {
		response.Errors = nil
	}

	return response, nil
}

func (services *Services) liveMailSearch(
	ctx context.Context,
	backend source.LiveSearchAdapter,
	options SearchOptions,
) ([]ObjectSearchResult, error) {
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}

	var hits []source.LiveSearchResult
	var err error
	if hydrater, ok := backend.(source.HydratingLiveSearchAdapter); ok && services.ingest != nil {
		hits, err = hydrater.SearchAndHydrate(ctx, services.ingest, source.LiveSearchRequest{
			Query: options.Query,
			Limit: limit,
		})
	}
	if hits == nil && err == nil {
		hits, err = backend.LiveSearch(ctx, source.LiveSearchRequest{
			Query: options.Query,
			Limit: limit,
		})
	}
	if err != nil {
		return nil, err
	}

	results := make([]ObjectSearchResult, 0, len(hits))
	for _, hit := range hits {
		if hit.ObjectDigest == "" {
			continue
		}

		result := ObjectSearchResult{
			ObjectDigest:      hit.ObjectDigest,
			Facets:            []string{MailMessageFacet},
			Score:             0.5,
			ProjectionPending: hit.Hydrated,
			AnalysisPending:   hit.Hydrated,
			Attributes: map[string]any{
				"source_kind":      backend.Kind(),
				"source_name":      backend.Name(),
				"external_id":      hit.ExternalID,
				"external_version": hit.ExternalVersion,
				"hydrated":         hit.Hydrated,
			},
		}
		if options.FullMessage {
			structure, err := services.Structure(ctx, hit.ObjectDigest)
			if err == nil {
				result.Structure = &structure
			}
		}

		results = append(results, result)
	}

	return results, nil
}

func (services *Services) expandSearchResults(
	ctx context.Context,
	results []contracts.SearchResult,
	includeStructure bool,
) ([]ObjectSearchResult, error) {
	expanded := make([]ObjectSearchResult, 0, len(results))
	for _, result := range results {
		item := ObjectSearchResult{
			Attributes:   copyMap(result.Attributes),
			ObjectDigest: result.ObjectDigest,
			Title:        result.Title,
			Snippet:      result.Snippet,
			Facets:       append([]string{}, result.Facets...),
			Score:        result.Score,
		}
		if includeStructure {
			structure, err := services.Structure(ctx, result.ObjectDigest)
			if err != nil {
				return nil, err
			}
			item.Structure = &structure
		}
		manifest, err := services.objects.ReadManifest(ctx, result.ObjectDigest)
		if err == nil {
			item.Provenance = append([]contracts.Provenance{}, manifest.Provenance...)
		}
		expanded = append(expanded, item)
	}

	return expanded, nil
}

func searchRequest(options SearchOptions) contracts.SearchRequest {
	return contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Query:         options.Query,
		Facets:        append([]string{}, options.Facets...),
		AnalyzerNames: append([]string{}, options.AnalyzerNames...),
		MediaTypes:    append([]string{}, options.MediaTypes...),
		Limit:         options.Limit,
		Offset:        options.Offset,
	}
}

func ensureFacet(facets []string, facet string) []string {
	for _, existing := range facets {
		if existing == facet {
			return append([]string{}, facets...)
		}
	}

	return append(append([]string{}, facets...), facet)
}

func mergeSearchResult(existing, next ObjectSearchResult) ObjectSearchResult {
	if existing.ObjectDigest == "" {
		return next
	}
	if next.Title != "" {
		existing.Title = next.Title
	}
	if next.Snippet != "" {
		existing.Snippet = next.Snippet
	}
	if next.Score > existing.Score {
		existing.Score = next.Score
	}
	existing.Facets = unionStrings(existing.Facets, next.Facets)
	existing.AnalysisPending = existing.AnalysisPending || next.AnalysisPending
	existing.ProjectionPending = existing.ProjectionPending || next.ProjectionPending
	if existing.Attributes == nil {
		existing.Attributes = map[string]any{}
	}
	for key, value := range next.Attributes {
		existing.Attributes[key] = value
	}
	if existing.Structure == nil {
		existing.Structure = next.Structure
	}
	if len(existing.Provenance) == 0 {
		existing.Provenance = next.Provenance
	}

	return existing
}

func unionStrings(left, right []string) []string {
	seen := map[string]bool{}
	values := []string{}
	for _, value := range append(append([]string{}, left...), right...) {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		values = append(values, value)
	}
	sort.Strings(values)

	return values
}

func copyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	copied := make(map[string]any, len(value))
	for key, item := range value {
		copied[key] = item
	}

	return copied
}

func manifestHasFacet(manifest contracts.Manifest, facet string) bool {
	return slices.ContainsFunc(manifest.Facets, func(candidate contracts.Facet) bool {
		return candidate.FacetKind() == facet
	})
}

func hasCapability(adapter source.Adapter, capability string) bool {
	return slices.Contains(adapter.Capabilities(), capability)
}

func readObjectContent(
	ctx context.Context,
	objects ObjectReader,
	digest contracts.ObjectDigest,
) (string, error) {
	reader, err := objects.Open(ctx, digest)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	content, err := io.ReadAll(io.LimitReader(reader, 1024*1024))
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func JSONText(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return string(encoded), nil
}
