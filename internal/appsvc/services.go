// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/source"
)

const (
	MailMessageFacet = "mail_message"
	jmapDefaultLimit = 50
)

var (
	emailAddressExpression = regexp.MustCompile(`<([^<>@\s]+@[^<>@\s]+)>`)
	firstURLExpression     = regexp.MustCompile(`https://[^\s<>"]+`)
	githubPRReviewURL      = regexp.MustCompile(
		`github\.com[:/]+([^/\s]+)/([^/\s]+)/pull/([0-9]+)(?:/(?:review/)?([0-9]+))?`,
	)
	githubPRReviewMessageID = regexp.MustCompile(
		`<?([^/\s<>]+)/([^/\s<>]+)/pull/([0-9]+)/(?:review/)?([0-9]+)@github\.com`,
	)
)

type Services struct {
	query            QueryReader
	objects          ObjectReader
	scheduler        SchedulerClient
	sources          SourceRegistry
	ingest           SourceIngestService
	operations       OperationStore
	operationWaiters map[string]*operationWaiter
	operationMu      sync.Mutex
}

type Options struct {
	Query      QueryReader
	Objects    ObjectReader
	Scheduler  SchedulerClient
	Sources    SourceRegistry
	Ingest     SourceIngestService
	Operations OperationStore
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

type MessageSummaryRequest struct {
	MessageID string `json:"message_id"`
}

type SummarySearchResponse struct {
	Messages []MessageSummaryListItem `json:"messages"`
	Query    string                   `json:"query,omitempty"`
	Total    int                      `json:"total"`
	Returned int                      `json:"returned"`
}

type MessageSummaryListItem struct {
	MessageID string `json:"message_id,omitempty"`
	Date      string `json:"date,omitempty"`
	Subject   string `json:"subject,omitempty"`
	To        string `json:"to,omitempty"`
	From      string `json:"from,omitempty"`
	Summary   string `json:"summary,omitempty"`
	Digest    string `json:"digest,omitempty"`
}

type ObjectSearchResponse struct {
	Results []ObjectSearchResult `json:"results"`
	Total   int                  `json:"total"`
}

type ObjectSearchResult struct {
	Attributes        map[string]any         `json:"attributes,omitempty"`
	Structure         *contracts.Structure   `json:"structure,omitempty"`
	Message           *CanonicalMessage      `json:"message,omitempty"`
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
	Message  *CanonicalMessage  `json:"message,omitempty"`
	Content  string             `json:"content,omitempty"`
}

type CanonicalMessage struct {
	Graph           CanonicalMessageGraph        `json:"graph,omitempty"`
	SelectedHeaders CanonicalSelectedHeaders     `json:"selected_headers"`
	MessageID       string                       `json:"message_id,omitempty"`
	Digest          string                       `json:"digest"`
	ObjectID        string                       `json:"object_id,omitempty"`
	ThreadID        string                       `json:"thread_id,omitempty"`
	Summary         string                       `json:"summary,omitempty"`
	Bullets         []string                     `json:"bullets,omitempty"`
	Categories      []string                     `json:"categories,omitempty"`
	Body            string                       `json:"body,omitempty"`
	Attachments     []CanonicalMessageAttachment `json:"attachments"`
}

type CanonicalSelectedHeaders struct {
	Date    string `json:"date,omitempty"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Cc      string `json:"cc,omitempty"`
	Subject string `json:"subject,omitempty"`
}

type CanonicalMessageGraph struct {
	Source  map[string]any         `json:"source,omitempty"`
	Topics  []string               `json:"topics,omitempty"`
	Actions []string               `json:"actions,omitempty"`
	Links   []CanonicalMessageLink `json:"links,omitempty"`
}

type CanonicalMessageLink struct {
	Rel string `json:"rel"`
	URL string `json:"url"`
}

type CanonicalMessageAttachment struct {
	RetrievalID string `json:"retrieval_id"`
	Filename    string `json:"filename,omitempty"`
	MediaType   string `json:"media_type,omitempty"`
	Summary     string `json:"summary,omitempty"`
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

type JMAPEmailQueryResponse struct {
	IDs    []contracts.ObjectDigest `json:"ids"`
	Total  int                      `json:"total"`
	Offset int                      `json:"offset"`
	Limit  int                      `json:"limit"`
}

type JMAPEmailMutation struct {
	ObjectDigest     contracts.ObjectDigest `json:"object_digest"`
	MailboxIDs       map[string]bool        `json:"mailbox_ids,omitempty"`
	Keywords         map[string]bool        `json:"keywords,omitempty"`
	ReplaceMailboxes bool                   `json:"replace_mailboxes,omitempty"`
	ReplaceKeywords  bool                   `json:"replace_keywords,omitempty"`
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

	operations := options.Operations
	if operations == nil {
		if queryOperations, ok := options.Query.(OperationStore); ok {
			operations = queryOperations
		} else {
			operations = NewMemoryOperationStore()
		}
	}

	return &Services{
		query:            options.Query,
		objects:          options.Objects,
		scheduler:        options.Scheduler,
		sources:          options.Sources,
		ingest:           options.Ingest,
		operations:       operations,
		operationWaiters: map[string]*operationWaiter{},
	}, nil
}

func NewStaticSourceRegistry(adapters ...source.Adapter) *StaticSourceRegistry {
	return &StaticSourceRegistry{adapters: append([]source.Adapter{}, adapters...)}
}

func (registry *StaticSourceRegistry) LiveSearchBackends(
	facet string,
) []source.LiveSearchAdapter {
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

func (registry *StaticSourceRegistry) ActionBackend(
	kind, name string,
) (source.ActionAdapter, bool) {
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

	results, err := services.expandSearchResults(
		ctx,
		response.Results,
		options.FullMessage,
	)
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
		observability.DefaultMetrics().AddCounter("gmeow_source_errors", 1)
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
	for index := range results {
		message, err := services.canonicalMessage(ctx, results[index].ObjectDigest)
		if err != nil {
			continue
		}
		results[index].Message = &message
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
	if manifestHasFacet(manifest, MailMessageFacet) {
		message, err := services.canonicalMessage(ctx, digest)
		if err == nil {
			response.Message = &message
		}
	}
	if includeContent {
		content, err := readObjectContent(ctx, services.objects, digest)
		if err != nil {
			return RetrieveResponse{}, err
		}
		response.Content = content
	}

	return response, nil
}

func (services *Services) MessageSummary(
	ctx context.Context,
	request MessageSummaryRequest,
) (CanonicalMessage, error) {
	messageID := strings.TrimSpace(request.MessageID)
	if messageID == "" {
		return CanonicalMessage{}, errors.New("message_id is required")
	}

	response, err := services.MailSearch(ctx, SearchOptions{
		Query: messageIDSearchQuery(messageID),
		Limit: 25,
	})
	if err != nil {
		return CanonicalMessage{}, err
	}
	normalized := normalizeMessageID(messageID)
	for _, result := range response.Results {
		if result.Message == nil {
			continue
		}
		if normalizeMessageID(result.Message.MessageID) == normalized {
			return *result.Message, nil
		}
	}

	return CanonicalMessage{}, fmt.Errorf("message_id %q not found", messageID)
}

func (services *Services) SummarySearch(
	ctx context.Context,
	options SearchOptions,
) (SummarySearchResponse, error) {
	response, err := services.MailSearch(ctx, options)
	if err != nil {
		return SummarySearchResponse{}, err
	}

	messages := make([]MessageSummaryListItem, 0, len(response.Results))
	for _, result := range response.Results {
		if result.Message == nil {
			continue
		}
		messages = append(messages, summaryListItem(*result.Message))
	}

	return SummarySearchResponse{
		Query:    options.Query,
		Total:    response.Total,
		Returned: len(messages),
		Messages: messages,
	}, nil
}

func (services *Services) canonicalMessage(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (CanonicalMessage, error) {
	manifest, err := services.objects.ReadManifest(ctx, digest)
	if err != nil {
		return CanonicalMessage{}, err
	}
	if !manifestHasFacet(manifest, MailMessageFacet) {
		return CanonicalMessage{}, fmt.Errorf("object %s is not a mail message", digest)
	}

	headers := map[string]string{}
	body := ""
	attachments := []CanonicalMessageAttachment{}
	for _, part := range manifest.Compound.Parts {
		switch part.Role {
		case "rfc822_headers":
			headers = mergeHeaderMaps(headers, services.messageHeaders(ctx, part.Digest))
		case "email_body":
			if body == "" {
				body = canonicalBodyText(services.partContent(ctx, part.Digest))
			}
		case "attachment":
			attachment, ok := services.canonicalAttachment(ctx, part)
			if ok {
				attachments = append(attachments, attachment)
			}
		}
	}

	metadata := mailFacetMetadata(manifest)
	analysis := services.messageAnalysis(ctx, manifest, digest)
	summary, bullets := canonicalSummary(analysis, digest)
	categories := canonicalCategories(analysis)
	messageID := firstNonEmpty(
		headers["message-id"],
		stringFromAny(metadata["rfc_message_id"]),
	)
	subject := firstNonEmpty(headers["subject"], stringFromAny(metadata["subject"]))
	threadID := stringFromAny(metadata["thread_id"])

	return CanonicalMessage{
		MessageID: messageID,
		Digest:    string(digest),
		ObjectID:  manifest.ObjectID,
		ThreadID:  threadID,
		SelectedHeaders: CanonicalSelectedHeaders{
			Date:    headers["date"],
			From:    headers["from"],
			To:      headers["to"],
			Cc:      headers["cc"],
			Subject: subject,
		},
		Summary:     collapseWhitespace(summary),
		Bullets:     collapseStringList(bullets),
		Categories:  categories,
		Graph:       canonicalGraph(headers, subject, body),
		Body:        body,
		Attachments: attachments,
	}, nil
}

func (services *Services) partContent(
	ctx context.Context,
	digest contracts.ObjectDigest,
) string {
	content, err := readObjectContent(ctx, services.objects, digest)
	if err != nil {
		return ""
	}

	return content
}

func (services *Services) messageHeaders(
	ctx context.Context,
	digest contracts.ObjectDigest,
) map[string]string {
	content := services.partContent(ctx, digest)
	if strings.TrimSpace(content) == "" {
		return map[string]string{}
	}

	var rows []map[string]string
	if err := json.Unmarshal([]byte(content), &rows); err != nil {
		return map[string]string{}
	}
	headers := map[string]string{}
	for _, row := range rows {
		name := strings.ToLower(strings.TrimSpace(row["name"]))
		value := collapseWhitespace(row["value"])
		if name == "" || value == "" {
			continue
		}
		headers[name] = value
	}

	return headers
}

func (services *Services) canonicalAttachment(
	ctx context.Context,
	part contracts.CompoundPart,
) (CanonicalMessageAttachment, bool) {
	manifest, err := services.objects.ReadManifest(ctx, part.Digest)
	if err != nil {
		return CanonicalMessageAttachment{}, false
	}
	filename := stringFromAny(part.Metadata["filename"])
	if filename == "" {
		filename = attachmentFilename(manifest)
	}
	if filename == "" && strings.EqualFold(manifest.MediaType, "text/html") {
		return CanonicalMessageAttachment{}, false
	}

	return CanonicalMessageAttachment{
		RetrievalID: string(part.Digest),
		Filename:    filename,
		MediaType:   manifest.MediaType,
		Summary:     attachmentSummary(manifest, filename),
	}, true
}

func (services *Services) messageAnalysis(
	ctx context.Context,
	manifest contracts.Manifest,
	digest contracts.ObjectDigest,
) []contracts.AnalysisStatus {
	digests := []contracts.ObjectDigest{digest}
	for _, part := range manifest.Compound.Parts {
		if part.Role == "email_body" {
			digests = append(digests, part.Digest)
		}
	}
	response, err := services.query.AnalysisStatus(ctx, contracts.AnalysisStatusRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigests: digests,
		AnalyzerNames: []string{"summary.model", "categories.sklearn"},
		Limit:         20,
	})
	if err != nil {
		return nil
	}

	return response.Statuses
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

func (services *Services) JMAPMailboxes(
	ctx context.Context,
) ([]contracts.JMAPMailbox, error) {
	reader, ok := services.query.(JMAPQueryReader)
	if !ok {
		return nil, errors.New("JMAP query reader is not configured")
	}

	return reader.JMAPMailboxes(ctx)
}

func (services *Services) JMAPEmailStates(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error) {
	reader, ok := services.query.(JMAPQueryReader)
	if !ok {
		return nil, errors.New("JMAP query reader is not configured")
	}

	return reader.JMAPEmailStates(ctx, digests)
}

func (services *Services) JMAPEmailQuery(
	ctx context.Context,
	offset, limit int,
) (JMAPEmailQueryResponse, error) {
	if limit <= 0 {
		limit = jmapDefaultLimit
	}
	response, err := services.query.Search(ctx, contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Facets:        []string{MailMessageFacet},
		Limit:         limit,
		Offset:        offset,
	})
	if err != nil {
		return JMAPEmailQueryResponse{}, err
	}

	ids := make([]contracts.ObjectDigest, 0, len(response.Results))
	for _, result := range response.Results {
		ids = append(ids, result.ObjectDigest)
	}

	return JMAPEmailQueryResponse{
		IDs:    ids,
		Total:  response.Total,
		Offset: offset,
		Limit:  limit,
	}, nil
}

func (services *Services) UpdateJMAPEmailState(
	ctx context.Context,
	mutation JMAPEmailMutation,
) (contracts.JMAPEmailState, error) {
	writer, ok := services.objects.(ObjectWriter)
	if !ok {
		return contracts.JMAPEmailState{}, errors.New(
			"JMAP object writer is not configured",
		)
	}
	reader, ok := services.query.(JMAPQueryReader)
	if !ok {
		return contracts.JMAPEmailState{}, errors.New(
			"JMAP query reader is not configured",
		)
	}

	states, err := reader.JMAPEmailStates(
		ctx,
		[]contracts.ObjectDigest{mutation.ObjectDigest},
	)
	if err != nil {
		return contracts.JMAPEmailState{}, err
	}
	current, ok := states[mutation.ObjectDigest]
	if !ok {
		return contracts.JMAPEmailState{}, fmt.Errorf(
			"JMAP email state for %s not found",
			mutation.ObjectDigest,
		)
	}

	update := contracts.JMAPEmailStateUpdate{
		ObjectDigest: mutation.ObjectDigest,
		MailboxIDs: applyJMAPBoolMap(
			current.MailboxIDs,
			mutation.MailboxIDs,
			mutation.ReplaceMailboxes,
		),
		Keywords: applyJMAPBoolMap(
			current.Keywords,
			mutation.Keywords,
			mutation.ReplaceKeywords,
		),
	}
	if len(update.MailboxIDs) == 0 {
		return contracts.JMAPEmailState{}, errors.New(
			"JMAP email must remain in at least one mailbox",
		)
	}

	if err := writer.WriteOverlays(ctx, mutation.ObjectDigest, map[string]any{
		"jmap": map[string]any{
			"mailbox_ids": update.MailboxIDs,
			"keywords":    update.Keywords,
		},
	}); err != nil {
		return contracts.JMAPEmailState{}, err
	}

	return reader.UpdateJMAPEmailState(ctx, update)
}

func (services *Services) ForceAnalysis(
	ctx context.Context,
	request ForceAnalysisRequest,
) (contracts.SchedulerScanResponse, error) {
	if services.scheduler == nil {
		return contracts.SchedulerScanResponse{}, errors.New(
			"scheduler client is not configured",
		)
	}

	requestedBy := strings.TrimSpace(request.RequestedBy)
	if requestedBy == "" {
		requestedBy = "interface"
	}

	return services.scheduler.Force(
		ctx,
		request.Digest,
		request.Analyzers,
		requestedBy,
		request.TraceID,
	)
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
		return source.ActionResult{}, fmt.Errorf(
			"object %s is not a mail message",
			request.Digest,
		)
	}

	for _, provenance := range manifest.Provenance {
		backend, ok := services.sources.ActionBackend(
			provenance.SourceKind,
			provenance.SourceName,
		)
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

	return source.ActionResult{}, fmt.Errorf(
		"no action-capable source for object %s",
		request.Digest,
	)
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
			response.Counts["scheduler_retry"] = status.Retry
			response.Counts["scheduler_failed"] = status.Failed
			response.Counts["scheduler_dead_letter"] = status.DeadLetter
			metrics := observability.DefaultMetrics()
			metrics.SetGauge("gmeow_queue_depth_pending", float64(status.Pending))
			metrics.SetGauge("gmeow_queue_depth_retry", float64(status.Retry))
			metrics.SetGauge("gmeow_failed_analyzers", float64(status.Failed))
			metrics.SetGauge("gmeow_dead_letter_jobs", float64(status.DeadLetter))
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
	response.Metadata = map[string]any{
		"metrics": observability.DefaultMetrics().Snapshot(),
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
	if hydrater, ok := backend.(source.HydratingLiveSearchAdapter); ok &&
		services.ingest != nil {
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

func applyJMAPBoolMap(existing []string, patch map[string]bool, replace bool) []string {
	values := map[string]bool{}
	if !replace {
		for _, value := range existing {
			if value != "" {
				values[value] = true
			}
		}
	}
	for key, enabled := range patch {
		if key == "" {
			continue
		}
		if enabled {
			values[key] = true
		} else {
			delete(values, key)
		}
	}

	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)

	return out
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

func mergeHeaderMaps(
	left map[string]string,
	right map[string]string,
) map[string]string {
	merged := map[string]string{}
	for key, value := range left {
		merged[key] = value
	}
	for key, value := range right {
		merged[key] = value
	}

	return merged
}

func mailFacetMetadata(manifest contracts.Manifest) map[string]any {
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == MailMessageFacet {
			return copyMap(facet.Metadata)
		}
	}

	return map[string]any{}
}

func canonicalBodyText(content string) string {
	text := collapseWhitespace(content)
	for _, marker := range []string{
		" -- Reply to this email",
		" Reply to this email directly",
		" You are receiving this because",
		" Message ID: <",
	} {
		if index := strings.Index(text, marker); index >= 0 {
			text = strings.TrimSpace(text[:index])
		}
	}

	return text
}

func canonicalSummary(
	statuses []contracts.AnalysisStatus,
	parentDigest contracts.ObjectDigest,
) (string, []string) {
	bodySummary := ""
	bodyBullets := []string{}
	parentSummary := ""
	parentBullets := []string{}

	for _, status := range statuses {
		if status.AnalyzerName != "summary.model" || status.Status != "complete" {
			continue
		}
		summary := stringFromAny(status.Data["summary"])
		bullets := stringSliceFromAny(status.Data["bullets"])
		if status.ObjectDigest != "" &&
			status.ObjectDigest != parentDigest &&
			bodySummary == "" {
			bodySummary = summary
			bodyBullets = bullets
		}
		if parentSummary == "" {
			parentSummary = summary
			parentBullets = bullets
		}
	}
	if bodySummary != "" || len(bodyBullets) > 0 {
		return bodySummary, bodyBullets
	}

	return parentSummary, parentBullets
}

func canonicalCategories(statuses []contracts.AnalysisStatus) []string {
	seen := map[string]bool{}
	categories := []string{}
	for _, status := range statuses {
		if status.AnalyzerName != "categories.sklearn" || status.Status != "complete" {
			continue
		}
		for _, category := range stringSliceFromAny(status.Data["category_ids"]) {
			if category != "" && !seen[category] {
				seen[category] = true
				categories = append(categories, category)
			}
		}
		for _, raw := range anySlice(status.Data["categories"]) {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			category := stringFromAny(item["category"])
			if category != "" && !seen[category] {
				seen[category] = true
				categories = append(categories, category)
			}
		}
	}
	sort.Strings(categories)

	return categories
}

func canonicalGraph(
	headers map[string]string,
	subject string,
	body string,
) CanonicalMessageGraph {
	link := firstURL(body)
	repository, pullRequest, reviewID := githubPRReview(link, headers["message-id"])
	source := map[string]any{}
	if repository != "" {
		source["kind"] = "pull_request_review"
		source["service"] = "github"
		source["repository"] = repository
		source["pull_request"] = pullRequest
		if reviewID != "" {
			source["review_id"] = reviewID
		}
		if actor := githubActor(headers["from"], body); actor != "" {
			source["actor"] = actor
		}
	}

	links := []CanonicalMessageLink{}
	if link != "" {
		links = append(links, CanonicalMessageLink{Rel: "canonical", URL: link})
	}

	return CanonicalMessageGraph{
		Source: source,
		Links:  links,
	}
}

func githubPRReview(rawURL, messageID string) (string, int, string) {
	text := rawURL
	if text == "" {
		text = messageID
	}
	matches := githubPRReviewURL.FindStringSubmatch(text)
	if len(matches) == 0 {
		matches = githubPRReviewMessageID.FindStringSubmatch(text)
	}
	if len(matches) == 0 {
		return "", 0, ""
	}
	pullRequest := intFromString(matches[3])
	reviewID := ""
	if len(matches) > 4 {
		reviewID = matches[4]
	}

	return matches[1] + "/" + matches[2], pullRequest, reviewID
}

func githubActor(from, body string) string {
	if from != "" {
		if index := strings.Index(from, " <"); index > 0 {
			return strings.Trim(from[:index], `"`)
		}

		return from
	}
	if strings.HasPrefix(body, "@") {
		if index := strings.Index(body, " "); index > 1 {
			return strings.TrimPrefix(body[:index], "@")
		}
	}

	return ""
}

func firstURL(text string) string {
	raw := firstURLExpression.FindString(text)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if parsed.Fragment != "" {
		return raw
	}

	return parsed.String()
}

func attachmentFilename(manifest contracts.Manifest) string {
	for _, facet := range manifest.Facets {
		if value := stringFromAny(facet.Metadata["display_name"]); value != "" {
			return value
		}
		if value := stringFromAny(facet.Attributes["display_name"]); value != "" {
			return value
		}
	}

	return ""
}

func attachmentSummary(manifest contracts.Manifest, filename string) string {
	if filename != "" {
		return collapseWhitespace(filename)
	}
	if manifest.MediaType != "" {
		return "Attachment with media type " + manifest.MediaType + "."
	}

	return ""
}

func summaryListItem(message CanonicalMessage) MessageSummaryListItem {
	return MessageSummaryListItem{
		MessageID: message.MessageID,
		Date:      abbreviatedMailDate(message.SelectedHeaders.Date),
		Subject:   message.SelectedHeaders.Subject,
		To:        firstEmailAddress(message.SelectedHeaders.To),
		From:      firstEmailAddress(message.SelectedHeaders.From),
		Summary:   message.Summary,
		Digest:    message.Digest,
	}
}

func firstEmailAddress(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if matches := emailAddressExpression.FindStringSubmatch(value); len(matches) > 1 {
		return matches[1]
	}
	first := strings.Split(value, ",")[0]
	fields := strings.Fields(first)
	for _, field := range fields {
		candidate := strings.Trim(field, "<>()\"'")
		if strings.Contains(candidate, "@") {
			return candidate
		}
	}

	return strings.TrimSpace(first)
}

func normalizeMessageID(value string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(value), "<>"))
}

func messageIDSearchQuery(value string) string {
	normalized := normalizeMessageID(value)
	replacer := strings.NewReplacer(
		"@", " ",
		".", " ",
		"/", " ",
		"_", " ",
		"-", " ",
		"+", " ",
	)
	return collapseWhitespace(replacer.Replace(normalized))
}

func abbreviatedMailDate(value string) string {
	parsed, err := mail.ParseDate(value)
	if err != nil {
		return ""
	}

	return parsed.Format("02/01/06")
}

func collapseStringList(values []string) []string {
	out := []string{}
	for _, value := range values {
		collapsed := collapseWhitespace(value)
		if collapsed != "" {
			out = append(out, collapsed)
		}
	}

	return out
}

func collapseWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func stringFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func stringSliceFromAny(value any) []string {
	values := []string{}
	for _, item := range anySlice(value) {
		text := stringFromAny(item)
		if text != "" {
			values = append(values, text)
		}
	}

	return values
}

func anySlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case []string:
		values := make([]any, 0, len(typed))
		for _, item := range typed {
			values = append(values, item)
		}

		return values
	default:
		return nil
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

func intFromString(value string) int {
	var parsed int
	_, _ = fmt.Sscanf(value, "%d", &parsed)

	return parsed
}

func JSONText(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return string(encoded), nil
}
