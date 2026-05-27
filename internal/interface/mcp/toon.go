// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	toon "github.com/toon-format/toon-go"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/source"
)

type toonErrorOutput struct {
	Tool  string `toon:"tool"`
	Error string `toon:"error"`
}

type toonSearchOutput struct {
	Tool     string             `toon:"tool"`
	Query    string             `toon:"query,omitempty"`
	Results  []toonSearchResult `toon:"results"`
	Total    int                `toon:"total"`
	Returned int                `toon:"returned"`
}

type toonSearchResult struct {
	Message *toonMessage `toon:"message,omitempty"`
	Rank    int          `toon:"rank"`
	Digest  string       `toon:"digest"`
	Score   float64      `toon:"score"`
	Title   string       `toon:"title"`
	Snippet string       `toon:"snippet"`
	Facets  string       `toon:"facets"`
	Pending string       `toon:"pending"`
}

type toonRetrieveOutput struct {
	Tool        string              `toon:"tool"`
	Message     *toonMessage        `toon:"message,omitempty"`
	Digest      string              `toon:"digest"`
	ObjectID    string              `toon:"object_id,omitempty"`
	MediaType   string              `toon:"media_type,omitempty"`
	Compression string              `toon:"compression,omitempty"`
	CreatedAt   string              `toon:"created_at,omitempty"`
	UpdatedAt   string              `toon:"updated_at,omitempty"`
	Facets      []toonFacetRow      `toon:"facets,omitempty"`
	Titles      []string            `toon:"titles,omitempty"`
	Provenance  []toonProvenanceRow `toon:"provenance,omitempty"`
	Compound    toonCompoundOutput  `toon:"compound,omitempty"`
	Content     string              `toon:"content,omitempty"`
	Size        int64               `toon:"size"`
}

type toonMessageSummaryOutput struct {
	Tool    string       `toon:"tool"`
	Message *toonMessage `toon:"message"`
}

type toonSummarySearchOutput struct {
	Tool     string                   `toon:"tool"`
	Query    string                   `toon:"query,omitempty"`
	Messages []toonSummaryMessageItem `toon:"messages"`
	Total    int                      `toon:"total"`
	Returned int                      `toon:"returned"`
}

type toonSummaryMessageItem struct {
	IDSubject string `toon:"msgid_subject"`
	ToFrom    string `toon:"to_from"`
	Summary   string `toon:"summary"`
}

type toonMessage struct {
	MessageID       string              `toon:"message_id,omitempty"`
	Digest          string              `toon:"digest"`
	ObjectID        string              `toon:"object_id,omitempty"`
	ThreadID        string              `toon:"thread_id,omitempty"`
	SelectedHeaders toonSelectedHeaders `toon:"selected_headers"`
	Summary         string              `toon:"summary,omitempty"`
	Bullets         []string            `toon:"bullets,omitempty"`
	Categories      []string            `toon:"categories,omitempty"`
	Graph           toonMessageGraph    `toon:"graph,omitempty"`
	Body            string              `toon:"body,omitempty"`
	Attachments     []toonAttachment    `toon:"attachments"`
}

type toonSelectedHeaders struct {
	Date    string `toon:"date,omitempty"`
	From    string `toon:"from,omitempty"`
	To      string `toon:"to,omitempty"`
	Cc      string `toon:"cc,omitempty"`
	Subject string `toon:"subject,omitempty"`
}

type toonMessageGraph struct {
	Source  map[string]any  `toon:"source,omitempty"`
	Topics  []string        `toon:"topics,omitempty"`
	Actions []string        `toon:"actions,omitempty"`
	Links   []toonGraphLink `toon:"links,omitempty"`
}

type toonGraphLink struct {
	Rel string `toon:"rel"`
	URL string `toon:"url"`
}

type toonAttachment struct {
	RetrievalID string `toon:"retrieval_id"`
	Filename    string `toon:"filename,omitempty"`
	MediaType   string `toon:"media_type,omitempty"`
	Summary     string `toon:"summary,omitempty"`
}

type toonStructureOutput struct {
	Tool     string        `toon:"tool"`
	Digest   string        `toon:"digest"`
	ObjectID string        `toon:"object_id,omitempty"`
	Facets   []string      `toon:"facets,omitempty"`
	Parts    []toonPartRow `toon:"parts,omitempty"`
}

type toonProvenanceOutput struct {
	Tool       string              `toon:"tool"`
	Provenance []toonProvenanceRow `toon:"provenance"`
}

type toonProvenanceRow struct {
	ObservedAt      string `toon:"observed_at,omitempty"`
	SourceKind      string `toon:"source_kind"`
	SourceName      string `toon:"source_name"`
	ExternalID      string `toon:"external_id,omitempty"`
	ExternalVersion string `toon:"external_version,omitempty"`
}

type toonFacetsOutput struct {
	Tool   string         `toon:"tool"`
	Facets []toonFacetRow `toon:"facets"`
}

type toonFacetRow struct {
	Kind    string         `toon:"kind"`
	Name    string         `toon:"name,omitempty"`
	Version string         `toon:"version,omitempty"`
	Data    map[string]any `toon:"data,omitempty"`
}

type toonCompoundOutput struct {
	Tool       string        `toon:"tool,omitempty"`
	Parts      []toonPartRow `toon:"parts,omitempty"`
	IsCompound bool          `toon:"is_compound"`
}

type toonPartRow struct {
	Role     string   `toon:"role"`
	Digest   string   `toon:"digest"`
	Facets   []string `toon:"facets,omitempty"`
	Order    int      `toon:"order,omitempty"`
	Required bool     `toon:"required,omitempty"`
}

type toonGraphOutput struct {
	Tool      string        `toon:"tool"`
	Node      string        `toon:"node,omitempty"`
	Predicate string        `toon:"predicate,omitempty"`
	Facts     []toonFactRow `toon:"facts"`
	Returned  int           `toon:"returned"`
}

type toonFactRow struct {
	Subject   string         `toon:"subject"`
	Predicate string         `toon:"predicate"`
	Object    string         `toon:"object"`
	Metadata  map[string]any `toon:"metadata,omitempty"`
}

type toonAnalysisOutput struct {
	Tool     string                  `toon:"tool"`
	Statuses []toonAnalysisStatusRow `toon:"statuses"`
	Returned int                     `toon:"returned"`
}

type toonAnalysisStatusRow struct {
	GeneratedAt string         `toon:"generated_at,omitempty"`
	Digest      string         `toon:"digest"`
	Analyzer    string         `toon:"analyzer"`
	Version     string         `toon:"version,omitempty"`
	Status      string         `toon:"status"`
	Data        map[string]any `toon:"data,omitempty"`
}

type toonForceAnalysisOutput struct {
	Tool     string `toon:"tool"`
	Digest   string `toon:"digest,omitempty"`
	Scanned  int    `toon:"scanned"`
	Enqueued int    `toon:"enqueued"`
	Skipped  int    `toon:"skipped"`
	Failed   int    `toon:"failed"`
}

type toonOperationOutput struct {
	Result      any                        `toon:"result,omitempty"`
	Progress    []toonOperationProgressRow `toon:"progress,omitempty"`
	Tool        string                     `toon:"tool"`
	OperationID string                     `toon:"operation_id"`
	Name        string                     `toon:"name"`
	Status      string                     `toon:"status"`
	Error       string                     `toon:"error,omitempty"`
	CreatedAt   string                     `toon:"created_at,omitempty"`
	UpdatedAt   string                     `toon:"updated_at,omitempty"`
	CompletedAt string                     `toon:"completed_at,omitempty"`
}

type toonOperationProgressRow struct {
	At       string  `toon:"at,omitempty"`
	Stage    string  `toon:"stage"`
	Message  string  `toon:"message,omitempty"`
	Progress float64 `toon:"progress,omitempty"`
	Total    float64 `toon:"total,omitempty"`
}

type toonSourceActionOutput struct {
	Attributes map[string]any `toon:"attributes,omitempty"`
	Tool       string         `toon:"tool"`
	Action     string         `toon:"action"`
	Applied    bool           `toon:"applied"`
}

type toonOpsStatusOutput struct {
	Metrics       map[string]float64  `toon:"metrics,omitempty"`
	Errors        map[string]string   `toon:"errors,omitempty"`
	Tool          string              `toon:"tool"`
	Counts        map[string]int      `toon:"counts,omitempty"`
	Scheduler     toonSchedulerStatus `toon:"scheduler"`
	SourceCursors []toonSourceCursor  `toon:"source_cursors,omitempty"`
}

type toonSchedulerStatus struct {
	Pending    int `toon:"pending"`
	Retry      int `toon:"retry"`
	Failed     int `toon:"failed"`
	DeadLetter int `toon:"dead_letter"`
}

type toonSourceCursor struct {
	Cursor     map[string]any `toon:"cursor,omitempty"`
	UpdatedAt  string         `toon:"updated_at,omitempty"`
	SourceKind string         `toon:"source_kind"`
	SourceName string         `toon:"source_name"`
}

func toonToolResult(value any) (*mcp.CallToolResult, error) {
	encoded, err := toon.MarshalString(
		value,
		toon.WithTimeFormatter(func(value time.Time) string {
			return value.UTC().Format(time.RFC3339Nano)
		}),
	)
	if err != nil {
		return nil, err
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: encoded}},
	}, nil
}

func toonToolError(tool string, err error) (*mcp.CallToolResult, error) {
	result, encodeErr := toonToolResult(toonErrorOutput{Tool: tool, Error: err.Error()})
	if encodeErr != nil {
		return nil, encodeErr
	}
	result.IsError = true

	return result, nil
}

func toonSearch(
	tool string,
	options appsvc.SearchOptions,
	response appsvc.ObjectSearchResponse,
) toonSearchOutput {
	results := make([]toonSearchResult, 0, len(response.Results))
	for index, result := range response.Results {
		results = append(results, toonSearchResult{
			Rank:    index + 1,
			Digest:  string(result.ObjectDigest),
			Message: toonMessageFromCanonical(result.Message),
			Score:   result.Score,
			Title:   result.Title,
			Snippet: result.Snippet,
			Facets:  strings.Join(result.Facets, "|"),
			Pending: pendingState(result.AnalysisPending, result.ProjectionPending),
		})
	}

	return toonSearchOutput{
		Tool:     tool,
		Query:    options.Query,
		Total:    response.Total,
		Returned: len(results),
		Results:  results,
	}
}

func toonRetrieve(response appsvc.RetrieveResponse) toonRetrieveOutput {
	manifest := response.Manifest

	return toonRetrieveOutput{
		Tool:        "object_retrieve",
		Digest:      string(manifest.ObjectDigest),
		ObjectID:    manifest.ObjectID,
		MediaType:   manifest.MediaType,
		Size:        manifest.Size,
		Compression: manifest.Compression,
		CreatedAt:   formatTime(manifest.CreatedAt),
		UpdatedAt:   formatTime(manifest.UpdatedAt),
		Facets:      toonFacets(manifest.Facets),
		Titles:      toonTitles(manifest.Titles),
		Provenance:  toonProvenance(manifest.Provenance),
		Compound:    toonCompound(manifest.Compound, false),
		Content:     response.Content,
		Message:     toonMessageFromCanonical(response.Message),
	}
}

func toonMessageSummary(message appsvc.CanonicalMessage) toonMessageSummaryOutput {
	return toonMessageSummaryOutput{
		Tool:    "message_summary",
		Message: toonMessageFromCanonical(&message),
	}
}

func toonSummarySearch(response appsvc.SummarySearchResponse) toonSummarySearchOutput {
	messages := make([]toonSummaryMessageItem, 0, len(response.Messages))
	for _, message := range response.Messages {
		messages = append(messages, toonSummaryMessageItem{
			IDSubject: summaryIDSubject(message),
			ToFrom:    message.To + " / " + message.From,
			Summary:   message.Summary,
		})
	}

	return toonSummarySearchOutput{
		Tool:     "summary_search",
		Query:    response.Query,
		Total:    response.Total,
		Returned: response.Returned,
		Messages: messages,
	}
}

func summaryIDSubject(message appsvc.MessageSummaryListItem) string {
	parts := []string{firstNonEmpty(message.MessageID, message.Digest)}
	if message.Date != "" {
		parts = append(parts, message.Date)
	}
	parts = append(parts, message.Subject)

	return strings.Join(parts, " / ")
}

func toonMessageFromCanonical(message *appsvc.CanonicalMessage) *toonMessage {
	if message == nil {
		return nil
	}

	attachments := make([]toonAttachment, 0, len(message.Attachments))
	for _, attachment := range message.Attachments {
		attachments = append(attachments, toonAttachment{
			RetrievalID: attachment.RetrievalID,
			Filename:    attachment.Filename,
			MediaType:   attachment.MediaType,
			Summary:     attachment.Summary,
		})
	}

	links := make([]toonGraphLink, 0, len(message.Graph.Links))
	for _, link := range message.Graph.Links {
		links = append(links, toonGraphLink{Rel: link.Rel, URL: link.URL})
	}

	return &toonMessage{
		MessageID: message.MessageID,
		Digest:    message.Digest,
		ObjectID:  message.ObjectID,
		ThreadID:  message.ThreadID,
		SelectedHeaders: toonSelectedHeaders{
			Date:    message.SelectedHeaders.Date,
			From:    message.SelectedHeaders.From,
			To:      message.SelectedHeaders.To,
			Cc:      message.SelectedHeaders.Cc,
			Subject: message.SelectedHeaders.Subject,
		},
		Summary:    message.Summary,
		Bullets:    append([]string{}, message.Bullets...),
		Categories: append([]string{}, message.Categories...),
		Graph: toonMessageGraph{
			Source:  message.Graph.Source,
			Topics:  append([]string{}, message.Graph.Topics...),
			Actions: append([]string{}, message.Graph.Actions...),
			Links:   links,
		},
		Body:        message.Body,
		Attachments: attachments,
	}
}

func toonStructure(response contracts.Structure) toonStructureOutput {
	parts := []toonPartRow{}
	roles := make([]string, 0, len(response.PartsByRole))
	for role := range response.PartsByRole {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		for _, part := range response.PartsByRole[role] {
			parts = append(parts, toonPartRow{
				Role:     firstNonEmpty(part.Role, role),
				Digest:   string(part.Digest),
				Order:    part.Order,
				Required: part.Required,
				Facets:   part.Facets,
			})
		}
	}
	sortParts(parts)

	return toonStructureOutput{
		Tool:     "get_structure",
		Digest:   string(response.ObjectDigest),
		ObjectID: response.ObjectID,
		Facets:   response.Facets,
		Parts:    parts,
	}
}

func toonProvenance(items []contracts.Provenance) []toonProvenanceRow {
	rows := make([]toonProvenanceRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, toonProvenanceRow{
			ObservedAt:      formatTime(item.ObservedAt),
			SourceKind:      item.SourceKind,
			SourceName:      item.SourceName,
			ExternalID:      item.ExternalID,
			ExternalVersion: item.ExternalVersion,
		})
	}
	sort.SliceStable(rows, func(left, right int) bool {
		if rows[left].SourceKind != rows[right].SourceKind {
			return rows[left].SourceKind < rows[right].SourceKind
		}
		if rows[left].SourceName != rows[right].SourceName {
			return rows[left].SourceName < rows[right].SourceName
		}

		return rows[left].ExternalID < rows[right].ExternalID
	})

	return rows
}

func toonProvenanceResult(items []contracts.Provenance) toonProvenanceOutput {
	return toonProvenanceOutput{Tool: "get_provenance", Provenance: toonProvenance(items)}
}

func toonFacets(items []contracts.Facet) []toonFacetRow {
	rows := make([]toonFacetRow, 0, len(items))
	for _, item := range items {
		data := map[string]any{}
		for key, value := range item.Attributes {
			data[key] = value
		}
		for key, value := range item.Metadata {
			data[key] = value
		}
		if len(data) == 0 {
			data = nil
		}
		rows = append(rows, toonFacetRow{
			Kind:    item.FacetKind(),
			Name:    item.Name,
			Version: item.Version,
			Data:    data,
		})
	}
	sort.SliceStable(rows, func(left, right int) bool {
		if rows[left].Kind != rows[right].Kind {
			return rows[left].Kind < rows[right].Kind
		}

		return rows[left].Name < rows[right].Name
	})

	return rows
}

func toonFacetsResult(items []contracts.Facet) toonFacetsOutput {
	return toonFacetsOutput{Tool: "get_facets", Facets: toonFacets(items)}
}

func toonCompound(compound contracts.Compound, includeTool bool) toonCompoundOutput {
	parts := make([]toonPartRow, 0, len(compound.Parts))
	for _, part := range compound.Parts {
		parts = append(parts, toonPartRow{
			Role:     part.Role,
			Digest:   string(part.Digest),
			Order:    part.Order,
			Required: part.Required,
		})
	}
	sortParts(parts)

	output := toonCompoundOutput{IsCompound: compound.IsCompound, Parts: parts}
	if includeTool {
		output.Tool = "compound_expand"
	}

	return output
}

func toonGraph(
	request contracts.GraphRequest,
	response contracts.GraphResponse,
) toonGraphOutput {
	facts := make([]toonFactRow, 0, len(response.Facts))
	for _, fact := range response.Facts {
		facts = append(facts, toonFactRow{
			Subject:   fact.Subject,
			Predicate: fact.Predicate,
			Object:    fact.Object,
			Metadata:  fact.Metadata,
		})
	}

	return toonGraphOutput{
		Tool:      "graph_explore",
		Node:      request.Node,
		Predicate: request.Predicate,
		Returned:  len(facts),
		Facts:     facts,
	}
}

func toonAnalysisStatus(response contracts.AnalysisStatusResponse) toonAnalysisOutput {
	statuses := make([]toonAnalysisStatusRow, 0, len(response.Statuses))
	for _, status := range response.Statuses {
		statuses = append(statuses, toonAnalysisStatusRow{
			Digest:      string(status.ObjectDigest),
			Analyzer:    status.AnalyzerName,
			Version:     status.AnalyzerVer,
			Status:      status.Status,
			GeneratedAt: formatTime(status.GeneratedAt),
			Data:        status.Data,
		})
	}

	return toonAnalysisOutput{
		Tool:     "analysis_status",
		Returned: len(statuses),
		Statuses: statuses,
	}
}

func toonForceAnalysis(
	request appsvc.ForceAnalysisRequest,
	response contracts.SchedulerScanResponse,
) toonForceAnalysisOutput {
	return toonForceAnalysisOutput{
		Tool:     "force_analysis",
		Digest:   string(request.Digest),
		Scanned:  response.Scanned,
		Enqueued: response.Enqueued,
		Skipped:  response.Skipped,
		Failed:   response.Failed,
	}
}

func toonOperation(response contracts.OperationStatusResponse) toonOperationOutput {
	return toonOperationOutput{
		Tool:        "operation_status",
		OperationID: response.OperationID,
		Name:        response.Name,
		Status:      string(response.Status),
		Error:       response.Error,
		CreatedAt:   formatTime(response.CreatedAt),
		UpdatedAt:   formatTime(response.UpdatedAt),
		CompletedAt: formatTime(response.CompletedAt),
		Progress:    toonProgress(response.Progress),
	}
}

func toonOperationResult(
	response contracts.OperationResultResponse,
) toonOperationOutput {
	output := toonOperation(response.Operation)
	output.Tool = "operation_result"
	output.Result = toonNestedOperationResult(response.Operation.Name, response.Result)

	return output
}

func toonOperationResume(
	response contracts.OperationResultResponse,
) toonOperationOutput {
	output := toonOperationResult(response)
	output.Tool = "operation_resume"

	return output
}

func toonOperationForTool(
	tool string,
	response contracts.OperationResultResponse,
) toonOperationOutput {
	output := toonOperationResult(response)
	output.Tool = tool

	return output
}

func toonNestedOperationResult(name string, result map[string]any) any {
	if len(result) == 0 {
		return nil
	}

	switch name {
	case "mail_search":
		if output, ok := toonSearchFromMap("mail_search", result); ok {
			return output
		}
	case "object_retrieve":
		if output, ok := toonRetrieveFromMap(result); ok {
			return output
		}
	case "graph_explore":
		if output, ok := toonGraphFromMap(result); ok {
			return output
		}
	case "analysis_status":
		if output, ok := toonAnalysisStatusFromMap(result); ok {
			return output
		}
	case "force_analysis":
		if output, ok := toonForceAnalysisFromMap(result); ok {
			return output
		}
	}

	return result
}

func toonProgress(
	events []contracts.OperationProgressEvent,
) []toonOperationProgressRow {
	rows := make([]toonOperationProgressRow, 0, len(events))
	for _, event := range events {
		rows = append(rows, toonOperationProgressRow{
			At:       formatTime(event.At),
			Stage:    event.Stage,
			Message:  event.Message,
			Progress: event.Progress,
			Total:    event.Total,
		})
	}

	return rows
}

func toonSourceAction(response source.ActionResult) toonSourceActionOutput {
	return toonSourceActionOutput{
		Tool:       "source_action",
		Action:     response.Action,
		Applied:    response.Applied,
		Attributes: response.Attributes,
	}
}

func toonOpsStatus(response appsvc.OpsStatusResponse) toonOpsStatusOutput {
	metrics := floatMapValue(response.Metadata["metrics"])
	cursors := make([]toonSourceCursor, 0, len(response.Cursors.Cursors))
	for _, cursor := range response.Cursors.Cursors {
		cursors = append(cursors, toonSourceCursor{
			UpdatedAt:  formatTime(cursor.UpdatedAt),
			SourceKind: cursor.SourceKind,
			SourceName: cursor.SourceName,
			Cursor:     cursor.Cursor,
		})
	}

	return toonOpsStatusOutput{
		Tool:   "ops_status",
		Counts: response.Counts,
		Scheduler: toonSchedulerStatus{
			Pending:    response.Scheduler.Pending,
			Retry:      response.Scheduler.Retry,
			Failed:     response.Scheduler.Failed,
			DeadLetter: response.Scheduler.DeadLetter,
		},
		SourceCursors: cursors,
		Errors:        response.Errors,
		Metrics:       metrics,
	}
}

func pendingState(analysisPending, projectionPending bool) string {
	switch {
	case analysisPending && projectionPending:
		return "analysis,projection"
	case analysisPending:
		return "analysis"
	case projectionPending:
		return "projection"
	default:
		return ""
	}
}

func toonTitles(titles []contracts.Title) []string {
	values := make([]string, 0, len(titles))
	for _, title := range titles {
		if title.Value != "" {
			values = append(values, title.Value)
		}
	}

	return values
}

func sortParts(parts []toonPartRow) {
	sort.SliceStable(parts, func(left, right int) bool {
		if parts[left].Role != parts[right].Role {
			return parts[left].Role < parts[right].Role
		}
		if parts[left].Order != parts[right].Order {
			return parts[left].Order < parts[right].Order
		}

		return parts[left].Digest < parts[right].Digest
	})
}

func toonSearchFromMap(tool string, value map[string]any) (toonSearchOutput, bool) {
	rawResults, ok := listValue(value["results"])
	if !ok {
		return toonSearchOutput{}, false
	}
	results := make([]toonSearchResult, 0, len(rawResults))
	for index, rawResult := range rawResults {
		row, ok := mapValue(rawResult)
		if !ok {
			continue
		}
		results = append(results, toonSearchResult{
			Rank:    index + 1,
			Digest:  stringValue(row["object_digest"]),
			Message: toonMessageFromValue(row["message"]),
			Score:   floatValue(row["score"]),
			Title:   stringValue(row["title"]),
			Snippet: stringValue(row["snippet"]),
			Facets:  strings.Join(stringListValue(row["facets"]), "|"),
			Pending: pendingState(
				boolValue(row["analysis_pending"]),
				boolValue(row["projection_pending"]),
			),
		})
	}

	return toonSearchOutput{
		Tool:     tool,
		Total:    intValue(value["total"]),
		Returned: len(results),
		Results:  results,
	}, true
}

func toonRetrieveFromMap(value map[string]any) (toonRetrieveOutput, bool) {
	manifest, ok := mapValue(value["manifest"])
	if !ok {
		return toonRetrieveOutput{}, false
	}

	return toonRetrieveOutput{
		Tool:        "object_retrieve",
		Digest:      stringValue(manifest["digest"]),
		ObjectID:    stringValue(manifest["object_id"]),
		MediaType:   stringValue(manifest["media_type"]),
		Size:        int64Value(manifest["size"]),
		Compression: stringValue(manifest["compression"]),
		CreatedAt:   stringValue(manifest["created_at"]),
		UpdatedAt:   stringValue(manifest["updated_at"]),
		Facets:      toonFacetRowsFromValue(manifest["facets"]),
		Titles:      toonTitlesFromValue(manifest["titles"]),
		Provenance:  toonProvenanceRowsFromValue(manifest["provenance"]),
		Compound:    toonCompoundFromValue(manifest["compound"], false),
		Content:     stringValue(value["content"]),
		Message:     toonMessageFromValue(value["message"]),
	}, true
}

func toonMessageFromValue(value any) *toonMessage {
	raw, ok := mapValue(value)
	if !ok {
		return nil
	}
	headers, _ := mapValue(raw["selected_headers"])
	graph, _ := mapValue(raw["graph"])
	source, _ := mapValue(graph["source"])

	links := []toonGraphLink{}
	for _, rawLink := range listValueOrEmpty(graph["links"]) {
		link, ok := mapValue(rawLink)
		if !ok {
			continue
		}
		links = append(links, toonGraphLink{
			Rel: stringValue(link["rel"]),
			URL: stringValue(link["url"]),
		})
	}

	attachments := []toonAttachment{}
	for _, rawAttachment := range listValueOrEmpty(raw["attachments"]) {
		attachment, ok := mapValue(rawAttachment)
		if !ok {
			continue
		}
		attachments = append(attachments, toonAttachment{
			RetrievalID: stringValue(attachment["retrieval_id"]),
			Filename:    stringValue(attachment["filename"]),
			MediaType:   stringValue(attachment["media_type"]),
			Summary:     stringValue(attachment["summary"]),
		})
	}

	return &toonMessage{
		MessageID: stringValue(raw["message_id"]),
		Digest:    stringValue(raw["digest"]),
		ObjectID:  stringValue(raw["object_id"]),
		ThreadID:  stringValue(raw["thread_id"]),
		SelectedHeaders: toonSelectedHeaders{
			Date:    stringValue(headers["date"]),
			From:    stringValue(headers["from"]),
			To:      stringValue(headers["to"]),
			Cc:      stringValue(headers["cc"]),
			Subject: stringValue(headers["subject"]),
		},
		Summary:    stringValue(raw["summary"]),
		Bullets:    stringListValue(raw["bullets"]),
		Categories: stringListValue(raw["categories"]),
		Graph: toonMessageGraph{
			Source:  source,
			Topics:  stringListValue(graph["topics"]),
			Actions: stringListValue(graph["actions"]),
			Links:   links,
		},
		Body:        stringValue(raw["body"]),
		Attachments: attachments,
	}
}

func toonGraphFromMap(value map[string]any) (toonGraphOutput, bool) {
	rawFacts, ok := listValue(value["facts"])
	if !ok {
		return toonGraphOutput{}, false
	}
	facts := make([]toonFactRow, 0, len(rawFacts))
	for _, rawFact := range rawFacts {
		fact, ok := mapValue(rawFact)
		if !ok {
			continue
		}
		facts = append(facts, toonFactRow{
			Subject:   stringValue(fact["subject"]),
			Predicate: stringValue(fact["predicate"]),
			Object:    stringValue(fact["object"]),
			Metadata:  anyMapValue(fact["metadata"]),
		})
	}

	return toonGraphOutput{
		Tool:     "graph_explore",
		Returned: len(facts),
		Facts:    facts,
	}, true
}

func toonAnalysisStatusFromMap(value map[string]any) (toonAnalysisOutput, bool) {
	rawStatuses, ok := listValue(value["statuses"])
	if !ok {
		return toonAnalysisOutput{}, false
	}
	statuses := make([]toonAnalysisStatusRow, 0, len(rawStatuses))
	for _, rawStatus := range rawStatuses {
		status, ok := mapValue(rawStatus)
		if !ok {
			continue
		}
		statuses = append(statuses, toonAnalysisStatusRow{
			Digest:      stringValue(status["object_digest"]),
			Analyzer:    stringValue(status["analyzer_name"]),
			Version:     stringValue(status["analyzer_version"]),
			Status:      stringValue(status["status"]),
			GeneratedAt: stringValue(status["generated_at"]),
			Data:        anyMapValue(status["data"]),
		})
	}

	return toonAnalysisOutput{
		Tool:     "analysis_status",
		Returned: len(statuses),
		Statuses: statuses,
	}, true
}

func toonForceAnalysisFromMap(value map[string]any) (toonForceAnalysisOutput, bool) {
	if _, ok := value["scanned"]; !ok {
		return toonForceAnalysisOutput{}, false
	}

	return toonForceAnalysisOutput{
		Tool:     "force_analysis",
		Scanned:  intValue(value["scanned"]),
		Enqueued: intValue(value["enqueued"]),
		Skipped:  intValue(value["skipped"]),
		Failed:   intValue(value["failed"]),
	}, true
}

func toonFacetRowsFromValue(value any) []toonFacetRow {
	rawFacets, ok := listValue(value)
	if !ok {
		return nil
	}
	facets := make([]toonFacetRow, 0, len(rawFacets))
	for _, rawFacet := range rawFacets {
		facet, ok := mapValue(rawFacet)
		if !ok {
			continue
		}
		facets = append(facets, toonFacetRow{
			Kind:    stringValue(facet["kind"]),
			Name:    stringValue(facet["name"]),
			Version: stringValue(facet["version"]),
			Data:    firstMapValue(facet["attributes"], facet["metadata"]),
		})
	}

	return facets
}

func toonTitlesFromValue(value any) []string {
	rawTitles, ok := listValue(value)
	if !ok {
		return nil
	}
	titles := make([]string, 0, len(rawTitles))
	for _, rawTitle := range rawTitles {
		title, ok := mapValue(rawTitle)
		if !ok {
			continue
		}
		if value := stringValue(title["value"]); value != "" {
			titles = append(titles, value)
		}
	}

	return titles
}

func toonProvenanceRowsFromValue(value any) []toonProvenanceRow {
	rawProvenance, ok := listValue(value)
	if !ok {
		return nil
	}
	provenance := make([]toonProvenanceRow, 0, len(rawProvenance))
	for _, rawEntry := range rawProvenance {
		entry, ok := mapValue(rawEntry)
		if !ok {
			continue
		}
		provenance = append(provenance, toonProvenanceRow{
			ObservedAt:      stringValue(entry["observed_at"]),
			SourceKind:      stringValue(entry["source_kind"]),
			SourceName:      stringValue(entry["source_name"]),
			ExternalID:      stringValue(entry["external_id"]),
			ExternalVersion: stringValue(entry["external_version"]),
		})
	}

	return provenance
}

func toonCompoundFromValue(value any, includeTool bool) toonCompoundOutput {
	compound, ok := mapValue(value)
	if !ok {
		return toonCompoundOutput{}
	}
	rawParts, _ := listValue(compound["parts"])
	parts := make([]toonPartRow, 0, len(rawParts))
	for _, rawPart := range rawParts {
		part, ok := mapValue(rawPart)
		if !ok {
			continue
		}
		parts = append(parts, toonPartRow{
			Role:     stringValue(part["role"]),
			Digest:   stringValue(part["digest"]),
			Order:    intValue(part["order"]),
			Required: boolValue(part["required"]),
		})
	}
	sortParts(parts)

	output := toonCompoundOutput{
		IsCompound: boolValue(compound["is_compound"]),
		Parts:      parts,
	}
	if includeTool {
		output.Tool = "compound_expand"
	}

	return output
}

func firstMapValue(values ...any) map[string]any {
	for _, value := range values {
		if mapped := anyMapValue(value); len(mapped) > 0 {
			return mapped
		}
	}

	return nil
}

func anyMapValue(value any) map[string]any {
	mapped, _ := mapValue(value)

	return mapped
}

func floatMapValue(value any) map[string]float64 {
	if direct, ok := value.(map[string]float64); ok {
		return direct
	}
	raw, ok := mapValue(value)
	if !ok {
		return nil
	}
	result := make(map[string]float64, len(raw))
	for key, value := range raw {
		if value, ok := numberValue(value); ok {
			result[key] = value
		}
	}

	return result
}

func mapValue(value any) (map[string]any, bool) {
	mapped, ok := value.(map[string]any)

	return mapped, ok
}

func listValue(value any) ([]any, bool) {
	list, ok := value.([]any)

	return list, ok
}

func listValueOrEmpty(value any) []any {
	list, ok := listValue(value)
	if !ok {
		return nil
	}

	return list
}

func stringListValue(value any) []string {
	rawValues, ok := listValue(value)
	if !ok {
		return nil
	}
	values := make([]string, 0, len(rawValues))
	for _, rawValue := range rawValues {
		if value := stringValue(rawValue); value != "" {
			values = append(values, value)
		}
	}

	return values
}

func stringValue(value any) string {
	if value, ok := value.(string); ok {
		return value
	}

	return ""
}

func boolValue(value any) bool {
	if value, ok := value.(bool); ok {
		return value
	}

	return false
}

func floatValue(value any) float64 {
	number, _ := numberValue(value)

	return number
}

func numberValue(value any) (float64, bool) {
	switch value := value.(type) {
	case float64:
		return value, true
	case float32:
		return float64(value), true
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case int32:
		return float64(value), true
	default:
		return 0, false
	}
}

func intValue(value any) int {
	return int(floatValue(value))
}

func int64Value(value any) int64 {
	return int64(floatValue(value))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339Nano)
}
