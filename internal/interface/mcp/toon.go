// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"encoding/json"
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
	Rank    int     `toon:"rank"`
	Digest  string  `toon:"digest"`
	Score   float64 `toon:"score"`
	Title   string  `toon:"title"`
	Snippet string  `toon:"snippet"`
	Facets  string  `toon:"facets"`
	Pending string  `toon:"pending"`
}

type toonRetrieveOutput struct {
	Tool        string              `toon:"tool"`
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
		var response appsvc.ObjectSearchResponse
		if decodeMap(result, &response) == nil {
			return toonSearch("mail_search", appsvc.SearchOptions{}, response)
		}
	case "object_retrieve":
		var response appsvc.RetrieveResponse
		if decodeMap(result, &response) == nil {
			return toonRetrieve(response)
		}
	case "graph_explore":
		var response contracts.GraphResponse
		if decodeMap(result, &response) == nil {
			return toonGraph(contracts.GraphRequest{}, response)
		}
	case "analysis_status":
		var response contracts.AnalysisStatusResponse
		if decodeMap(result, &response) == nil {
			return toonAnalysisStatus(response)
		}
	case "force_analysis":
		var response contracts.SchedulerScanResponse
		if decodeMap(result, &response) == nil {
			return toonForceAnalysis(appsvc.ForceAnalysisRequest{}, response)
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
	metrics, _ := response.Metadata["metrics"].(map[string]float64)
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

func decodeMap(value map[string]any, output any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return json.Unmarshal(encoded, output)
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
