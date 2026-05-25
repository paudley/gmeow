// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package memory

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"

	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
	"blackat.ca/gmeow/internal/query"
)

type Index struct {
	source  query.ProjectionSource
	mutex   sync.RWMutex
	objects map[contracts.ObjectDigest]projectedObject
}

type projectedObject struct {
	manifest    contracts.Manifest
	annotations []contracts.Annotation
}

func New(source query.ProjectionSource) *Index {
	return &Index{
		source:  source,
		objects: map[contracts.ObjectDigest]projectedObject{},
	}
}

func (index *Index) Project(
	ctx context.Context,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	return index.ProjectObject(ctx, filestore.ProjectionObject{
		Digest:      manifest.ObjectDigest,
		Manifest:    manifest,
		Annotations: annotations,
	})
}

func (index *Index) ProjectObject(
	ctx context.Context,
	object filestore.ProjectionObject,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(object.Findings) > 0 {
		return nil
	}
	index.mutex.Lock()
	defer index.mutex.Unlock()
	index.objects[object.Manifest.ObjectDigest] = projectedObject{
		manifest:    object.Manifest,
		annotations: append([]contracts.Annotation(nil), object.Annotations...),
	}
	return nil
}

func (index *Index) Rebuild(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	index.mutex.Lock()
	index.objects = map[contracts.ObjectDigest]projectedObject{}
	index.mutex.Unlock()
	if index.source == nil {
		return nil
	}
	return index.source.WalkProjection(ctx, func(object filestore.ProjectionObject) error {
		return index.ProjectObject(ctx, object)
	})
}

func (index *Index) Search(
	ctx context.Context,
	request contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	if err := ctx.Err(); err != nil {
		return contracts.SearchResponse{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	results := []contracts.SearchResult{}
	for _, object := range index.sortedObjects() {
		if !matchesSearch(object, request) {
			continue
		}
		results = append(results, contracts.SearchResult{
			ObjectDigest: object.manifest.ObjectDigest,
			Score:        1,
			Title:        projectedTitle(object.manifest),
			Facets:       facetKinds(object.manifest.Facets),
			Attributes: map[string]any{
				"object_id":  object.manifest.ObjectID,
				"media_type": object.manifest.MediaType,
			},
		})
	}
	return contracts.SearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       pageSearchResults(results, request.Offset, request.Limit),
		Total:         len(results),
	}, nil
}

func (index *Index) Structure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Structure{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	object := index.objects[digest]
	structure := contracts.Structure{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		ObjectID:      object.manifest.ObjectID,
		Facets:        facetKinds(object.manifest.Facets),
		PartsByRole:   map[string][]contracts.StructurePart{},
	}
	for _, part := range object.manifest.Compound.Parts {
		child := index.objects[part.Digest]
		structure.PartsByRole[part.Role] = append(
			structure.PartsByRole[part.Role],
			contracts.StructurePart{
				Digest:   part.Digest,
				Role:     part.Role,
				Order:    part.Order,
				Required: part.Required,
				Facets:   facetKinds(child.manifest.Facets),
				Metadata: part.Metadata,
			},
		)
	}
	return structure, nil
}

func (index *Index) Relationships(
	ctx context.Context,
	request contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	if err := ctx.Err(); err != nil {
		return contracts.RelationshipResponse{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	relationships := []contracts.Relationship{}
	for _, object := range index.sortedObjects() {
		for _, relationship := range object.manifest.Relationships {
			if matchesRelationship(relationship, request.Filter) {
				relationships = append(relationships, relationship)
			}
		}
	}
	limit := normalizedLimit(request.Limit)
	if len(relationships) > limit {
		relationships = relationships[:limit]
	}
	return contracts.RelationshipResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Relationships: relationships,
	}, nil
}

func (index *Index) Graph(
	ctx context.Context,
	request contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	if err := ctx.Err(); err != nil {
		return contracts.GraphResponse{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	facts := []contracts.GraphFact{}
	for _, object := range index.sortedObjects() {
		for _, fact := range object.manifest.Graph {
			if request.Node != "" && fact.Subject != request.Node &&
				fact.Object != request.Node {
				continue
			}
			if request.Predicate != "" && fact.Predicate != request.Predicate {
				continue
			}
			facts = append(facts, fact)
		}
	}
	limit := normalizedLimit(request.Limit)
	if len(facts) > limit {
		facts = facts[:limit]
	}
	return contracts.GraphResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Facts:         facts,
	}, nil
}

func (index *Index) AnalysisStatus(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return contracts.AnalysisStatusResponse{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	statuses := []contracts.AnalysisStatus{}
	for _, object := range index.sortedObjects() {
		if len(request.ObjectDigests) > 0 &&
			!containsDigest(request.ObjectDigests, object.manifest.ObjectDigest) {
			continue
		}
		for _, annotation := range object.annotations {
			if annotation.Kind != "analysis" {
				continue
			}
			if len(request.AnalyzerNames) > 0 &&
				!containsString(request.AnalyzerNames, annotation.AnalyzerName) {
				continue
			}
			status := "complete"
			if value, ok := annotation.Data["status"].(string); ok && value != "" {
				status = value
			}
			statuses = append(statuses, contracts.AnalysisStatus{
				ObjectDigest: annotation.ObjectDigest,
				AnalyzerName: annotation.AnalyzerName,
				AnalyzerVer:  annotation.AnalyzerVer,
				Status:       status,
				GeneratedAt:  annotation.GeneratedAt,
				Data:         annotation.Data,
			})
		}
	}
	limit := normalizedLimit(request.Limit)
	if len(statuses) > limit {
		statuses = statuses[:limit]
	}
	return contracts.AnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Statuses:      statuses,
	}, nil
}

func (index *Index) VectorSearch(
	ctx context.Context,
	request contracts.VectorSearchRequest,
) (contracts.VectorSearchResponse, error) {
	if err := ctx.Err(); err != nil {
		return contracts.VectorSearchResponse{}, err
	}
	index.mutex.RLock()
	defer index.mutex.RUnlock()
	results := []contracts.VectorSearchResult{}
	for _, object := range index.sortedObjects() {
		if len(request.Facets) > 0 && !hasAnyFacet(object.manifest.Facets, request.Facets) {
			continue
		}
		for _, embedding := range object.manifest.Embeddings {
			if request.Model != "" && embedding.Model != request.Model {
				continue
			}
			if request.Dimensions > 0 && embedding.Dimensions != request.Dimensions {
				continue
			}
			results = append(results, contracts.VectorSearchResult{
				ObjectDigest: object.manifest.ObjectDigest,
				Model:        embedding.Model,
				Distance:     0,
			})
		}
	}
	sort.SliceStable(results, func(left, right int) bool {
		if math.Abs(results[left].Distance-results[right].Distance) > 0 {
			return results[left].Distance < results[right].Distance
		}
		return results[left].ObjectDigest < results[right].ObjectDigest
	})
	limit := normalizedLimit(request.Limit)
	if len(results) > limit {
		results = results[:limit]
	}
	return contracts.VectorSearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
	}, nil
}

func (index *Index) sortedObjects() []projectedObject {
	objects := make([]projectedObject, 0, len(index.objects))
	for _, object := range index.objects {
		objects = append(objects, object)
	}
	sort.SliceStable(objects, func(left, right int) bool {
		return objects[left].manifest.ObjectDigest < objects[right].manifest.ObjectDigest
	})
	return objects
}

func matchesSearch(object projectedObject, request contracts.SearchRequest) bool {
	if len(request.Facets) > 0 && !hasAnyFacet(object.manifest.Facets, request.Facets) {
		return false
	}
	if len(request.MediaTypes) > 0 &&
		!containsString(request.MediaTypes, object.manifest.MediaType) {
		return false
	}
	if !matchesProvenance(object.manifest.Provenance, request.Provenance) {
		return false
	}
	if !matchesAnyRelationship(object.manifest.Relationships, request.Relationships) {
		return false
	}
	if len(request.CompoundRoles) > 0 &&
		!matchesCompoundRoles(object.manifest.Compound.Parts, request.CompoundRoles) {
		return false
	}
	needle := strings.ToLower(strings.TrimSpace(request.Query))
	if needle == "" {
		return true
	}
	return strings.Contains(strings.ToLower(searchableText(object)), needle)
}

func searchableText(object projectedObject) string {
	encoded, _ := json.Marshal(struct {
		Manifest    contracts.Manifest     `json:"manifest"`
		Annotations []contracts.Annotation `json:"annotations"`
	}{
		Manifest:    object.manifest,
		Annotations: object.annotations,
	})
	return string(encoded)
}

func projectedTitle(manifest contracts.Manifest) string {
	if len(manifest.Titles) > 0 {
		return manifest.Titles[0].Value
	}
	return manifest.ObjectID
}

func facetKinds(facets []contracts.Facet) []string {
	values := make([]string, 0, len(facets))
	for _, facet := range facets {
		if facet.Kind != "" {
			values = append(values, facet.Kind)
		}
	}
	sort.Strings(values)
	return values
}

func hasAnyFacet(facets []contracts.Facet, wanted []string) bool {
	for _, facet := range facets {
		if containsString(wanted, facet.Kind) || containsString(wanted, facet.Name) {
			return true
		}
	}
	return false
}

func matchesProvenance(
	items []contracts.Provenance,
	filter contracts.ProvenanceFilter,
) bool {
	if len(filter.SourceKinds) == 0 && len(filter.SourceNames) == 0 &&
		len(filter.ExternalIDs) == 0 {
		return true
	}
	for _, item := range items {
		if len(filter.SourceKinds) > 0 &&
			!containsString(filter.SourceKinds, item.SourceKind) {
			continue
		}
		if len(filter.SourceNames) > 0 &&
			!containsString(filter.SourceNames, item.SourceName) {
			continue
		}
		if len(filter.ExternalIDs) > 0 &&
			!containsString(filter.ExternalIDs, item.ExternalID) {
			continue
		}
		return true
	}
	return false
}

func matchesAnyRelationship(
	relationships []contracts.Relationship,
	filter contracts.RelationshipFilter,
) bool {
	if len(filter.Types) == 0 && filter.From == "" && filter.To == "" &&
		len(filter.Roles) == 0 && filter.Any == "" {
		return true
	}
	for _, relationship := range relationships {
		if matchesRelationship(relationship, filter) {
			return true
		}
	}
	return false
}

func matchesRelationship(
	relationship contracts.Relationship,
	filter contracts.RelationshipFilter,
) bool {
	if len(filter.Types) > 0 && !containsString(filter.Types, relationship.Type) {
		return false
	}
	if filter.From != "" && relationship.From != filter.From {
		return false
	}
	if filter.To != "" && relationship.To != filter.To {
		return false
	}
	if len(filter.Roles) > 0 && !containsString(filter.Roles, relationship.Role) {
		return false
	}
	if filter.Any != "" && relationship.From != filter.Any &&
		relationship.To != filter.Any {
		return false
	}
	return true
}

func matchesCompoundRoles(parts []contracts.CompoundPart, roles []string) bool {
	for _, part := range parts {
		if containsString(roles, part.Role) {
			return true
		}
	}
	return false
}

func pageSearchResults(
	results []contracts.SearchResult,
	offset int,
	limit int,
) []contracts.SearchResult {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(results) {
		return nil
	}
	limit = normalizedLimit(limit)
	end := offset + limit
	if end > len(results) {
		end = len(results)
	}
	return results[offset:end]
}

func normalizedLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func containsDigest(
	values []contracts.ObjectDigest,
	value contracts.ObjectDigest,
) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

var _ query.Index = (*Index)(nil)
