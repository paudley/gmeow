// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

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

var ErrUnsupportedOperation = errors.New("unsupported source operation")

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
	ReadManifest(context.Context, contracts.ObjectDigest) (contracts.Manifest, error)
	AttachProvenance(context.Context, contracts.ObjectDigest, []contracts.Provenance) error
	PutCompound(context.Context, rpc.CompoundPutRequest) (contracts.ObjectDigest, error)
	WriteSourceCursor(context.Context, contracts.SourceCursor) error
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
	ObservedAt    time.Time
	Reader        io.Reader
	Compound      *CompoundObject
	MediaType     string
	SourceKind    string
	SourceName    string
	ExternalID    string
	ExternalVer   string
	SourceHint    string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
}

type CompoundObject struct {
	ObjectID      string
	MediaType     string
	SourceHint    string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
	Parts         []contracts.CompoundPart
}

type Service struct {
	store FilestoreClient
}

type IngestService interface {
	Ingest(ctx context.Context, object IngestObject) (contracts.ObjectDigest, bool, error)
	LookupSourceObject(ctx context.Context, ref contracts.SourceObjectRef) (contracts.ObjectDigest, bool, error)
}

func NewService(store FilestoreClient) (*Service, error) {
	if store == nil {
		return nil, errors.New("source filestore client is required")
	}

	return &Service{store: store}, nil
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
		if digest, found, err := service.store.LookupSourceObject(ctx, ref); err != nil {
			return "", false, err
		} else if found {
			if len(provenance) > 0 {
				if err := service.store.AttachProvenance(ctx, digest, provenance); err != nil {
					return "", false, err
				}
			}

			return digest, false, nil
		}
	}

	var claim contracts.SourceIngestClaim
	if hasRef {
		var acquired bool
		claim, acquired, err = service.store.TryAcquireSourceIngest(ctx, ref)
		if err != nil {
			return "", false, err
		}

		if !acquired {
			return "", false, fmt.Errorf(
				"source ingest already in progress for %s/%s/%s",
				ref.SourceKind,
				ref.SourceName,
				ref.ExternalID,
			)
		}

		defer service.releaseClaim(ctx, claim)
	}

	if object.Compound != nil {
		compound := *object.Compound
		compound.Provenance = mergeProvenance(compound.Provenance, provenance)
		digest, err := service.store.PutCompound(ctx, rpc.CompoundPutRequest(compound))

		return digest, true, err
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
		ContentRoles:  append([]string{}, object.ContentRoles...),
		Facets:        append([]contracts.Facet{}, object.Facets...),
		Provenance:    mergeProvenance(object.Provenance, provenance),
		Relationships: append([]contracts.Relationship{}, object.Relationships...),
	})

	return digest, true, err
}

func (service *Service) LookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	return service.store.LookupSourceObject(ctx, ref)
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
