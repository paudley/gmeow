// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"io"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type Store interface {
	LookupSourceObject(
		ctx context.Context,
		ref contracts.SourceObjectRef,
	) (contracts.ObjectDigest, bool, error)
	TryAcquireSourceIngest(
		ctx context.Context,
		ref contracts.SourceObjectRef,
	) (contracts.SourceIngestClaim, bool, error)
	ReleaseSourceIngest(
		ctx context.Context,
		claim contracts.SourceIngestClaim,
	) error
	Put(ctx context.Context, request PutRequest) (contracts.ObjectDigest, error)
	AttachProvenance(
		ctx context.Context,
		digest contracts.ObjectDigest,
		provenance []contracts.Provenance,
	) error
	PutCompound(
		ctx context.Context,
		request CompoundPutRequest,
	) (contracts.ObjectDigest, error)
	Open(ctx context.Context, digest contracts.ObjectDigest) (io.ReadCloser, error)
	ReadManifest(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Manifest, error)
	ProjectionObject(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (ProjectionObject, bool, error)
	GetStructure(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Structure, error)
	HasAnalysisAnnotation(
		ctx context.Context,
		digest contracts.ObjectDigest,
		analyzerName string,
		analyzerVersion string,
	) (bool, error)
	WriteAnnotation(ctx context.Context, annotation contracts.Annotation) error
	WriteOverlays(
		ctx context.Context,
		digest contracts.ObjectDigest,
		overlays map[string]any,
	) error
	WriteSourceCursor(ctx context.Context, cursor contracts.SourceCursor) error
	ReadSourceCursor(
		ctx context.Context,
		ref contracts.SourceCursorRef,
	) (contracts.SourceCursor, bool, error)
	WalkProjection(ctx context.Context, fn ProjectionFunc) error
	WalkChangedProjection(
		ctx context.Context,
		since time.Time,
		fn ProjectionFunc,
	) error
	WalkSourceCursors(ctx context.Context, fn SourceCursorProjectionFunc) error
	Verify(ctx context.Context) (VerifyReport, error)
	StorageBreakdown(
		ctx context.Context,
		request StorageBreakdownRequest,
	) (StorageBreakdownReport, error)
	ResolvePath(ctx context.Context, request PathResolveRequest) (PathResolveReport, error)
}

type ProjectionFinding struct {
	Digest  contracts.ObjectDigest `json:"digest,omitempty"`
	Path    string                 `json:"path,omitempty"`
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
}

type ProjectionObject struct {
	Digest      contracts.ObjectDigest
	Path        string
	Manifest    contracts.Manifest
	Annotations []contracts.Annotation
	Findings    []ProjectionFinding
}

type ProjectionFunc func(ProjectionObject) error

type SourceCursorProjectionFunc func(contracts.SourceCursor) error

type PutRequest struct {
	Reader        io.Reader
	MediaType     string
	SourceHint    string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
}

type CompoundPutRequest struct {
	ObjectID      string
	MediaType     string
	SourceHint    string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
	Parts         []contracts.CompoundPart
}

type VerifyStatus string

const (
	VerifyStatusOK    VerifyStatus = "ok"
	VerifyStatusError VerifyStatus = "error"
)

type VerifyFinding struct {
	Digest  contracts.ObjectDigest `json:"digest,omitempty"`
	Path    string                 `json:"path,omitempty"`
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
}

type VerifyReport struct {
	Status   VerifyStatus    `json:"status"`
	Findings []VerifyFinding `json:"findings,omitempty"`
	Checked  int             `json:"checked"`
}
