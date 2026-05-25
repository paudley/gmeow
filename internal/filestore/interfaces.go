// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"context"
	"io"

	"blackat.ca/gmeow/internal/contracts"
)

type Store interface {
	Put(ctx context.Context, request PutRequest) (contracts.ObjectDigest, error)
	Open(ctx context.Context, digest contracts.ObjectDigest) (io.ReadCloser, error)
	ReadManifest(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Manifest, error)
	WriteAnnotation(ctx context.Context, annotation contracts.Annotation) error
}

type PutRequest struct {
	Reader     io.Reader
	MediaType  string
	Facets     []contracts.Facet
	Provenance []contracts.Provenance
}
