// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package query

import (
	"context"

	"blackat.ca/gmeow/internal/contracts"
)

type Index interface {
	Project(
		ctx context.Context,
		manifest contracts.Manifest,
		annotations []contracts.Annotation,
	) error
	Search(
		ctx context.Context,
		request contracts.SearchRequest,
	) (contracts.SearchResponse, error)
	Rebuild(ctx context.Context) error
}
