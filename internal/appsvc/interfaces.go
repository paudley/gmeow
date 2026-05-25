// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"

	"blackat.ca/gmeow/internal/contracts"
)

type SearchService interface {
	Search(
		ctx context.Context,
		request contracts.SearchRequest,
	) (contracts.SearchResponse, error)
}

type RetrievalService interface {
	Manifest(
		ctx context.Context,
		digest contracts.ObjectDigest,
	) (contracts.Manifest, error)
}

type AnalysisService interface {
	Analyze(
		ctx context.Context,
		digest contracts.ObjectDigest,
		analyzer string,
		forced bool,
	) (contracts.AnalyzerJob, error)
}
