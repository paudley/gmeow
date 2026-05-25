// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"

	"blackat.ca/gmeow/internal/contracts"
)

type Adapter interface {
	Name() string
	Capabilities() []string
	Pull(ctx context.Context) ([]contracts.SourceEvent, error)
	Hydrate(ctx context.Context, externalID string) (contracts.SourceEvent, error)
}
