// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"fmt"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

type DriveAdapter struct {
	name string
}

func NewDriveAdapter(name string) (*DriveAdapter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errRequired("drive source name")
	}

	return &DriveAdapter{name: name}, nil
}

func (adapter *DriveAdapter) Name() string {
	return adapter.name
}

func (*DriveAdapter) Kind() string {
	return "drive"
}

func (*DriveAdapter) Capabilities() []string {
	return []string{CapabilityExport}
}

func (adapter *DriveAdapter) Pull(
	context.Context,
	IngestService,
	PullRequest,
) ([]IngestObject, contracts.SourceCursor, error) {
	return nil, contracts.SourceCursor{}, adapter.unsupported("pull")
}

func (adapter *DriveAdapter) Hydrate(context.Context, string) (IngestObject, error) {
	return IngestObject{}, adapter.unsupported("hydrate")
}

func (adapter *DriveAdapter) LiveSearch(
	context.Context,
	LiveSearchRequest,
) ([]LiveSearchResult, error) {
	return nil, adapter.unsupported("live_search")
}

func (adapter *DriveAdapter) LiveRetrieve(
	context.Context,
	string,
) (IngestObject, error) {
	return IngestObject{}, adapter.unsupported("live_retrieve")
}

func (adapter *DriveAdapter) ApplyAction(
	context.Context,
	ActionRequest,
) (ActionResult, error) {
	return ActionResult{}, adapter.unsupported("actions")
}

func (adapter *DriveAdapter) unsupported(operation string) error {
	return fmt.Errorf(
		"%w: drive source %q operation %q is design-only in phase 05",
		ErrUnsupportedOperation,
		adapter.name,
		operation,
	)
}
