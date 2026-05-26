// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/config"
)

func TestRequireInstanceConfirmationRefusesProductionLikeInstance(t *testing.T) {
	loaded := &config.Loaded{
		Config: config.Config{
			System: config.SystemConfig{InstanceID: "local"},
		},
	}

	err := requireInstanceConfirmation(loaded, "query rebuild", "")
	if err == nil {
		t.Fatal("expected production-like instance confirmation error")
	}
	if !strings.Contains(err.Error(), "--confirm-instance local") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRequireInstanceConfirmationAllowsMatchingInstance(t *testing.T) {
	loaded := &config.Loaded{
		Config: config.Config{
			System: config.SystemConfig{InstanceID: "local"},
		},
	}

	if err := requireInstanceConfirmation(loaded, "query rebuild", "local"); err != nil {
		t.Fatal(err)
	}
}

func TestRequireInstanceConfirmationBypassesTestInstance(t *testing.T) {
	loaded := &config.Loaded{
		Config: config.Config{
			System: config.SystemConfig{InstanceID: "test"},
		},
	}

	if err := requireInstanceConfirmation(loaded, "query rebuild", ""); err != nil {
		t.Fatal(err)
	}
}
