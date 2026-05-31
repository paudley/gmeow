// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

// TestExternalAnalyzerUnavailableClassification exercises the failure
// classification through the full Analyze path now that external analyzers run as
// persistent backends. The boundary moved from per-object exit codes to backend
// lifecycle: a backend that never becomes ready is genuinely down (park ⇒
// ErrAnalyzerUnavailable), while a ready backend that dies mid-request is a
// job-specific failure (bounded retry ⇒ ordinary error), so a poison object can
// never wedge the tag.
func TestExternalAnalyzerUnavailableClassification(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "external analyzer availability classification")

	cases := []struct {
		name        string
		command     string
		args        []string
		unavailable bool
	}{
		{
			name:        "command cannot launch is unavailable",
			command:     "gmeow-nonexistent-analyzer-xyz",
			unavailable: true,
		},
		{
			name:    "starts but never reports ready is unavailable",
			command: os.Args[0],
			args: []string{
				"-test.run=TestHelperProcess",
				"--",
				backendModeFlag,
				"no-ready",
			},
			unavailable: true,
		},
		{
			name:    "ready then crashes mid-request is job-specific",
			command: os.Args[0],
			args: []string{
				"-test.run=TestHelperProcess",
				"--",
				backendModeFlag,
				"crash-on-request",
			},
			unavailable: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewBackendManager(BackendManagerConfig{})
			t.Cleanup(manager.Close)

			analyzer, err := NewExternalCommandAnalyzer(ExternalCommandConfig{
				Spec:           contracts.AnalyzerSpec{Name: "categories.sklearn", Version: "v1"},
				Command:        testCase.command,
				Args:           testCase.args,
				StartupTimeout: 500 * time.Millisecond,
				Manager:        manager,
			})
			if err != nil {
				t.Fatal(err)
			}

			_, err = analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
			if err == nil {
				t.Fatal("expected the external analyzer to fail")
			}
			if got := errors.Is(err, ErrAnalyzerUnavailable); got != testCase.unavailable {
				t.Fatalf(
					"ErrAnalyzerUnavailable = %t, want %t (err=%v)",
					got, testCase.unavailable, err,
				)
			}
		})
	}
}
