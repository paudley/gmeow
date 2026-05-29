// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

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
			name:        "clean non-zero exit is job-specific",
			command:     "sh",
			args:        []string{"-c", "exit 3"},
			unavailable: false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analyzer, err := NewExternalCommandAnalyzer(ExternalCommandConfig{
				Spec:    contracts.AnalyzerSpec{Name: "categories.sklearn", Version: "v1"},
				Command: testCase.command,
				Args:    testCase.args,
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
