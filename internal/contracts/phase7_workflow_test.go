// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/query/memory"
)

func TestPhaseSevenLoadWorkflowIngestAnalysisProjectionAndSearch(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	index := memory.New(store)
	analyzer := fixedAnalyzer{spec: contracts.AnalyzerSpec{
		Name:    "summary.load",
		Version: "1",
	}}
	registry, err := analysis.NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := analysis.NewRuntime(blockingJobSource{}, store, registry)
	if err != nil {
		t.Fatal(err)
	}

	const objectCount = 64
	digests := make([]contracts.ObjectDigest, 0, objectCount)
	for offset := range objectCount {
		digest, err := store.Put(ctx, filestore.PutRequest{
			Reader: strings.NewReader(
				fmt.Sprintf("phase seven load object %02d searchable payload", offset),
			),
			Facets: []contracts.Facet{{Kind: "file"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		digests = append(digests, digest)
		if err := runtime.Process(ctx, contracts.AnalyzerJob{
			SchemaVersion:  contracts.SchemaVersionPhase00,
			JobID:          "load-job",
			IdempotencyKey: fmt.Sprintf("load-job-%02d", offset),
			ObjectDigest:   digest,
			Analyzer:       analyzer.spec,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := index.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := index.Search(ctx, contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Query:         "searchable",
		Limit:         objectCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != objectCount {
		t.Fatalf("expected load ingest to produce searchable objects, got %#v", response)
	}

	report, err := store.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != filestore.VerifyStatusOK {
		t.Fatalf("load workflow produced invalid FILESTORE: %#v", report)
	}

	metrics := observability.DefaultMetrics().Snapshot()
	if metrics["gmeow_analysis_latency_seconds_count"] == 0 {
		t.Fatalf("expected analysis latency metrics, got %#v", metrics)
	}
	_ = digests
}

type fixedAnalyzer struct {
	spec contracts.AnalyzerSpec
}

func (analyzer fixedAnalyzer) Spec() contracts.AnalyzerSpec {
	return analyzer.spec
}

func (fixedAnalyzer) Analyze(
	context.Context,
	analysis.ObjectStore,
	contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	return contracts.Annotation{Data: map[string]any{
		"summary": "phase seven load object searchable summary",
	}}, nil
}

type blockingJobSource struct{}

func (blockingJobSource) Receive(ctx context.Context) (analysis.JobReceipt, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}
