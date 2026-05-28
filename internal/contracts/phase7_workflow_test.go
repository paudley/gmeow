// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestPhaseSevenLoadWorkflowIngestAnalysisProjectionAndSearch(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	defer queryService.Close()
	analyzer := fixedAnalyzer{spec: contracts.AnalyzerSpec{
		Name:    "summary.load",
		Version: "1",
	}}
	registry, err := analysis.NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	jobSource := analysisJobSource(t, ctx)
	runtime, err := analysis.NewRuntime(jobSource, filestoreService.Client, registry)
	if err != nil {
		t.Fatal(err)
	}

	const objectCount = 64
	digests := make([]contracts.ObjectDigest, 0, objectCount)
	for offset := range objectCount {
		digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
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

	if err := queryService.Client.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := queryService.Client.Search(ctx, contracts.SearchRequest{
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

	report, err := filestoreService.Store.Verify(ctx, filestore.VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if string(report.Status) != "ok" {
		t.Fatalf("load workflow produced invalid FILESTORE: %#v", report)
	}

	metrics := observability.DefaultMetrics().Snapshot()
	if metrics["gmeow_analysis_latency_seconds_count"] == 0 {
		t.Fatalf("expected analysis latency metrics, got %#v", metrics)
	}
	_ = digests
}

func TestPhaseSevenFilestoreRestoreDrillRebuildsSearchableProjection(t *testing.T) {
	ctx := context.Background()
	sourceService := testsupport.StartFilestoreGRPC(t, ctx)
	defer sourceService.Close()
	originalDigest, err := sourceService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader("phase seven restore drill searchable payload"),
		MediaType: "text/plain",
		Facets: []contracts.Facet{{
			Kind:    "file",
			Version: "1",
			Metadata: map[string]any{
				"display_name": "restore-drill.txt",
			},
		}},
		Provenance: []contracts.Provenance{{
			SourceKind: "filesystem",
			SourceName: "restore-drill",
			ExternalID: "restore-drill.txt",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceService.Client.WriteAnnotation(ctx, contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  originalDigest,
		Kind:          "analysis",
		AnalyzerName:  "summary.restore",
		AnalyzerVer:   "1",
		Data:          map[string]any{"status": "complete", "summary": "restored"},
	}); err != nil {
		t.Fatal(err)
	}

	restoredRoot := filepath.Join(t.TempDir(), "restored-filestore")
	copyTree(t, sourceService.Root, restoredRoot)
	restoredService := testsupport.StartFilestoreGRPCAt(t, ctx, restoredRoot)
	defer restoredService.Close()
	report, err := restoredService.Store.Verify(ctx, filestore.VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if string(report.Status) != "ok" || len(report.Findings) != 0 {
		t.Fatalf("restored FILESTORE did not verify cleanly: %#v", report)
	}

	queryService := testsupport.StartQueryGRPC(t, ctx, restoredService.Store)
	defer queryService.Close()
	if err := queryService.Client.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := queryService.Client.Search(ctx, contracts.SearchRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Query:         "restored",
		Limit:         10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || response.Results[0].ObjectDigest != originalDigest {
		t.Fatalf("restore drill did not rebuild searchable QUERY projection: %#v", response)
	}

	reader, err := restoredService.Client.Open(ctx, originalDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "phase seven restore drill searchable payload" {
		t.Fatalf("restored content mismatch: %q", content)
	}
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

func copyTree(t *testing.T, sourceRoot, targetRoot string) {
	t.Helper()
	err := filepath.WalkDir(
		sourceRoot,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(sourceRoot, path)
			if err != nil {
				return err
			}
			target := filepath.Join(targetRoot, relative)
			if entry.IsDir() {
				return os.MkdirAll(target, 0o755)
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			return copyFile(path, target, info.Mode())
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func copyFile(sourcePath, targetPath string, mode os.FileMode) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	target, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()

		return err
	}

	return target.Close()
}

func analysisJobSource(t *testing.T, ctx context.Context) analysis.JobSource {
	t.Helper()
	return testsupport.NewAnalysisJobSource(
		t,
		ctx,
		fmt.Sprintf("gmeow.test.analysis.%d.", time.Now().UnixNano()),
	)
}
