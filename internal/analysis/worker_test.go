// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestRuntimeProcessWritesDurableAnnotationIdempotently(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "hello phase four")
	analyzer := &countingAnalyzer{
		spec: contracts.AnalyzerSpec{
			Name:    "quality.checked",
			Version: "v1",
		},
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(
		&blockingSource{},
		store,
		registry,
		WithClock(func() time.Time {
			return time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	job := analyzerJob(digest, analyzer.spec)
	if err := runtime.Process(ctx, job); err != nil {
		t.Fatal(err)
	}
	if analyzer.calls != 1 {
		t.Fatalf("expected first process to call analyzer once, got %d", analyzer.calls)
	}
	annotation := findAnalysisAnnotation(t, ctx, store, digest, analyzer.spec.Name)
	if annotation.AnalyzerVer != "v1" {
		t.Fatalf("expected analyzer version v1, got %q", annotation.AnalyzerVer)
	}
	if annotation.Data["status"] != "complete" {
		t.Fatalf("expected complete annotation, got %#v", annotation.Data)
	}
	if annotation.Data["idempotency_key"] != job.IdempotencyKey {
		t.Fatalf("expected idempotency key in annotation, got %#v", annotation.Data)
	}
	if err := runtime.Process(ctx, job); err != nil {
		t.Fatal(err)
	}
	if analyzer.calls != 2 {
		t.Fatalf(
			"expected repeated job to remain runnable and idempotent, got %d calls",
			analyzer.calls,
		)
	}
	again := findAnalysisAnnotation(t, ctx, store, digest, analyzer.spec.Name)
	if again.Data["idempotency_key"] != job.IdempotencyKey {
		t.Fatalf("expected stable idempotency key after repeat, got %#v", again.Data)
	}
}

func TestRuntimeRefusesUnregisteredAnalyzerInsteadOfFallback(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "quality must not degrade")
	registry, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Process(ctx, analyzerJob(digest, contracts.AnalyzerSpec{
		Name:       "ner.spacy",
		Version:    "python-current",
		WorkerKind: "python",
	}))
	if err == nil {
		t.Fatal("expected unregistered Python analyzer to be rejected")
	}
	if !strings.Contains(err.Error(), "refusing lower-quality fallback") {
		t.Fatalf("expected quality fallback error, got %v", err)
	}
}

func TestRuntimeHandleAcksOnlyAfterSuccessfulWrite(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "ack after write")
	analyzer := &countingAnalyzer{
		spec: contracts.AnalyzerSpec{Name: "ack.checked", Version: "v1"},
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &memoryReceipt{job: analyzerJob(digest, analyzer.spec)}
	if err := runtime.Handle(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.acked {
		t.Fatal("expected successful job to be acked")
	}
	if receipt.retried {
		t.Fatal("successful job must not be retried")
	}
	_ = findAnalysisAnnotation(t, ctx, store, digest, analyzer.spec.Name)
}

func TestRuntimeHandleRoutesAnalyzerFailureWithoutAck(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "retry failure")
	analyzer := &countingAnalyzer{
		spec: contracts.AnalyzerSpec{Name: "fail.checked", Version: "v1"},
		err:  errors.New("model unavailable"),
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &memoryReceipt{job: analyzerJob(digest, analyzer.spec)}
	if err := runtime.Handle(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.acked {
		t.Fatal("failed job must not be acked as success")
	}
	if !receipt.retried {
		t.Fatal("failed job should be routed for retry")
	}
}

type countingAnalyzer struct {
	spec  contracts.AnalyzerSpec
	err   error
	calls int
}

func (analyzer *countingAnalyzer) Spec() contracts.AnalyzerSpec {
	return analyzer.spec
}

func (analyzer *countingAnalyzer) Analyze(
	context.Context,
	ObjectStore,
	contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	analyzer.calls++
	if analyzer.err != nil {
		return contracts.Annotation{}, analyzer.err
	}
	return contracts.Annotation{Data: map[string]any{"payload": "ok"}}, nil
}

type blockingSource struct{}

func (blockingSource) Receive(ctx context.Context) (JobReceipt, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type memoryReceipt struct {
	job     contracts.AnalyzerJob
	acked   bool
	retried bool
}

func (receipt *memoryReceipt) Job() contracts.AnalyzerJob {
	return receipt.job
}

func (receipt *memoryReceipt) Ack(context.Context) error {
	receipt.acked = true
	return nil
}

func (receipt *memoryReceipt) Retry(context.Context, error) error {
	receipt.retried = true
	return nil
}

func putTextObject(
	t *testing.T,
	ctx context.Context,
	store filestore.Store,
	text string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(text),
		MediaType: "text/plain",
		Facets: []contracts.Facet{{
			Kind:    "file",
			Version: "1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func analyzerJob(
	digest contracts.ObjectDigest,
	spec contracts.AnalyzerSpec,
) contracts.AnalyzerJob {
	job := contracts.AnalyzerJob{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		Analyzer:      spec,
	}
	job.IdempotencyKey = string(digest) + ":" + spec.Name + ":" + spec.Version
	job.JobID = job.IdempotencyKey
	return job
}

func findAnalysisAnnotation(
	t *testing.T,
	ctx context.Context,
	store filestore.Store,
	digest contracts.ObjectDigest,
	name string,
) contracts.Annotation {
	t.Helper()
	var found contracts.Annotation
	errStop := errors.New("stop")
	err := store.WalkProjection(ctx, func(object filestore.ProjectionObject) error {
		if object.Manifest.ObjectDigest != digest {
			return nil
		}
		for _, annotation := range object.Annotations {
			if annotation.Kind == "analysis" && annotation.AnalyzerName == name {
				found = annotation
				return errStop
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		t.Fatal(err)
	}
	if found.AnalyzerName == "" {
		t.Fatalf("analysis annotation %q not found", name)
	}
	return found
}
