// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"blackat.ca/gmeow/internal/analysis"
	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
	"blackat.ca/gmeow/internal/query"
)

func TestPhase00FakeStackSmoke(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{manifests: map[contracts.ObjectDigest]contracts.Manifest{}}
	index := &fakeIndex{}
	analyzer := fakeAnalyzer{}

	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    bytes.NewBufferString("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Name: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	job := contracts.AnalyzerJob{
		SchemaVersion: contracts.SchemaVersionPhase00,
		JobID:         "job-1",
		Analyzer:      analyzer.Spec(),
		ObjectDigest:  digest,
		CreatedAt:     time.Now(),
	}
	annotation, err := analyzer.Analyze(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Project(
		ctx,
		manifest,
		[]contracts.Annotation{annotation},
	); err != nil {
		t.Fatal(err)
	}
	response, err := index.Search(
		ctx,
		contracts.SearchRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			Query:         "hello",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 {
		t.Fatalf("expected one projected result, got %d", response.Total)
	}

	var _ filestore.Store = store
	var _ query.Index = index
	var _ analysis.Analyzer = analyzer
}

type fakeStore struct {
	manifests map[contracts.ObjectDigest]contracts.Manifest
}

func (store *fakeStore) Put(
	ctx context.Context,
	request filestore.PutRequest,
) (contracts.ObjectDigest, error) {
	del, _ := io.ReadAll(request.Reader)
	_ = del
	now := time.Now()
	digest := contracts.ObjectDigest("fake-digest")
	store.manifests[digest] = contracts.Manifest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		MediaType:     request.MediaType,
		Facets:        request.Facets,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	_ = ctx
	return digest, nil
}

func (store *fakeStore) Open(
	context.Context,
	contracts.ObjectDigest,
) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBufferString("hello")), nil
}

func (store *fakeStore) ReadManifest(
	_ context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return store.manifests[digest], nil
}

func (store *fakeStore) WriteAnnotation(context.Context, contracts.Annotation) error {
	return nil
}

type fakeIndex struct {
	projected bool
}

func (index *fakeIndex) Project(
	_ context.Context,
	_ contracts.Manifest,
	_ []contracts.Annotation,
) error {
	index.projected = true
	return nil
}

func (index *fakeIndex) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	if !index.projected {
		return contracts.SearchResponse{SchemaVersion: contracts.SchemaVersionPhase00}, nil
	}
	return contracts.SearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results: []contracts.SearchResult{
			{ObjectDigest: "fake-digest", Title: "hello"},
		},
		Total: 1,
	}, nil
}

func (index *fakeIndex) Rebuild(context.Context) error {
	index.projected = true
	return nil
}

type fakeAnalyzer struct{}

func (fakeAnalyzer) Spec() contracts.AnalyzerSpec {
	return contracts.AnalyzerSpec{
		Name:       "fake",
		Version:    "phase00",
		Enabled:    true,
		WorkerKind: "go",
	}
}

func (fakeAnalyzer) Analyze(
	_ context.Context,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	return contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  job.ObjectDigest,
		AnalyzerName:  job.Analyzer.Name,
		AnalyzerVer:   job.Analyzer.Version,
		Kind:          "analysis",
		GeneratedAt:   time.Now(),
		Data:          map[string]any{"ok": true},
	}, nil
}
