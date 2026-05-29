// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestHandleParksImmediatelyOnAnalyzerUnavailable(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "unavailable analyzer parks immediately")
	analyzer := &countingAnalyzer{
		spec: contracts.AnalyzerSpec{Name: "summary.model", Version: "v1"},
		err:  fmt.Errorf("call summary endpoint: %w", ErrAnalyzerUnavailable),
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry, WithParkBackoff(0))
	if err != nil {
		t.Fatal(err)
	}

	// The very first availability failure parks — without accumulating breaker
	// failures — so a worker restart (which resets the in-memory breaker) still
	// parks unavailable work instead of retrying it toward a dead-letter.
	receipt := &memoryReceipt{job: analyzerJob(digest, analyzer.spec)}
	if err := runtime.Handle(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.parked || receipt.retried {
		t.Fatalf(
			"expected immediate park on unavailable analyzer: parked=%t retried=%t",
			receipt.parked, receipt.retried,
		)
	}
	if analyzer.calls != 1 {
		t.Fatalf("expected one analyzer call, got %d", analyzer.calls)
	}
}

func TestCircuitBreakerOpensProbesAndCloses(t *testing.T) {
	clock := time.Now().UTC()
	breaker := newCircuitBreaker(func() time.Time { return clock })
	key := "ner.spacy@v1"

	// Failures below the threshold keep the circuit closed.
	for i := range defaultBreakerThreshold - 1 {
		if !breaker.allow(key) {
			t.Fatalf("circuit should be closed at %d failures", i)
		}
		breaker.recordFailure(key)
	}
	// The threshold-th failure trips the breaker open.
	if !breaker.allow(key) {
		t.Fatal("circuit should still allow the threshold-th attempt")
	}
	breaker.recordFailure(key)
	if breaker.allow(key) || !breaker.isOpen(key) {
		t.Fatal("circuit should be open after threshold consecutive failures")
	}

	// Inside the backoff window it stays closed to traffic.
	clock = clock.Add(defaultBreakerBaseBackoff - time.Second)
	if breaker.allow(key) {
		t.Fatal("circuit should stay open inside the backoff window")
	}

	// After the window it admits a single half-open probe.
	clock = clock.Add(2 * time.Second)
	if !breaker.allow(key) {
		t.Fatal("circuit should admit a half-open probe after the window")
	}
	// Probe fails -> re-open with a longer window.
	breaker.recordFailure(key)
	if breaker.allow(key) {
		t.Fatal("failed probe should re-open the circuit")
	}

	// After the longer window, a successful probe closes the circuit.
	clock = clock.Add(defaultBreakerMaxBackoff + time.Second)
	if !breaker.allow(key) {
		t.Fatal("circuit should admit a probe after the longer window")
	}
	breaker.recordSuccess(key)
	if !breaker.allow(key) || breaker.isOpen(key) {
		t.Fatal("successful probe should close the circuit")
	}
}

type manifestReadingAnalyzer struct {
	spec contracts.AnalyzerSpec
}

func (analyzer *manifestReadingAnalyzer) Spec() contracts.AnalyzerSpec {
	return analyzer.spec
}

func (analyzer *manifestReadingAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	if _, err := store.ReadManifest(ctx, job.ObjectDigest); err != nil {
		return contracts.Annotation{}, err
	}

	return contracts.Annotation{}, nil
}

func TestHandleDropsBogusJobWhenObjectMissing(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	analyzer := &manifestReadingAnalyzer{
		spec: contracts.AnalyzerSpec{Name: "needs.object", Version: "v1"},
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry, WithParkBackoff(0))
	if err != nil {
		t.Fatal(err)
	}

	// A digest that was never stored: the manifest read returns os.ErrNotExist.
	missing := contracts.ObjectDigest(strings.Repeat("ab", 32))
	receipt := &memoryReceipt{job: analyzerJob(missing, analyzer.spec)}

	if err := runtime.Handle(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.acked {
		t.Fatal("a job for a missing object should be dropped (acked)")
	}
	if receipt.retried || receipt.parked {
		t.Fatal("a bogus job must not be retried or parked")
	}
}

func TestHandleParksWhenAnalyzerCircuitOpens(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putTextObject(t, ctx, store, "circuit breaker park")
	analyzer := &countingAnalyzer{
		spec: contracts.AnalyzerSpec{Name: "fail.checked", Version: "v1"},
		err:  errors.New("model unavailable"),
	}
	registry, err := NewRegistry(analyzer)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(&blockingSource{}, store, registry, WithParkBackoff(0))
	if err != nil {
		t.Fatal(err)
	}
	job := analyzerJob(digest, analyzer.spec)

	// Failures up to the threshold follow the normal retry path.
	for i := range defaultBreakerThreshold - 1 {
		receipt := &memoryReceipt{job: job}
		if err := runtime.Handle(ctx, receipt); err != nil {
			t.Fatal(err)
		}
		if !receipt.retried || receipt.parked {
			t.Fatalf("attempt %d should retry while the circuit is closed", i)
		}
	}

	// The failure that trips the breaker parks instead of dead-lettering.
	tripping := &memoryReceipt{job: job}
	if err := runtime.Handle(ctx, tripping); err != nil {
		t.Fatal(err)
	}
	if !tripping.parked || tripping.retried {
		t.Fatal("the tripping failure should park the job, not retry it")
	}

	callsBefore := analyzer.calls
	// With the circuit open, further jobs park without invoking the analyzer.
	parked := &memoryReceipt{job: job}
	if err := runtime.Handle(ctx, parked); err != nil {
		t.Fatal(err)
	}
	if !parked.parked {
		t.Fatal("jobs should park while the circuit is open")
	}
	if analyzer.calls != callsBefore {
		t.Fatal("open circuit must not invoke the unavailable analyzer")
	}
}
