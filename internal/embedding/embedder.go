// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Embedder turns texts into full-dimension vectors. Implementations are expected
// to honor batching: the whole slice is one logical request so callers can embed
// all of a record's new claims at once.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([]Vector, error)
}

const (
	// defaultEmbedTimeout is intentionally modest: a request that hangs longer
	// than this is treated as a stall and we BACK OFF rather than wait minutes,
	// which is what previously let work pile up on a struggling model server.
	defaultEmbedTimeout = 60 * time.Second
	// maxTextRunes caps a single claim's embedded text.
	maxTextRunes = 1600
	// defaultBatchTexts / defaultBatchTokenEst keep each request conservatively
	// small for an UNKNOWN endpoint (well under the 8192-token physical batch).
	// When gmeow controls a dedicated backend, raise these (and drop the pace) for
	// throughput — see EmbedTuning.
	defaultBatchTexts    = 12
	defaultBatchTokenEst = 1500
	// defaultMinInterval paces requests so we never hammer an unknown endpoint
	// back-to-back. With a controlled backend this can go to zero.
	defaultMinInterval = 200 * time.Millisecond
)

// EmbedTuning controls request batching and pacing. Conservative defaults suit
// an unknown/shared endpoint; with a gmeow-controlled dedicated backend, bigger
// batches and no pacing are much faster and safe.
type EmbedTuning struct {
	BatchTexts    int           // max texts per request (default 12)
	BatchTokenEst int           // max estimated tokens per request (default 1500)
	MinInterval   time.Duration // min wait between requests (default 200ms; 0 = none)
}

func (t EmbedTuning) withDefaults() EmbedTuning {
	if t.BatchTexts <= 0 {
		t.BatchTexts = defaultBatchTexts
	}
	if t.BatchTokenEst <= 0 {
		t.BatchTokenEst = defaultBatchTokenEst
	}
	if t.MinInterval < 0 {
		t.MinInterval = 0 // explicit "no pacing"
	}

	return t
}

// HTTPEmbedder calls the local embedding endpoint (nomic-embed-text,
// OpenAI-style {model,input}->{data:[{embedding}]}). It sends the whole batch as
// an array input and returns vectors in input order. This is the only component
// that touches the model endpoint; everything else works off the cache.
type HTTPEmbedder struct {
	lastCall      time.Time
	client        *http.Client
	endpoint      string
	model         string
	minInterval   time.Duration
	batchTexts    int
	batchTokenEst int
	paceMu        sync.Mutex
}

// NewHTTPEmbedder builds an embedder with conservative default tuning (small
// batches + 200ms pacing) suited to an unknown endpoint.
func NewHTTPEmbedder(
	endpoint, model string,
	client *http.Client,
) (*HTTPEmbedder, error) {
	return NewHTTPEmbedderTuned(
		endpoint,
		model,
		client,
		EmbedTuning{MinInterval: defaultMinInterval},
	)
}

// NewHTTPEmbedderTuned builds an embedder with explicit batching/pacing tuning.
func NewHTTPEmbedderTuned(
	endpoint, model string,
	client *http.Client,
	tuning EmbedTuning,
) (*HTTPEmbedder, error) {
	if endpoint == "" {
		return nil, errors.New("embedding endpoint is required")
	}

	if model == "" {
		return nil, errors.New("embedding model is required")
	}

	if client == nil {
		client = &http.Client{Timeout: defaultEmbedTimeout}
	}

	tuning = tuning.withDefaults()

	return &HTTPEmbedder{
		endpoint:      endpoint,
		model:         model,
		client:        client,
		minInterval:   tuning.MinInterval,
		batchTexts:    tuning.BatchTexts,
		batchTokenEst: tuning.BatchTokenEst,
	}, nil
}

// pace serializes requests and waits out the minimum inter-request interval,
// respecting context cancellation. This keeps the importer from overwhelming the
// local model server (which a previous unthrottled run did).
func (e *HTTPEmbedder) pace(ctx context.Context) error {
	e.paceMu.Lock()
	defer e.paceMu.Unlock()

	if e.minInterval > 0 && !e.lastCall.IsZero() {
		wait := e.minInterval - time.Since(e.lastCall)
		if wait > 0 {
			timer := time.NewTimer(wait)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}

	e.lastCall = time.Now()

	return nil
}

// Embed truncates each text and splits the inputs into sub-batches that stay
// under the endpoint's physical batch limit, then concatenates the results in
// input order. A single oversized batch otherwise 500s ("input too large").
func (e *HTTPEmbedder) Embed(ctx context.Context, texts []string) ([]Vector, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	out := make([]Vector, 0, len(texts))
	batch := make([]string, 0, e.batchTexts)
	tokenEst := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		vectors, err := e.embedBatch(ctx, batch)
		if err != nil {
			return err
		}

		out = append(out, vectors...)
		batch = batch[:0]
		tokenEst = 0

		return nil
	}

	for _, text := range texts {
		text = truncateToRunes(text, maxTextRunes)

		est := len([]rune(text))/3 + 1
		if len(batch) > 0 &&
			(len(batch) >= e.batchTexts || tokenEst+est > e.batchTokenEst) {
			err := flush()
			if err != nil {
				return nil, err
			}
		}

		batch = append(batch, text)
		tokenEst += est
	}

	err := flush()
	if err != nil {
		return nil, err
	}

	return out, nil
}

func truncateToRunes(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}

	return string(runes[:limit])
}

// embedRetryBackoffs is the wait between retries of a transient embedding-
// endpoint failure (network error, 5xx, or 429). Backoffs are GENEROUS: a
// stalled/restarting model server needs time to recover, and retrying too
// eagerly piles work onto it (which previously contributed to a crash).
var embedRetryBackoffs = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
}

func (e *HTTPEmbedder) embedBatch(
	ctx context.Context,
	texts []string,
) ([]Vector, error) {
	var lastErr error

	for attempt := 0; attempt <= len(embedRetryBackoffs); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(embedRetryBackoffs[attempt-1]):
			}
		}

		vectors, retryable, err := e.embedBatchOnce(ctx, texts)
		if err == nil {
			return vectors, nil
		}

		lastErr = err
		if !retryable || ctx.Err() != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf(
		"embedding endpoint failed after %d retries: %w",
		len(embedRetryBackoffs),
		lastErr,
	)
}

func (e *HTTPEmbedder) embedBatchOnce(
	ctx context.Context,
	texts []string,
) ([]Vector, bool, error) {
	if err := e.pace(ctx); err != nil {
		return nil, false, err
	}

	body, err := json.Marshal(map[string]any{"model": e.model, "input": texts})
	if err != nil {
		return nil, false, err
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		e.endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, false, err
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := e.client.Do(request)
	if err != nil {
		// Network/timeout errors are transient: the endpoint may be restarting or
		// briefly overloaded. Retry unless the caller's context was cancelled.
		return nil, ctx.Err() == nil, fmt.Errorf("call embedding endpoint: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		retryable := response.StatusCode >= 500 ||
			response.StatusCode == http.StatusTooManyRequests

		return nil, retryable, fmt.Errorf(
			"embedding endpoint returned %s: %s",
			response.Status,
			bytes.TrimSpace(detail),
		)
	}

	var decoded struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return nil, true, fmt.Errorf("decode embedding response: %w", err)
	}

	if len(decoded.Data) != len(texts) {
		return nil, false, fmt.Errorf(
			"embedding endpoint returned %d vectors for %d inputs",
			len(decoded.Data),
			len(texts),
		)
	}

	// The OpenAI contract returns data in input order, but index is authoritative;
	// place each vector by its index to be robust to reordering.
	out := make([]Vector, len(texts))

	for _, item := range decoded.Data {
		idx := item.Index
		if idx < 0 || idx >= len(texts) {
			return nil, false, fmt.Errorf("embedding response index %d out of range", idx)
		}

		if len(item.Embedding) == 0 {
			return nil, false, errors.New("embedding endpoint returned an empty vector")
		}

		out[idx] = VectorFromFloat64(item.Embedding)
	}

	for i, v := range out {
		if v == nil {
			return nil, false, fmt.Errorf("embedding response missing vector for input %d", i)
		}
	}

	return out, false, nil
}

// Resolver is the caching brain ingest calls: it returns claim vectors, hitting
// the embedder ONLY for statement hashes not already cached, batching all misses
// into a single Embedder call. This is what makes redundant ingests free — every
// claim is a cache hit, so Embed is never invoked.
type Resolver struct {
	cache    Cache
	embedder Embedder
	// lookups counts every claim-vector lookup; misses counts those that hit the
	// embedder. The hit rate (1 - misses/lookups) should converge toward 1.0 as
	// the corpus's distinct claims become cached.
	lookups atomic.Int64
	misses  atomic.Int64
}

func NewResolver(cache Cache, embedder Embedder) *Resolver {
	if cache == nil {
		cache = NewMemoryCache()
	}

	return &Resolver{cache: cache, embedder: embedder}
}

// Cache exposes the resolver's claim-vector cache (for state snapshot/restore).
func (r *Resolver) Cache() Cache { return r.cache }

// Stats returns cumulative cache lookups and embedder-hitting misses.
func (r *Resolver) Stats() (lookups, misses int64) {
	return r.lookups.Load(), r.misses.Load()
}

// Vectors returns the full-dimension vector for each text, in order. Cache
// misses are embedded together in one batch and written back. Returns the number
// of texts that required the embedder (zero == a fully-cached, zero-cost call).
func (r *Resolver) Vectors(ctx context.Context, texts []string) ([]Vector, int, error) {
	r.lookups.Add(int64(len(texts)))
	out := make([]Vector, len(texts))
	hashes := make([]string, len(texts))

	var (
		missTexts     []string
		missPositions []int
	)

	seenMiss := map[string]int{} // hash -> first miss slot, to dedup within a batch

	for i, text := range texts {
		hash := StatementHash(text)

		hashes[i] = hash
		if v, ok := r.cache.Get(hash); ok {
			out[i] = v

			continue
		}

		if _, dup := seenMiss[hash]; dup {
			continue // same new claim twice in one record; embed once
		}

		seenMiss[hash] = len(missTexts)
		missTexts = append(missTexts, text)
		missPositions = append(missPositions, i)
	}

	r.misses.Add(int64(len(missTexts)))

	if len(missTexts) > 0 {
		if r.embedder == nil {
			return nil, 0, errors.New("embedding cache miss but no embedder configured")
		}

		vectors, err := r.embedder.Embed(ctx, missTexts)
		if err != nil {
			return nil, 0, err
		}

		for j, v := range vectors {
			r.cache.Put(StatementHash(missTexts[j]), v)
			out[missPositions[j]] = v
		}
	}

	// Fill any positions that were intra-batch duplicates of a freshly-embedded claim.
	for i, v := range out {
		if v == nil {
			if cached, ok := r.cache.Get(hashes[i]); ok {
				out[i] = cached
			}
		}
	}

	return out, len(missTexts), nil
}

// Pool returns the renormalized weighted mean-pool (profile centroid) of the
// given claim texts, embedding only new claims. The second return is the number
// of embedder calls (new claims) — ingest uses zero-misses as the NOOP signal.
func (r *Resolver) Pool(
	ctx context.Context,
	texts []string,
	weights []float64,
) (Vector, int, error) {
	vectors, misses, err := r.Vectors(ctx, texts)
	if err != nil {
		return nil, 0, err
	}

	pooled, err := MeanPool(vectors, weights)
	if err != nil {
		return nil, misses, err
	}

	return pooled, misses, nil
}
