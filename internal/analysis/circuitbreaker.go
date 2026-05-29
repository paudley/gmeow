// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"sync"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

// A per-analyzer circuit breaker guards analyzer retries. When an analyzer is
// unavailable (model endpoint down, external command missing) its jobs fail
// repeatedly; without a breaker each job would burn its retry budget and then
// dead-letter, and the worker would keep hammering the dead dependency.
//
// Instead, after a threshold of consecutive failures the breaker opens for an
// exponentially growing window. While open, the worker parks the analyzer's
// jobs (requeues them without consuming retries or dead-lettering) so they wait
// for the analyzer to recover. When the window elapses the breaker goes
// half-open and admits a single probe job: success closes it and resumes normal
// draining; failure re-opens it with a longer window. Genuine per-job failures
// (only one job failing while the analyzer is otherwise healthy) leave the
// circuit closed and follow the normal bounded-retry path.
const (
	defaultBreakerThreshold   = 5
	defaultBreakerBaseBackoff = 15 * time.Second
	defaultBreakerMaxBackoff  = 10 * time.Minute
)

type breakerState struct {
	openUntil    time.Time
	failures     int
	openSequence int
	halfOpen     bool
}

type circuitBreaker struct {
	now         func() time.Time
	states      map[string]*breakerState
	threshold   int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	mu          sync.Mutex
}

func newCircuitBreaker(now func() time.Time) *circuitBreaker {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	return &circuitBreaker{
		now:         now,
		states:      map[string]*breakerState{},
		threshold:   defaultBreakerThreshold,
		baseBackoff: defaultBreakerBaseBackoff,
		maxBackoff:  defaultBreakerMaxBackoff,
	}
}

func analyzerKey(spec contracts.AnalyzerSpec) string {
	return spec.Name + "@" + spec.Version
}

// allow reports whether a job for key may be processed now. A closed circuit
// always allows. An open circuit allows exactly one half-open probe once its
// backoff window elapses, and otherwise denies (the caller should park).
func (breaker *circuitBreaker) allow(key string) bool {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	state := breaker.states[key]
	if state == nil || state.failures < breaker.threshold {
		return true
	}
	if breaker.now().Before(state.openUntil) {
		return false
	}
	// Window elapsed: admit exactly one half-open probe. Once a probe is in
	// flight, deny the rest until its result is recorded, so a still-down
	// analyzer is not flooded.
	if state.halfOpen {
		return false
	}
	state.halfOpen = true

	return true
}

func (breaker *circuitBreaker) recordSuccess(key string) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	delete(breaker.states, key)
}

func (breaker *circuitBreaker) recordFailure(key string) {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	state := breaker.states[key]
	if state == nil {
		state = &breakerState{}
		breaker.states[key] = state
	}

	state.failures++
	if state.halfOpen || state.failures >= breaker.threshold {
		state.halfOpen = false
		state.openSequence++
		state.openUntil = breaker.now().Add(breaker.backoffFor(state.openSequence))
	}
}

// isOpen reports whether the circuit for key is currently open (tripped and
// inside its backoff window).
func (breaker *circuitBreaker) isOpen(key string) bool {
	breaker.mu.Lock()
	defer breaker.mu.Unlock()

	state := breaker.states[key]
	if state == nil || state.failures < breaker.threshold {
		return false
	}

	return breaker.now().Before(state.openUntil)
}

func (breaker *circuitBreaker) backoffFor(sequence int) time.Duration {
	backoff := breaker.baseBackoff
	for range sequence - 1 {
		backoff *= 2
		if backoff >= breaker.maxBackoff {
			return breaker.maxBackoff
		}
	}
	if backoff > breaker.maxBackoff {
		return breaker.maxBackoff
	}

	return backoff
}
