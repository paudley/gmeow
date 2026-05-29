// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"time"
)

const modelRequestTimeout = 60 * time.Second

type modelGate struct {
	slots chan struct{}
}

func newModelGate() modelGate {
	return modelGate{slots: make(chan struct{}, 1)}
}

func (gate modelGate) withLock(
	ctx context.Context,
	timeout time.Duration,
	call func(context.Context) error,
) error {
	if timeout <= 0 {
		timeout = modelRequestTimeout
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case gate.slots <- struct{}{}:
		defer func() { <-gate.slots }()
	case <-callCtx.Done():
		return modelEndpointError{Err: errors.New("model endpoint lock timed out")}
	}

	if err := call(callCtx); err != nil {
		return modelEndpointError{Err: err}
	}

	return nil
}

// ErrAnalyzerUnavailable marks a failure caused by the analyzer's backing
// service being unavailable (endpoint unreachable, timed out, or saturated)
// rather than by the job itself. The worker parks such jobs to wait for the
// dependency to recover instead of retrying them toward a dead-letter. Parking
// on this signal — not on accumulated circuit-breaker state — keeps the
// behavior correct across worker restarts, which reset the in-memory breaker.
var ErrAnalyzerUnavailable = errors.New("analyzer temporarily unavailable")

type modelEndpointError struct {
	Err error
}

func (err modelEndpointError) Error() string {
	return err.Err.Error()
}

func (err modelEndpointError) Unwrap() error {
	return err.Err
}

// Is reports modelEndpointError as an availability failure so callers can
// uniformly detect "analyzer down" via errors.Is(err, ErrAnalyzerUnavailable).
func (err modelEndpointError) Is(target error) bool {
	return target == ErrAnalyzerUnavailable
}
