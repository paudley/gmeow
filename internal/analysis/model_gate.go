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

type modelEndpointError struct {
	Err error
}

func (err modelEndpointError) Error() string {
	return err.Err.Error()
}

func (err modelEndpointError) Unwrap() error {
	return err.Err
}
