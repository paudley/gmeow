// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package observability

import (
	"context"
	"log/slog"
)

type traceIDKey struct{}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

func TraceID(ctx context.Context) string {
	value, _ := ctx.Value(traceIDKey{}).(string)

	return value
}

func Logger(ctx context.Context) *slog.Logger {
	traceID := TraceID(ctx)
	if traceID == "" {
		return slog.Default()
	}

	return slog.Default().With("trace_id", traceID)
}
