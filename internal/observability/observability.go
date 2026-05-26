// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package observability

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
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

type Metrics struct {
	mu        sync.RWMutex
	counters  map[string]float64
	gauges    map[string]float64
	durations map[string][]time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		counters:  map[string]float64{},
		gauges:    map[string]float64{},
		durations: map[string][]time.Duration{},
	}
}

func DefaultMetrics() *Metrics {
	return defaultMetrics
}

func (metrics *Metrics) AddCounter(name string, value float64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.counters[metricName(name)] += value
}

func (metrics *Metrics) SetGauge(name string, value float64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	metrics.gauges[metricName(name)] = value
}

func (metrics *Metrics) ObserveDuration(name string, duration time.Duration) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	name = metricName(name)
	metrics.durations[name] = append(metrics.durations[name], duration)
}

func (metrics *Metrics) Snapshot() map[string]float64 {
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()

	snapshot := map[string]float64{}
	for name, value := range metrics.counters {
		snapshot[name+"_total"] = value
	}
	for name, value := range metrics.gauges {
		snapshot[name] = value
	}
	for name, values := range metrics.durations {
		var total time.Duration
		for _, value := range values {
			total += value
		}
		snapshot[name+"_seconds_sum"] = total.Seconds()
		snapshot[name+"_seconds_count"] = float64(len(values))
	}

	return snapshot
}

func (metrics *Metrics) PrometheusText() string {
	snapshot := metrics.Snapshot()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)

	var builder strings.Builder
	for _, name := range names {
		builder.WriteString(fmt.Sprintf("%s %g\n", name, snapshot[name]))
	}

	return builder.String()
}

func metricName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "gmeow_unknown"
	}

	var builder strings.Builder
	for _, char := range name {
		switch {
		case char >= 'a' && char <= 'z':
			builder.WriteRune(char)
		case char >= 'A' && char <= 'Z':
			builder.WriteRune(char + ('a' - 'A'))
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
		default:
			builder.WriteByte('_')
		}
	}

	return strings.Trim(builder.String(), "_")
}

var defaultMetrics = NewMetrics()
