// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// backendModeFlag marks the re-executed test binary as a fake analyzer backend.
// It is placed after "--" so the test flag parser treats it as a positional and
// does not reject it.
const backendModeFlag = "--gmeow-backend-mode"

// TestHelperProcess is not a real test: when the binary is re-executed with a
// backend mode it impersonates an analyzer backend speaking the NDJSON protocol,
// then exits before the test framework can print to stdout. A normal test run has
// no mode and returns immediately.
func TestHelperProcess(t *testing.T) {
	mode := backendHelperMode()
	if mode == "" {
		return
	}

	runFakeBackend(mode)
}

func backendHelperMode() string {
	for index, arg := range os.Args {
		if arg == backendModeFlag && index+1 < len(os.Args) {
			return os.Args[index+1]
		}
	}

	return ""
}

// runFakeBackend implements the backend wire protocol for tests. The "serve" mode
// reports ready then, per request line, crashes / rejects / answers with its PID;
// the "no-ready" mode never reports ready so startup-timeout handling can be
// exercised.
func runFakeBackend(mode string) {
	switch mode {
	case "no-ready":
		select {} // never report ready; the manager kills us after startup timeout.
	case "crash-on-request":
		fmt.Fprintln(os.Stdout, `{"ready":true}`)

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		if scanner.Scan() {
			os.Exit(1) // ready, then die on the first real request (in-flight crash).
		}
	case "serve":
		fmt.Fprintln(os.Stdout, `{"ready":true}`)

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.Contains(line, `"crash"`):
				os.Exit(1)
			case strings.Contains(line, `"reject"`):
				fmt.Fprintln(os.Stdout, `{"error":"rejected object"}`)
			default:
				fmt.Fprintf(os.Stdout, `{"annotation":{"pid":%d}}`+"\n", os.Getpid())
			}
		}
	}

	os.Exit(0)
}

func helperSpec(key, mode string, maxInstances int) BackendSpec {
	return BackendSpec{
		Key:            key,
		Command:        os.Args[0],
		Args:           []string{"-test.run=TestHelperProcess", "--", backendModeFlag, mode},
		MaxInstances:   maxInstances,
		StartupTimeout: 5 * time.Second,
		RequestTimeout: 5 * time.Second,
	}
}

func backendPID(t *testing.T, raw json.RawMessage) int {
	t.Helper()

	var payload struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode backend pid from %q: %v", raw, err)
	}
	if payload.PID == 0 {
		t.Fatalf("backend returned zero pid: %q", raw)
	}

	return payload.PID
}

func TestBackendManagerReusesProcessAcrossRequests(t *testing.T) {
	manager := NewBackendManager(BackendManagerConfig{})
	t.Cleanup(manager.Close)

	spec := helperSpec("reuse", "serve", 1)

	var first int
	for attempt := range 3 {
		raw, err := manager.Run(t.Context(), spec, []byte(`{}`))
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", attempt, err)
		}

		pid := backendPID(t, raw)
		if attempt == 0 {
			first = pid

			continue
		}
		if pid != first {
			t.Fatalf("request %d ran in pid %d, want resident pid %d", attempt, pid, first)
		}
	}
}

func TestBackendManagerPerObjectErrorKeepsBackendAlive(t *testing.T) {
	manager := NewBackendManager(BackendManagerConfig{})
	t.Cleanup(manager.Close)

	spec := helperSpec("reject", "serve", 1)

	first, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err != nil {
		t.Fatalf("priming request: %v", err)
	}
	firstPID := backendPID(t, first)

	// A per-object rejection is an ordinary error (bounded retry), and it must NOT
	// be ErrAnalyzerUnavailable: the analyzer is healthy, only the object is bad.
	_, err = manager.Run(t.Context(), spec, []byte(`{"reject":true}`))
	if err == nil {
		t.Fatal("expected a per-object rejection error")
	}
	if errors.Is(err, ErrAnalyzerUnavailable) {
		t.Fatalf("per-object rejection must not park the analyzer: %v", err)
	}

	// The same resident process must still serve the next object.
	second, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err != nil {
		t.Fatalf("post-rejection request: %v", err)
	}
	if pid := backendPID(t, second); pid != firstPID {
		t.Fatalf("rejection discarded the backend: pid %d != %d", pid, firstPID)
	}
}

func TestBackendManagerStartupTimeoutIsUnavailable(t *testing.T) {
	manager := NewBackendManager(BackendManagerConfig{})
	t.Cleanup(manager.Close)

	spec := helperSpec("slow", "no-ready", 1)
	spec.StartupTimeout = 200 * time.Millisecond

	_, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err == nil {
		t.Fatal("expected startup-timeout error")
	}
	if !errors.Is(err, ErrAnalyzerUnavailable) {
		t.Fatalf("a backend that never starts must park (unavailable): %v", err)
	}
}

func TestBackendManagerCrashTriggersRestartNotUnavailable(t *testing.T) {
	manager := NewBackendManager(BackendManagerConfig{})
	t.Cleanup(manager.Close)

	spec := helperSpec("crash", "serve", 1)

	first, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err != nil {
		t.Fatalf("priming request: %v", err)
	}
	firstPID := backendPID(t, first)

	// A crash while the request is in flight is a job-specific failure (bounded
	// retry), not an unavailable analyzer: a poison object must never park the tag.
	_, err = manager.Run(t.Context(), spec, []byte(`{"crash":true}`))
	if err == nil {
		t.Fatal("expected an error when the backend crashes mid-request")
	}
	if errors.Is(err, ErrAnalyzerUnavailable) {
		t.Fatalf("a mid-request crash must not park the analyzer: %v", err)
	}

	// The crashed process must be replaced, and the tag must recover.
	second, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err != nil {
		t.Fatalf("post-crash request: %v", err)
	}
	if pid := backendPID(t, second); pid == firstPID {
		t.Fatalf("crashed backend was not replaced: still pid %d", pid)
	}
}

func TestBackendManagerReapsIdleBackends(t *testing.T) {
	manager := NewBackendManager(BackendManagerConfig{
		IdleTimeout:  50 * time.Millisecond,
		ReapInterval: 20 * time.Millisecond,
	})
	t.Cleanup(manager.Close)

	spec := helperSpec("reap", "serve", 1)

	first, err := manager.Run(t.Context(), spec, []byte(`{}`))
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	firstPID := backendPID(t, first)

	// Let the reaper reclaim the now-idle backend.
	deadline := time.Now().Add(2 * time.Second)
	for {
		second, runErr := serveAfter(manager, spec, 250*time.Millisecond)
		if runErr != nil {
			t.Fatalf("post-idle request: %v", runErr)
		}
		if backendPID(t, second) != firstPID {
			return // reaped: a fresh process served the later request.
		}
		if time.Now().After(deadline) {
			t.Fatal("idle backend was never reaped")
		}
	}
}

// serveAfter waits a fixed delay (longer than the idle timeout) and then issues a
// request, so the reaper has a chance to run between requests.
func serveAfter(
	manager *BackendManager,
	spec BackendSpec,
	delay time.Duration,
) (json.RawMessage, error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	return manager.Run(context.Background(), spec, []byte(`{}`))
}
