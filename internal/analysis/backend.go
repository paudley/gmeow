// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

// External analyzers (e.g. the Python gmeow-intel package) load heavy models.
// Running them as a fork/exec per object reloads those models every time, which
// collapses throughput and, when load exceeds the per-job timeout, makes every
// job look unavailable and churn forever. The BackendManager instead keeps each
// analyzer's process resident: it loads its models once, signals readiness, then
// serves one object per request over framed stdin/stdout. Idle backends are
// reaped to reclaim memory.
//
// Wire protocol (newline-delimited JSON): on start the backend emits one
// {"ready":true} line after loading models; for each request line (an
// ExternalCommandRequest) it emits one response line — {"annotation":{…}} on
// success or {"error":"…"} on a per-object failure (the backend keeps serving).
// EOF on stdin ends the backend.
type backendMessage struct {
	Annotation json.RawMessage `json:"annotation,omitempty"`
	Error      string          `json:"error,omitempty"`
	Ready      bool            `json:"ready,omitempty"`
}

// BackendSpec describes how to launch and talk to one analyzer's backend.
type BackendSpec struct {
	Key            string
	Command        string
	Args           []string
	MaxInstances   int
	StartupTimeout time.Duration
	RequestTimeout time.Duration
}

// BackendManagerConfig configures lifecycle policy shared across analyzers.
type BackendManagerConfig struct {
	IdleTimeout  time.Duration
	ReapInterval time.Duration
}

// BackendManager owns the per-analyzer backend pools and the idle reaper.
type BackendManager struct {
	cfg   BackendManagerConfig
	quit  chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	pools map[string]*backendPool
}

// NewBackendManager starts the idle reaper and returns a manager. Call Close on
// shutdown to terminate all backends.
func NewBackendManager(cfg BackendManagerConfig) *BackendManager {
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = time.Hour
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = time.Minute
	}

	manager := &BackendManager{
		cfg:   cfg,
		quit:  make(chan struct{}),
		pools: map[string]*backendPool{},
	}
	manager.wg.Add(1)
	go manager.reapLoop()

	return manager
}

// Run dispatches one request to a persistent backend for spec.Key and returns the
// annotation JSON. A spawn/ready failure is wrapped as ErrAnalyzerUnavailable so
// the worker parks (the analyzer is genuinely down). A per-object error, or a
// crash while a request is in flight, is returned as an ordinary error so the
// worker follows the bounded-retry path (a poison object cannot wedge the tag).
func (manager *BackendManager) Run(
	ctx context.Context,
	spec BackendSpec,
	request []byte,
) (json.RawMessage, error) {
	pool := manager.poolFor(spec)

	backend, err := pool.acquire(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		return nil, fmt.Errorf(
			"%w: start analyzer %s: %w", ErrAnalyzerUnavailable, spec.Key, err,
		)
	}

	annotation, alive, err := backend.roundTrip(ctx, request, spec.RequestTimeout)
	pool.release(backend, alive)

	return annotation, err
}

// Close stops the reaper and terminates every backend.
func (manager *BackendManager) Close() {
	close(manager.quit)
	manager.wg.Wait()

	manager.mu.Lock()
	pools := make([]*backendPool, 0, len(manager.pools))
	for _, pool := range manager.pools {
		pools = append(pools, pool)
	}
	manager.mu.Unlock()

	for _, pool := range pools {
		pool.closeAll()
	}
}

func (manager *BackendManager) poolFor(spec BackendSpec) *backendPool {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	pool, ok := manager.pools[spec.Key]
	if !ok {
		pool = newBackendPool(spec, manager.cfg.IdleTimeout)
		manager.pools[spec.Key] = pool
	}

	return pool
}

func (manager *BackendManager) reapLoop() {
	defer manager.wg.Done()

	ticker := time.NewTicker(manager.cfg.ReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-manager.quit:
			return
		case <-ticker.C:
			manager.reapIdle()
		}
	}
}

func (manager *BackendManager) reapIdle() {
	manager.mu.Lock()
	pools := make([]*backendPool, 0, len(manager.pools))
	for _, pool := range manager.pools {
		pools = append(pools, pool)
	}
	manager.mu.Unlock()

	now := time.Now()
	for _, pool := range pools {
		pool.reap(now)
	}
}

// backendPool bounds concurrent backends per analyzer at MaxInstances using two
// channels: slots holds capacity tokens (one per not-yet-spawned backend) and
// idle holds ready, not-in-use backends. A backend lives in exactly one place at
// a time (idle, or out being used), so capacity = MaxInstances - len(slots).
type backendPool struct {
	spec        BackendSpec
	idleTimeout time.Duration
	idle        chan *backend
	slots       chan struct{}
	mu          sync.Mutex
	all         map[*backend]struct{}
}

func newBackendPool(spec BackendSpec, idleTimeout time.Duration) *backendPool {
	maxInstances := max(spec.MaxInstances, 1)

	pool := &backendPool{
		spec:        spec,
		idleTimeout: idleTimeout,
		idle:        make(chan *backend, maxInstances),
		slots:       make(chan struct{}, maxInstances),
		all:         map[*backend]struct{}{},
	}
	for range maxInstances {
		pool.slots <- struct{}{}
	}

	return pool
}

func (pool *backendPool) acquire(ctx context.Context) (*backend, error) {
	// Prefer a ready backend, then a free capacity slot to spawn a new one.
	select {
	case backend := <-pool.idle:
		return backend, nil
	default:
	}
	select {
	case <-pool.slots:
		return pool.spawnOrReturnSlot(ctx)
	default:
	}

	// At capacity with none idle: wait for a backend to free or a slot to open.
	select {
	case backend := <-pool.idle:
		return backend, nil
	case <-pool.slots:
		return pool.spawnOrReturnSlot(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (pool *backendPool) spawnOrReturnSlot(ctx context.Context) (*backend, error) {
	backend, err := pool.spawn(ctx)
	if err != nil {
		pool.slots <- struct{}{}

		return nil, err
	}

	return backend, nil
}

func (pool *backendPool) release(backend *backend, alive bool) {
	if !alive {
		pool.discard(backend)

		return
	}

	backend.lastUsed = time.Now()
	select {
	case pool.idle <- backend:
	default:
		// idle is sized to capacity, so this should not happen; close defensively.
		pool.discard(backend)
	}
}

// discard terminates a backend and returns its capacity slot for a future spawn.
func (pool *backendPool) discard(backend *backend) {
	backend.close()

	pool.mu.Lock()
	delete(pool.all, backend)
	pool.mu.Unlock()

	pool.slots <- struct{}{}
}

func (pool *backendPool) reap(now time.Time) {
	var keep []*backend
	for {
		select {
		case backend := <-pool.idle:
			if now.Sub(backend.lastUsed) >= pool.idleTimeout {
				pool.discard(backend)
			} else {
				keep = append(keep, backend)
			}

			continue
		default:
		}

		break
	}

	for _, backend := range keep {
		select {
		case pool.idle <- backend:
		default:
			pool.discard(backend)
		}
	}
}

func (pool *backendPool) closeAll() {
	pool.mu.Lock()
	all := make([]*backend, 0, len(pool.all))
	for backend := range pool.all {
		all = append(all, backend)
	}
	pool.mu.Unlock()

	for _, backend := range all {
		backend.close()
	}
}

func (pool *backendPool) spawn(_ context.Context) (*backend, error) {
	// Not tied to a request context: the process is persistent and outlives the
	// request that triggered its creation; its lifecycle is the pool's.
	cmd := exec.Command(pool.spec.Command, pool.spec.Args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	// Surface the backend's own diagnostics (model-load errors, tracebacks) in the
	// worker log rather than swallowing them.
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	backend := &backend{cmd: cmd, stdin: stdin, stdout: bufio.NewReader(stdout)}
	pool.mu.Lock()
	pool.all[backend] = struct{}{}
	pool.mu.Unlock()

	message, err := backend.readLine(context.Background(), pool.spec.StartupTimeout)
	if err != nil || !message.Ready {
		backend.close()
		pool.mu.Lock()
		delete(pool.all, backend)
		pool.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("await analyzer ready: %w", err)
		}

		return nil, errors.New("analyzer did not report ready")
	}

	backend.lastUsed = time.Now()

	return backend, nil
}

type backend struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   *bufio.Reader
	lastUsed time.Time
}

// roundTrip sends one request and reads one response. alive reports whether the
// backend may be reused: a per-object analyzer error keeps it alive (bounded
// retry of the job), while a write/read failure means the process is gone.
func (backend *backend) roundTrip(
	ctx context.Context,
	request []byte,
	timeout time.Duration,
) (json.RawMessage, bool, error) {
	if _, err := backend.stdin.Write(request); err != nil {
		return nil, false, fmt.Errorf("write analyzer request: %w", err)
	}
	if _, err := backend.stdin.Write([]byte("\n")); err != nil {
		return nil, false, fmt.Errorf("write analyzer request: %w", err)
	}

	message, err := backend.readLine(ctx, timeout)
	if err != nil {
		// Crash/timeout while the request was in flight: treat as a job-specific
		// failure (bounded retry), and the process is no longer usable.
		return nil, false, fmt.Errorf("read analyzer response: %w", err)
	}
	if message.Error != "" {
		// The analyzer ran and rejected this object: healthy backend, bad job.
		return nil, true, errors.New(message.Error)
	}
	if len(message.Annotation) == 0 {
		return nil, true, errors.New("analyzer returned an empty response")
	}

	backend.lastUsed = time.Now()

	return message.Annotation, true, nil
}

func (backend *backend) readLine(
	ctx context.Context,
	timeout time.Duration,
) (backendMessage, error) {
	type result struct {
		message backendMessage
		err     error
	}
	channel := make(chan result, 1)
	go func() {
		line, err := backend.stdout.ReadBytes('\n')
		if err != nil {
			channel <- result{err: err}

			return
		}
		var message backendMessage
		if err := json.Unmarshal(line, &message); err != nil {
			channel <- result{err: fmt.Errorf("decode analyzer message: %w", err)}

			return
		}
		channel <- result{message: message}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case res := <-channel:
		return res.message, res.err
	case <-timer.C:
		return backendMessage{}, errors.New("analyzer response timed out")
	case <-ctx.Done():
		return backendMessage{}, ctx.Err()
	}
}

func (backend *backend) close() {
	// Closing stdin signals EOF so the backend exits its loop cleanly; fall back
	// to a kill if it does not exit promptly (also unblocks any pending readLine).
	_ = backend.stdin.Close()

	done := make(chan struct{})
	go func() {
		_ = backend.cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		if backend.cmd.Process != nil {
			_ = backend.cmd.Process.Kill()
		}
		<-done
	}
}
