// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type OperationStore interface {
	CreateOrGet(
		ctx context.Context,
		request contracts.CreateOperationRequest,
	) (contracts.OperationRecord, bool, error)
	AppendProgress(
		ctx context.Context,
		operationID string,
		event contracts.OperationProgressEvent,
	) error
	Complete(ctx context.Context, operationID string, result json.RawMessage) error
	Fail(ctx context.Context, operationID, message string) error
	Get(ctx context.Context, operationID string) (contracts.OperationRecord, bool, error)
	GetByRequestHash(
		ctx context.Context,
		requestHash string,
	) (contracts.OperationRecord, bool, error)
}

type OperationProgressSink func(contracts.OperationProgressEvent)

type operationWaiter struct {
	done chan struct{}
}

type MemoryOperationStore struct {
	byRequestHash map[string]string
	records       map[string]contracts.OperationRecord
	mu            sync.Mutex
}

func NewMemoryOperationStore() *MemoryOperationStore {
	return &MemoryOperationStore{
		records:       map[string]contracts.OperationRecord{},
		byRequestHash: map[string]string{},
	}
}

func (store *MemoryOperationStore) CreateOrGet(
	_ context.Context,
	request contracts.CreateOperationRequest,
) (contracts.OperationRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if existingID := store.byRequestHash[request.RequestHash]; existingID != "" {
		return cloneOperationRecord(store.records[existingID]), false, nil
	}

	now := time.Now().UTC()
	record := contracts.OperationRecord{
		OperationID: request.OperationID,
		RequestHash: request.RequestHash,
		Name:        request.Name,
		Request:     append(json.RawMessage{}, request.Request...),
		Status:      contracts.OperationStatusRunning,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	store.records[record.OperationID] = record
	store.byRequestHash[record.RequestHash] = record.OperationID

	return cloneOperationRecord(record), true, nil
}

func (store *MemoryOperationStore) AppendProgress(
	_ context.Context,
	operationID string,
	event contracts.OperationProgressEvent,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	record, ok := store.records[operationID]
	if !ok {
		return fmt.Errorf("operation %s not found", operationID)
	}
	record.Progress = append(record.Progress, event)
	record.UpdatedAt = event.At
	store.records[operationID] = record

	return nil
}

func (store *MemoryOperationStore) Complete(
	_ context.Context,
	operationID string,
	result json.RawMessage,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	record, ok := store.records[operationID]
	if !ok {
		return fmt.Errorf("operation %s not found", operationID)
	}
	now := time.Now().UTC()
	record.Status = contracts.OperationStatusComplete
	record.Result = append(json.RawMessage{}, result...)
	record.UpdatedAt = now
	record.CompletedAt = now
	store.records[operationID] = record

	return nil
}

func (store *MemoryOperationStore) Fail(
	_ context.Context,
	operationID string,
	message string,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	record, ok := store.records[operationID]
	if !ok {
		return fmt.Errorf("operation %s not found", operationID)
	}
	now := time.Now().UTC()
	record.Status = contracts.OperationStatusFailed
	record.Error = message
	record.UpdatedAt = now
	record.CompletedAt = now
	store.records[operationID] = record

	return nil
}

func (store *MemoryOperationStore) Get(
	_ context.Context,
	operationID string,
) (contracts.OperationRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	record, ok := store.records[operationID]

	return cloneOperationRecord(record), ok, nil
}

func (store *MemoryOperationStore) GetByRequestHash(
	_ context.Context,
	requestHash string,
) (contracts.OperationRecord, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	operationID := store.byRequestHash[requestHash]
	if operationID == "" {
		return contracts.OperationRecord{}, false, nil
	}

	return cloneOperationRecord(store.records[operationID]), true, nil
}

func (services *Services) runOperation(
	ctx context.Context,
	name string,
	request any,
	progressObserver OperationProgressSink,
	work func(context.Context, OperationProgressSink) (any, error),
) (contracts.OperationResultResponse, error) {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return contracts.OperationResultResponse{}, err
	}
	record, created, err := services.operations.CreateOrGet(
		ctx,
		contracts.CreateOperationRequest{
			OperationID: randomOperationID(),
			RequestHash: operationRequestHash(name, requestJSON),
			Name:        name,
			Request:     requestJSON,
		},
	)
	if err != nil {
		return contracts.OperationResultResponse{}, err
	}

	waiter := completedOperationWaiter()
	if record.Status == contracts.OperationStatusRunning {
		var waiterCreated bool
		waiter, waiterCreated = services.operationWaiter(record.OperationID)
		if created || waiterCreated {
			go services.executeOperation(record.OperationID, progressObserver, work)
		}
	}

	return services.waitForOperation(ctx, record.OperationID, waiter, progressObserver)
}

func (services *Services) executeOperation(
	operationID string,
	progressObserver OperationProgressSink,
	work func(context.Context, OperationProgressSink) (any, error),
) {
	defer services.finishOperationWaiter(operationID)

	sink := func(event contracts.OperationProgressEvent) {
		if event.At.IsZero() {
			event.At = time.Now().UTC()
		}
		_ = services.operations.AppendProgress(context.Background(), operationID, event)
		if progressObserver != nil {
			progressObserver(event)
		}
	}
	sink(
		contracts.OperationProgressEvent{Stage: "accepted", Message: "operation accepted"},
	)

	result, err := work(context.Background(), sink)
	if err != nil {
		_ = services.operations.Fail(context.Background(), operationID, err.Error())

		return
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		_ = services.operations.Fail(context.Background(), operationID, err.Error())

		return
	}
	sink(
		contracts.OperationProgressEvent{
			Stage:   "result_persisting",
			Message: "persisting result",
		},
	)
	_ = services.operations.Complete(context.Background(), operationID, encoded)
}

func (services *Services) waitForOperation(
	ctx context.Context,
	operationID string,
	waiter *operationWaiter,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	seenProgress := 0
	for {
		record, found, err := services.operations.Get(ctx, operationID)
		if err != nil {
			return contracts.OperationResultResponse{}, err
		}
		if !found {
			return contracts.OperationResultResponse{}, fmt.Errorf(
				"operation %s not found",
				operationID,
			)
		}
		for _, event := range record.Progress[seenProgress:] {
			if progressObserver != nil {
				progressObserver(event)
			}
		}
		seenProgress = len(record.Progress)
		switch record.Status {
		case contracts.OperationStatusComplete:
			return operationResultResponse(record)
		case contracts.OperationStatusFailed:
			return contracts.OperationResultResponse{Operation: operationStatusResponse(record)},
				errors.New(record.Error)
		}

		select {
		case <-ctx.Done():
			return contracts.OperationResultResponse{
				Operation: operationStatusResponse(record),
			}, ctx.Err()
		case <-waiter.done:
		case <-ticker.C:
		}
	}
}

func (services *Services) operationWaiter(operationID string) (*operationWaiter, bool) {
	services.operationMu.Lock()
	defer services.operationMu.Unlock()

	waiter := services.operationWaiters[operationID]
	if waiter == nil {
		waiter = &operationWaiter{done: make(chan struct{})}
		services.operationWaiters[operationID] = waiter

		return waiter, true
	}

	return waiter, false
}

func (services *Services) finishOperationWaiter(operationID string) {
	services.operationMu.Lock()
	waiter := services.operationWaiters[operationID]
	delete(services.operationWaiters, operationID)
	services.operationMu.Unlock()
	if waiter != nil {
		close(waiter.done)
	}
}

func (services *Services) OperationStatus(
	ctx context.Context,
	request contracts.OperationStatusRequest,
) (contracts.OperationStatusResponse, error) {
	record, found, err := services.operations.Get(ctx, request.OperationID)
	if err != nil {
		return contracts.OperationStatusResponse{}, err
	}
	if !found {
		return contracts.OperationStatusResponse{}, fmt.Errorf(
			"operation %s not found",
			request.OperationID,
		)
	}

	return operationStatusResponse(record), nil
}

func (services *Services) OperationResult(
	ctx context.Context,
	request contracts.OperationResultRequest,
) (contracts.OperationResultResponse, error) {
	record, found, err := services.operations.Get(ctx, request.OperationID)
	if err != nil {
		return contracts.OperationResultResponse{}, err
	}
	if !found {
		return contracts.OperationResultResponse{}, fmt.Errorf(
			"operation %s not found",
			request.OperationID,
		)
	}
	if record.Status != contracts.OperationStatusComplete {
		return contracts.OperationResultResponse{Operation: operationStatusResponse(record)},
			fmt.Errorf("operation %s is %s", request.OperationID, record.Status)
	}

	return operationResultResponse(record)
}

func (services *Services) OperationResume(
	ctx context.Context,
	request contracts.OperationResultRequest,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	return services.waitForOperation(
		ctx,
		request.OperationID,
		services.mustOperationWaiter(request.OperationID),
		progressObserver,
	)
}

func (services *Services) mustOperationWaiter(operationID string) *operationWaiter {
	waiter, _ := services.operationWaiter(operationID)

	return waiter
}

func completedOperationWaiter() *operationWaiter {
	done := make(chan struct{})
	close(done)

	return &operationWaiter{done: done}
}

func (services *Services) MailSearchOperation(
	ctx context.Context,
	options SearchOptions,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	return services.runOperation(ctx, "mail_search", options, progressObserver,
		func(ctx context.Context, sink OperationProgressSink) (any, error) {
			sink(contracts.OperationProgressEvent{
				Stage:   "mail_search_started",
				Message: "searching indexed mail and live sources",
			})
			result, err := services.MailSearch(ctx, options)
			if err != nil {
				return nil, err
			}
			sink(contracts.OperationProgressEvent{
				Stage:    "mail_search_completed",
				Message:  "mail search completed",
				Progress: float64(len(result.Results)),
				Total:    float64(result.Total),
			})

			return result, nil
		},
	)
}

func (services *Services) ObjectRetrieveOperation(
	ctx context.Context,
	digest string,
	includeContent bool,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	request := retrieveOperationRequest{Digest: digest, IncludeContent: includeContent}

	return services.runOperation(ctx, "object_retrieve", request, progressObserver,
		func(ctx context.Context, sink OperationProgressSink) (any, error) {
			sink(contracts.OperationProgressEvent{
				Stage:   "object_retrieve_started",
				Message: "retrieving object",
			})
			result, err := services.Retrieve(ctx, contracts.ObjectDigest(digest), includeContent)
			if err != nil {
				return nil, err
			}
			sink(contracts.OperationProgressEvent{
				Stage:    "object_retrieve_completed",
				Message:  "object retrieve completed",
				Progress: 1,
				Total:    1,
			})

			return result, nil
		},
	)
}

func (services *Services) GraphExploreOperation(
	ctx context.Context,
	request contracts.GraphRequest,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	return services.runOperation(ctx, "graph_explore", request, progressObserver,
		func(ctx context.Context, sink OperationProgressSink) (any, error) {
			sink(contracts.OperationProgressEvent{
				Stage:   "graph_explore_started",
				Message: "exploring projected graph",
			})
			result, err := services.GraphExplore(ctx, request)
			if err != nil {
				return nil, err
			}
			sink(contracts.OperationProgressEvent{
				Stage:    "graph_explore_completed",
				Message:  "graph explore completed",
				Progress: float64(len(result.Facts)),
				Total:    float64(len(result.Facts)),
			})

			return result, nil
		},
	)
}

func (services *Services) AnalysisStatusOperation(
	ctx context.Context,
	request contracts.AnalysisStatusRequest,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	return services.runOperation(ctx, "analysis_status", request, progressObserver,
		func(ctx context.Context, sink OperationProgressSink) (any, error) {
			sink(contracts.OperationProgressEvent{
				Stage:   "analysis_status_started",
				Message: "checking analysis status",
			})
			result, err := services.AnalysisStatus(ctx, request)
			if err != nil {
				return nil, err
			}
			sink(contracts.OperationProgressEvent{
				Stage:    "analysis_status_completed",
				Message:  "analysis status completed",
				Progress: float64(len(result.Statuses)),
				Total:    float64(len(result.Statuses)),
			})

			return result, nil
		},
	)
}

func (services *Services) ForceAnalysisOperation(
	ctx context.Context,
	request ForceAnalysisRequest,
	progressObserver OperationProgressSink,
) (contracts.OperationResultResponse, error) {
	return services.runOperation(ctx, "force_analysis", request, progressObserver,
		func(ctx context.Context, sink OperationProgressSink) (any, error) {
			sink(contracts.OperationProgressEvent{
				Stage:   "force_analysis_started",
				Message: "scheduling forced analysis",
			})
			result, err := services.ForceAnalysis(ctx, request)
			if err != nil {
				return nil, err
			}
			sink(contracts.OperationProgressEvent{
				Stage:    "force_analysis_completed",
				Message:  "force analysis completed",
				Progress: 1,
				Total:    1,
			})

			return result, nil
		},
	)
}

type retrieveOperationRequest struct {
	Digest         string `json:"digest"`
	IncludeContent bool   `json:"include_content,omitempty"`
}

func operationResultResponse(
	record contracts.OperationRecord,
) (contracts.OperationResultResponse, error) {
	var result map[string]any
	if len(record.Result) > 0 {
		if err := json.Unmarshal(record.Result, &result); err != nil {
			return contracts.OperationResultResponse{}, err
		}
	}

	return contracts.OperationResultResponse{
		Operation: operationStatusResponse(record),
		Result:    result,
	}, nil
}

func operationStatusResponse(
	record contracts.OperationRecord,
) contracts.OperationStatusResponse {
	return contracts.OperationStatusResponse{
		OperationID: record.OperationID,
		RequestHash: record.RequestHash,
		Name:        record.Name,
		Status:      record.Status,
		Error:       record.Error,
		Progress:    append([]contracts.OperationProgressEvent{}, record.Progress...),
		CreatedAt:   record.CreatedAt,
		UpdatedAt:   record.UpdatedAt,
		CompletedAt: record.CompletedAt,
	}
}

func operationRequestHash(name string, requestJSON []byte) string {
	sum := sha256.Sum256(append([]byte(name+"\x00"), requestJSON...))

	return hex.EncodeToString(sum[:])
}

func randomOperationID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		now := time.Now().UTC().UnixNano()

		return fmt.Sprintf("op-%x", now)
	}

	return "op-" + hex.EncodeToString(raw[:])
}

func cloneOperationRecord(record contracts.OperationRecord) contracts.OperationRecord {
	record.Request = append(json.RawMessage{}, record.Request...)
	record.Result = append(json.RawMessage{}, record.Result...)
	record.Progress = append([]contracts.OperationProgressEvent{}, record.Progress...)

	return record
}
