// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"fmt"

	"blackcat.ca/gmeow/internal/contracts"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

type SchedulerClient struct {
	connection grpcClientConn
	client     pb.SchedulerServiceClient
}

func NewSchedulerClient(
	ctx context.Context,
	endpoint Endpoint,
) (*SchedulerClient, error) {
	connection, err := dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return &SchedulerClient{
		connection: connection,
		client:     pb.NewSchedulerServiceClient(connection),
	}, nil
}

func (client *SchedulerClient) Close() error {
	if client.connection == nil {
		return nil
	}

	err := client.connection.Close()
	if err != nil {
		return fmt.Errorf("close scheduler client: %w", err)
	}

	return nil
}

func (client *SchedulerClient) Scan(
	ctx context.Context,
	request contracts.SchedulerScanRequest,
) (contracts.SchedulerScanResponse, error) {
	response, err := client.client.Scan(ctx, &pb.SchedulerScanRequest{
		SchemaVersion: contracts.ClampInt32(int(request.SchemaVersion)),
		PriorityClass: request.PriorityClass,
		RequestedBy:   request.RequestedBy,
		Reason:        request.Reason,
		TraceId:       request.TraceID,
		Forced:        request.Forced,
	})
	if err != nil {
		return contracts.SchedulerScanResponse{}, fmt.Errorf("scheduler scan: %w", err)
	}

	return fromPBSchedulerScanResponse(response), nil
}

func (client *SchedulerClient) Enqueue(
	ctx context.Context,
	job contracts.AnalyzerJob,
) error {
	_, err := client.client.Enqueue(ctx, ToPBAnalyzerJob(job))
	if err != nil {
		return fmt.Errorf("scheduler enqueue: %w", err)
	}

	return nil
}

func (client *SchedulerClient) Force(
	ctx context.Context,
	digest contracts.ObjectDigest,
	analyzerNames []string,
	requestedBy string,
	traceID string,
) (contracts.SchedulerScanResponse, error) {
	response, err := client.client.Force(ctx, &pb.ForceRequest{
		Digest:        string(digest),
		AnalyzerNames: append([]string{}, analyzerNames...),
		RequestedBy:   requestedBy,
		TraceId:       traceID,
	})
	if err != nil {
		return contracts.SchedulerScanResponse{}, fmt.Errorf("scheduler force: %w", err)
	}

	return fromPBSchedulerScanResponse(response), nil
}

func (client *SchedulerClient) NotifyObjectsChanged(
	ctx context.Context,
	request contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	digests := make([]string, 0, len(request.ObjectDigests))
	for _, digest := range request.ObjectDigests {
		digests = append(digests, string(digest))
	}
	reason := request.Reason
	if request.ProjectionOnly && reason == "" {
		reason = "projection_refresh"
	}

	response, err := client.client.NotifyObjectsChanged(
		ctx,
		&pb.NotifyObjectsChangedRequest{
			Digests:       digests,
			PriorityClass: request.PriorityClass,
			RequestedBy:   request.RequestedBy,
			Reason:        reason,
			TraceId:       request.TraceID,
		},
	)
	if err != nil {
		return contracts.SchedulerScanResponse{}, fmt.Errorf(
			"scheduler notify objects changed: %w",
			err,
		)
	}

	return fromPBSchedulerScanResponse(response), nil
}

func (client *SchedulerClient) Requeue(
	ctx context.Context,
	request contracts.RequeueRequest,
) (contracts.RequeueResponse, error) {
	response, err := client.client.Requeue(ctx, &pb.RequeueRequest{
		SchemaVersion: contracts.ClampInt32(int(request.SchemaVersion)),
		Limit:         contracts.ClampInt32(request.Limit),
	})
	if err != nil {
		return contracts.RequeueResponse{}, fmt.Errorf("scheduler requeue: %w", err)
	}

	return contracts.RequeueResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Requeued:      int(response.GetRequeued()),
	}, nil
}

func (client *SchedulerClient) DeadLetters(
	ctx context.Context,
	request contracts.DeadLetterRequest,
) (contracts.DeadLetterResponse, error) {
	response, err := client.client.DeadLetters(ctx, &pb.DeadLetterRequest{
		SchemaVersion: contracts.ClampInt32(int(request.SchemaVersion)),
		Limit:         contracts.ClampInt32(request.Limit),
	})
	if err != nil {
		return contracts.DeadLetterResponse{}, fmt.Errorf("scheduler dead letters: %w", err)
	}

	jobs := make([]contracts.AnalyzerJob, 0, len(response.GetJobs()))
	for _, job := range response.GetJobs() {
		converted, err := FromPBAnalyzerJob(job)
		if err != nil {
			return contracts.DeadLetterResponse{}, err
		}
		jobs = append(jobs, converted)
	}

	return contracts.DeadLetterResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Jobs:          jobs,
	}, nil
}

func (client *SchedulerClient) Status(
	ctx context.Context,
) (contracts.SchedulerStatus, error) {
	response, err := client.client.Status(ctx, &pb.Empty{})
	if err != nil {
		return contracts.SchedulerStatus{}, fmt.Errorf("scheduler status: %w", err)
	}

	return contracts.SchedulerStatus{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Pending:       int(response.GetPending()),
		Retry:         int(response.GetRetry()),
		Failed:        int(response.GetFailed()),
		DeadLetter:    int(response.GetDeadLetter()),
	}, nil
}

func (client *SchedulerClient) SelfHealSweep(
	ctx context.Context,
) (contracts.SchedulerScanResponse, error) {
	return client.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		RequestedBy:   "scheduler",
		Reason:        "self_heal_sweep",
	})
}

func fromPBSchedulerScanResponse(
	response *pb.SchedulerScanResponse,
) contracts.SchedulerScanResponse {
	return contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersion(response.GetSchemaVersion()),
		Scanned:       int(response.GetScanned()),
		Enqueued:      int(response.GetEnqueued()),
		Skipped:       int(response.GetSkipped()),
		Failed:        int(response.GetFailed()),
	}
}
