// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/contracts"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/scheduler"
)

type SchedulerServer struct {
	pb.UnimplementedSchedulerServiceServer

	service scheduler.Scheduler
}

func NewSchedulerServer(service scheduler.Scheduler) *SchedulerServer {
	return &SchedulerServer{service: service}
}

func (server *SchedulerServer) Scan(
	ctx context.Context,
	request *pb.SchedulerScanRequest,
) (*pb.SchedulerScanResponse, error) {
	response, err := server.service.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		PriorityClass: request.GetPriorityClass(),
		RequestedBy:   request.GetRequestedBy(),
		Reason:        request.GetReason(),
		Forced:        request.GetForced(),
		TraceID:       request.GetTraceId(),
	})
	if err != nil {
		return nil, err
	}

	return toPBSchedulerScanResponse(response), nil
}

func (server *SchedulerServer) Enqueue(
	ctx context.Context,
	request *pb.AnalyzerJob,
) (*pb.Empty, error) {
	job, err := FromPBAnalyzerJob(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.Empty{}, server.service.Enqueue(ctx, job)
}

func (server *SchedulerServer) Force(
	ctx context.Context,
	request *pb.ForceRequest,
) (*pb.SchedulerScanResponse, error) {
	response, err := server.service.Force(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
		request.GetAnalyzerNames(),
		request.GetRequestedBy(),
		request.GetTraceId(),
	)
	if err != nil {
		return nil, err
	}

	return toPBSchedulerScanResponse(response), nil
}

func (server *SchedulerServer) Requeue(
	ctx context.Context,
	request *pb.RequeueRequest,
) (*pb.RequeueResponse, error) {
	response, err := server.service.Requeue(ctx, contracts.RequeueRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}

	return &pb.RequeueResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Requeued:      int32(response.Requeued),
	}, nil
}

func (server *SchedulerServer) DeadLetters(
	ctx context.Context,
	request *pb.DeadLetterRequest,
) (*pb.DeadLetterResponse, error) {
	response, err := server.service.DeadLetters(ctx, contracts.DeadLetterRequest{
		SchemaVersion: contracts.SchemaVersion(request.GetSchemaVersion()),
		Limit:         int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}

	jobs := make([]*pb.AnalyzerJob, 0, len(response.Jobs))
	for _, job := range response.Jobs {
		jobs = append(jobs, ToPBAnalyzerJob(job))
	}

	return &pb.DeadLetterResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Jobs:          jobs,
	}, nil
}

func (server *SchedulerServer) Status(
	ctx context.Context,
	_ *pb.Empty,
) (*pb.SchedulerStatus, error) {
	status, err := server.service.Status(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.SchedulerStatus{
		SchemaVersion: int32(status.SchemaVersion),
		Pending:       int32(status.Pending),
		Retry:         int32(status.Retry),
		Failed:        int32(status.Failed),
		DeadLetter:    int32(status.DeadLetter),
	}, nil
}

func (server *SchedulerServer) NotifyObjectsChanged(
	ctx context.Context,
	request *pb.NotifyObjectsChangedRequest,
) (*pb.SchedulerScanResponse, error) {
	digests := make([]contracts.ObjectDigest, 0, len(request.GetDigests()))
	for _, digest := range request.GetDigests() {
		digests = append(digests, contracts.ObjectDigest(digest))
	}

	response, err := server.service.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion:  contracts.SchemaVersionPhase00,
			ObjectDigests:  digests,
			PriorityClass:  request.GetPriorityClass(),
			RequestedBy:    request.GetRequestedBy(),
			Reason:         request.GetReason(),
			TraceID:        request.GetTraceId(),
			ProjectionOnly: request.GetReason() == "projection_refresh",
		},
	)
	if err != nil {
		return nil, err
	}

	return toPBSchedulerScanResponse(response), nil
}

func toPBSchedulerScanResponse(
	response contracts.SchedulerScanResponse,
) *pb.SchedulerScanResponse {
	return &pb.SchedulerScanResponse{
		SchemaVersion: int32(response.SchemaVersion),
		Scanned:       int32(response.Scanned),
		Enqueued:      int32(response.Enqueued),
		Skipped:       int32(response.Skipped),
		Failed:        int32(response.Failed),
	}
}
