// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"encoding/json"

	"blackcat.ca/gmeow/internal/contracts"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

func ToPBOperationRecord(record contracts.OperationRecord) *pb.OperationRecord {
	return &pb.OperationRecord{
		OperationId: record.OperationID,
		RequestHash: record.RequestHash,
		Name:        record.Name,
		RequestJson: append([]byte{}, record.Request...),
		Status:      string(record.Status),
		Progress:    ToPBOperationProgress(record.Progress),
		ResultJson:  append([]byte{}, record.Result...),
		Error:       record.Error,
		CreatedAt:   formatTime(record.CreatedAt),
		UpdatedAt:   formatTime(record.UpdatedAt),
		CompletedAt: formatTime(record.CompletedAt),
	}
}

func FromPBOperationRecord(
	record *pb.OperationRecord,
) (contracts.OperationRecord, error) {
	if record == nil {
		return contracts.OperationRecord{}, nil
	}

	progress, err := FromPBOperationProgress(record.GetProgress())
	if err != nil {
		return contracts.OperationRecord{}, err
	}
	createdAt, err := parseTime(record.GetCreatedAt())
	if err != nil {
		return contracts.OperationRecord{}, err
	}
	updatedAt, err := parseTime(record.GetUpdatedAt())
	if err != nil {
		return contracts.OperationRecord{}, err
	}
	completedAt, err := parseTime(record.GetCompletedAt())
	if err != nil {
		return contracts.OperationRecord{}, err
	}

	return contracts.OperationRecord{
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		CompletedAt: completedAt,
		Progress:    progress,
		Request:     append(json.RawMessage{}, record.GetRequestJson()...),
		Result:      append(json.RawMessage{}, record.GetResultJson()...),
		OperationID: record.GetOperationId(),
		RequestHash: record.GetRequestHash(),
		Name:        record.GetName(),
		Status:      contracts.OperationStatus(record.GetStatus()),
		Error:       record.GetError(),
	}, nil
}

func ToPBOperationProgress(
	progress []contracts.OperationProgressEvent,
) []*pb.OperationProgressEvent {
	out := make([]*pb.OperationProgressEvent, 0, len(progress))
	for _, event := range progress {
		out = append(out, &pb.OperationProgressEvent{
			At:       formatTime(event.At),
			Stage:    event.Stage,
			Message:  event.Message,
			Progress: event.Progress,
			Total:    event.Total,
		})
	}

	return out
}

func FromPBOperationProgress(
	progress []*pb.OperationProgressEvent,
) ([]contracts.OperationProgressEvent, error) {
	out := make([]contracts.OperationProgressEvent, 0, len(progress))
	for _, event := range progress {
		at, err := parseTime(event.GetAt())
		if err != nil {
			return nil, err
		}
		out = append(out, contracts.OperationProgressEvent{
			At:       at,
			Stage:    event.GetStage(),
			Message:  event.GetMessage(),
			Progress: event.GetProgress(),
			Total:    event.GetTotal(),
		})
	}

	return out, nil
}
