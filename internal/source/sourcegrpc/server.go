// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package sourcegrpc

import (
	"context"
	"errors"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/source"
)

type Server struct {
	pb.UnimplementedSourceServiceServer
	adapter source.Adapter
	service *source.Service
}

func NewServer(adapter source.Adapter, service *source.Service) (*Server, error) {
	if adapter == nil {
		return nil, errors.New("source server adapter is required")
	}
	if service == nil {
		return nil, errors.New("source server filestore service is required")
	}

	return &Server{adapter: adapter, service: service}, nil
}

func (server *Server) LiveSearch(
	ctx context.Context,
	request *pb.SourceSearchRequest,
) (*pb.SourceSearchResponse, error) {
	adapter, ok := server.adapter.(source.LiveSearchAdapter)
	if !ok {
		return nil, source.ErrUnsupportedOperation
	}
	hits, err := adapter.LiveSearch(ctx, source.LiveSearchRequest{
		Query: request.GetQuery(),
		Limit: int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}

	return &pb.SourceSearchResponse{Hits: sourceHitsToPB(hits)}, nil
}

func (server *Server) SearchAndHydrate(
	ctx context.Context,
	request *pb.SourceSearchRequest,
) (*pb.SourceSearchResponse, error) {
	adapter, ok := server.adapter.(source.HydratingLiveSearchAdapter)
	if !ok {
		return server.LiveSearch(ctx, request)
	}
	hits, err := adapter.SearchAndHydrate(ctx, server.service, source.LiveSearchRequest{
		Query: request.GetQuery(),
		Limit: int(request.GetLimit()),
	})
	if err != nil {
		return nil, err
	}

	return &pb.SourceSearchResponse{Hits: sourceHitsToPB(hits)}, nil
}

func (server *Server) ApplyAction(
	ctx context.Context,
	request *pb.SourceActionRequest,
) (*pb.SourceActionResponse, error) {
	adapter, ok := server.adapter.(source.ActionAdapter)
	if !ok {
		return nil, source.ErrUnsupportedOperation
	}
	parameters, err := rpc.DecodeMapForSource(request.GetParametersJson())
	if err != nil {
		return nil, err
	}
	result, err := server.service.ApplyAction(ctx, adapter, source.ActionRequest{
		ObjectDigest: contracts.ObjectDigest(request.GetObjectDigest()),
		Action:       request.GetAction(),
		Parameters:   parameters,
	})
	if err != nil {
		return nil, err
	}
	attributes, err := rpc.EncodeMapForSource(result.Attributes)
	if err != nil {
		return nil, err
	}

	return &pb.SourceActionResponse{
		Applied:        result.Applied,
		Action:         result.Action,
		AttributesJson: attributes,
	}, nil
}

func (server *Server) Backfill(
	ctx context.Context,
	request *pb.SourceBackfillRequest,
) (*pb.SourceBackfillResponse, error) {
	return server.runBackfill(ctx, source.BackfillRequest{
		Cursor: mapStringAny(map[string]string{
			"mode":  request.GetMode(),
			"query": request.GetQuery(),
		}, request.GetCursor()),
		CursorKey:   request.GetCursorNamespace(),
		PageSize:    int(request.GetPageSize()),
		MaxPages:    int(request.GetMaxPages()),
		Concurrency: int(request.GetConcurrency()),
		Resume:      request.GetResume(),
	})
}

func (server *Server) RefreshInbox(
	ctx context.Context,
	request *pb.SourceRefreshRequest,
) (*pb.SourceBackfillResponse, error) {
	return server.runBackfill(ctx, source.BackfillRequest{
		Cursor: map[string]any{
			"mode":  "full",
			"query": request.GetQuery(),
		},
		CursorKey:   "inbox_refresh",
		PageSize:    int(request.GetPageSize()),
		MaxPages:    int(request.GetMaxPages()),
		Concurrency: int(request.GetConcurrency()),
		Resume:      false,
	})
}

func (server *Server) Status(
	ctx context.Context,
	_ *pb.Empty,
) (*pb.SourceStatus, error) {
	cursor, found, err := server.service.ReadCursor(ctx, server.adapter)
	if err != nil {
		return nil, err
	}
	converted, err := rpc.ToPBSourceCursor(cursor)
	if err != nil {
		return nil, err
	}

	return &pb.SourceStatus{
		Kind:         server.adapter.Kind(),
		Name:         server.adapter.Name(),
		Capabilities: server.adapter.Capabilities(),
		Cursor:       converted,
		CursorFound:  found,
	}, nil
}

func (server *Server) runBackfill(
	ctx context.Context,
	request source.BackfillRequest,
) (*pb.SourceBackfillResponse, error) {
	adapter, ok := server.adapter.(source.PullAdapter)
	if !ok {
		return nil, source.ErrUnsupportedOperation
	}
	report, err := server.service.RunBackfill(ctx, adapter, request)
	response, convertErr := sourceBackfillReportToPB(report)
	if convertErr != nil {
		return nil, convertErr
	}

	return response, err
}

func sourceHitsToPB(hits []source.LiveSearchResult) []*pb.SourceSearchHit {
	out := make([]*pb.SourceSearchHit, 0, len(hits))
	for _, hit := range hits {
		out = append(out, &pb.SourceSearchHit{
			ObjectDigest:    string(hit.ObjectDigest),
			ExternalId:      hit.ExternalID,
			ExternalVersion: hit.ExternalVersion,
			Hydrated:        hit.Hydrated,
		})
	}

	return out
}

func sourceBackfillReportToPB(
	report source.BackfillReport,
) (*pb.SourceBackfillResponse, error) {
	cursor, err := rpc.ToPBSourceCursor(report.FinalCursor)
	if err != nil {
		return nil, err
	}

	return &pb.SourceBackfillResponse{
		FinalCursor:      cursor,
		FailedMessageIds: append([]string{}, report.FailedMessageIDs...),
		Processed:        contracts.ClampInt32(report.Processed),
		Created:          contracts.ClampInt32(report.Created),
		Skipped:          contracts.ClampInt32(report.Skipped),
		Failed:           contracts.ClampInt32(report.Failed),
		Pages:            contracts.ClampInt32(report.Pages),
		Completed:        report.Completed,
	}, nil
}

func mapStringAny(base, overlay map[string]string) map[string]any {
	out := map[string]any{}
	for key, value := range base {
		if value != "" {
			out[key] = value
		}
	}
	for key, value := range overlay {
		if value != "" {
			out[key] = value
		}
	}

	return out
}
