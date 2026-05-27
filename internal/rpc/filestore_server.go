// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"errors"
	"io"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

type FilestoreServer struct {
	pb.UnimplementedFilestoreServiceServer

	notifier  ObjectChangeNotifier
	notifySem chan struct{}
	store     filestore.Store
}

type ObjectChangeNotifier interface {
	NotifyObjectsChanged(
		ctx context.Context,
		request contracts.ObjectChangeRequest,
	) (contracts.SchedulerScanResponse, error)
}

type FilestoreServerOption func(*FilestoreServer)

func WithObjectChangeNotifier(notifier ObjectChangeNotifier) FilestoreServerOption {
	return func(server *FilestoreServer) {
		server.notifier = notifier
	}
}

func NewFilestoreServer(
	store filestore.Store,
	options ...FilestoreServerOption,
) *FilestoreServer {
	server := &FilestoreServer{
		notifySem: make(chan struct{}, 8),
		store:     store,
	}
	for _, option := range options {
		option(server)
	}

	return server
}

func (server *FilestoreServer) LookupSourceObject(
	ctx context.Context,
	request *pb.LookupSourceObjectRequest,
) (*pb.LookupSourceObjectResponse, error) {
	digest, found, err := server.store.LookupSourceObject(
		ctx,
		FromPBSourceObjectRef(request.GetRef()),
	)
	if err != nil {
		return nil, err
	}

	return &pb.LookupSourceObjectResponse{Digest: string(digest), Found: found}, nil
}

func (server *FilestoreServer) TryAcquireSourceIngest(
	ctx context.Context,
	request *pb.TryAcquireSourceIngestRequest,
) (*pb.TryAcquireSourceIngestResponse, error) {
	claim, acquired, err := server.store.TryAcquireSourceIngest(
		ctx,
		FromPBSourceObjectRef(request.GetRef()),
	)
	if err != nil {
		return nil, err
	}

	return &pb.TryAcquireSourceIngestResponse{
		Claim:    ToPBSourceIngestClaim(claim),
		Acquired: acquired,
	}, nil
}

func (server *FilestoreServer) ReleaseSourceIngest(
	ctx context.Context,
	request *pb.ReleaseSourceIngestRequest,
) (*pb.Empty, error) {
	claim, err := FromPBSourceIngestClaim(request.GetClaim())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.Empty{}, server.store.ReleaseSourceIngest(ctx, claim)
}

func (server *FilestoreServer) PutObject(
	stream pb.FilestoreService_PutObjectServer,
) error {
	startFrame, err := stream.Recv()
	if err != nil {
		return err
	}

	start := startFrame.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first PutObject frame must be start")
	}

	facets, err := FromPBFacets(start.GetFacets())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	provenance, err := FromPBProvenance(start.GetProvenance())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	reader, writer := io.Pipe()
	result := make(chan putResult, 1)

	go func() {
		digest, putErr := server.store.Put(stream.Context(), filestore.PutRequest{
			Reader:        reader,
			MediaType:     start.GetMediaType(),
			SourceHint:    start.GetSourceHint(),
			ContentRoles:  append([]string{}, start.GetContentRoles()...),
			Facets:        facets,
			Provenance:    provenance,
			Relationships: FromPBRelationships(start.GetRelationships()),
		})
		result <- putResult{digest: digest, err: putErr}
	}()

	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}

		if recvErr != nil {
			_ = writer.CloseWithError(recvErr)

			return recvErr
		}

		if frame.GetFinish() != nil {
			break
		}

		data := frame.GetData()
		if data == nil {
			_ = writer.CloseWithError(errors.New("PutObject frame must be data or finish"))

			return status.Error(codes.InvalidArgument, "PutObject frame must be data or finish")
		}

		if _, err := writer.Write(data); err != nil {
			_ = writer.CloseWithError(err)

			return err
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}

	put := <-result
	if put.err != nil {
		return put.err
	}

	server.notifyObjectChangedAsync(stream.Context(), put.digest, "object_changed")

	return stream.SendAndClose(&pb.PutObjectResponse{Digest: string(put.digest)})
}

func (server *FilestoreServer) AttachProvenance(
	ctx context.Context,
	request *pb.AttachProvenanceRequest,
) (*pb.Empty, error) {
	provenance, err := FromPBProvenance(request.GetProvenance())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err = server.store.AttachProvenance(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
		provenance,
	)
	if err != nil {
		return nil, err
	}

	server.notifyObjectChangedAsync(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
		"object_changed",
	)

	return &pb.Empty{}, nil
}

func (server *FilestoreServer) PutCompound(
	ctx context.Context,
	request *pb.PutCompoundRequest,
) (*pb.PutCompoundResponse, error) {
	facets, err := FromPBFacets(request.GetFacets())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	provenance, err := FromPBProvenance(request.GetProvenance())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	parts, err := FromPBCompoundParts(request.GetParts())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	digest, err := server.store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:      request.GetObjectId(),
		MediaType:     request.GetMediaType(),
		SourceHint:    request.GetSourceHint(),
		ContentRoles:  append([]string{}, request.GetContentRoles()...),
		Facets:        facets,
		Provenance:    provenance,
		Relationships: FromPBRelationships(request.GetRelationships()),
		Parts:         parts,
	})
	if err != nil {
		return nil, err
	}

	server.notifyObjectChangedAsync(ctx, digest, "object_changed")

	return &pb.PutCompoundResponse{Digest: string(digest)}, nil
}

func (server *FilestoreServer) Open(
	request *pb.OpenRequest,
	stream pb.FilestoreService_OpenServer,
) error {
	reader, err := server.store.Open(
		stream.Context(),
		contracts.ObjectDigest(request.GetDigest()),
	)
	if err != nil {
		return err
	}
	defer reader.Close()

	buffer := make([]byte, 1024*1024)
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			err := stream.Send(
				&pb.ObjectChunk{Data: append([]byte{}, buffer[:n]...)},
			)
			if err != nil {
				return err
			}
		}

		if errors.Is(readErr, io.EOF) {
			return nil
		}

		if readErr != nil {
			return readErr
		}
	}
}

func (server *FilestoreServer) ReadManifest(
	ctx context.Context,
	request *pb.ReadManifestRequest,
) (*pb.ReadManifestResponse, error) {
	manifest, err := server.store.ReadManifest(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
	)
	if err != nil {
		return nil, err
	}

	converted, err := ToPBManifest(manifest)
	if err != nil {
		return nil, err
	}

	return &pb.ReadManifestResponse{Manifest: converted}, nil
}

func (server *FilestoreServer) GetStructure(
	ctx context.Context,
	request *pb.GetStructureRequest,
) (*pb.GetStructureResponse, error) {
	structure, err := server.store.GetStructure(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
	)
	if err != nil {
		return nil, err
	}

	converted, err := ToPBStructure(structure)
	if err != nil {
		return nil, err
	}

	return &pb.GetStructureResponse{Structure: converted}, nil
}

func (server *FilestoreServer) HasAnalysisAnnotation(
	ctx context.Context,
	request *pb.HasAnalysisAnnotationRequest,
) (*pb.HasAnalysisAnnotationResponse, error) {
	found, err := server.store.HasAnalysisAnnotation(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
		request.GetAnalyzerName(),
		request.GetAnalyzerVersion(),
	)
	if err != nil {
		return nil, err
	}

	return &pb.HasAnalysisAnnotationResponse{Found: found}, nil
}

func (server *FilestoreServer) WriteAnnotation(
	ctx context.Context,
	request *pb.WriteAnnotationRequest,
) (*pb.Empty, error) {
	annotation, err := FromPBAnnotation(request.GetAnnotation())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := server.store.WriteAnnotation(ctx, annotation); err != nil {
		return nil, err
	}

	server.notifyProjectionRefreshAsync(ctx, annotation.ObjectDigest)

	return &pb.Empty{}, nil
}

func (server *FilestoreServer) WriteOverlays(
	ctx context.Context,
	request *pb.WriteOverlaysRequest,
) (*pb.Empty, error) {
	overlays, err := decodeMap(request.GetOverlaysJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	digest := contracts.ObjectDigest(request.GetDigest())
	err = server.store.WriteOverlays(
		ctx,
		digest,
		overlays,
	)
	if err != nil {
		return nil, err
	}

	server.notifyProjectionRefreshAsync(ctx, digest)

	return &pb.Empty{}, nil
}

func (server *FilestoreServer) notifyObjectChanged(
	ctx context.Context,
	digest contracts.ObjectDigest,
	reason string,
) error {
	if server.notifier == nil {
		return nil
	}

	_, err := server.notifier.NotifyObjectsChanged(ctx, contracts.ObjectChangeRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigests: []contracts.ObjectDigest{digest},
		RequestedBy:   "filestore",
		Reason:        reason,
	})

	return err
}

func (server *FilestoreServer) notifyProjectionRefresh(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	if server.notifier == nil {
		return nil
	}

	_, err := server.notifier.NotifyObjectsChanged(ctx, contracts.ObjectChangeRequest{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		ObjectDigests:  []contracts.ObjectDigest{digest},
		RequestedBy:    "filestore",
		Reason:         "projection_refresh",
		ProjectionOnly: true,
	})

	return err
}

func (server *FilestoreServer) notifyObjectChangedAsync(
	ctx context.Context,
	digest contracts.ObjectDigest,
	reason string,
) {
	if server.notifier == nil {
		return
	}

	server.acquireNotifySlot()
	go func() {
		defer server.releaseNotifySlot()
		err := server.notifyObjectChanged(context.WithoutCancel(ctx), digest, reason)
		if err != nil {
			log.Printf(
				"filestore notification failed digest=%s reason=%s error=%v",
				digest,
				reason,
				err,
			)
		}
	}()
}

func (server *FilestoreServer) notifyProjectionRefreshAsync(
	ctx context.Context,
	digest contracts.ObjectDigest,
) {
	if server.notifier == nil {
		return
	}

	server.acquireNotifySlot()
	go func() {
		defer server.releaseNotifySlot()
		err := server.notifyProjectionRefresh(context.WithoutCancel(ctx), digest)
		if err != nil {
			log.Printf(
				"filestore projection notification failed digest=%s error=%v",
				digest,
				err,
			)
		}
	}()
}

func (server *FilestoreServer) acquireNotifySlot() {
	server.notifySem <- struct{}{}
}

func (server *FilestoreServer) releaseNotifySlot() {
	<-server.notifySem
}

func (server *FilestoreServer) WriteSourceCursor(
	ctx context.Context,
	request *pb.WriteSourceCursorRequest,
) (*pb.Empty, error) {
	cursor, err := FromPBSourceCursor(request.GetCursor())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.Empty{}, server.store.WriteSourceCursor(ctx, cursor)
}

func (server *FilestoreServer) ReadSourceCursor(
	ctx context.Context,
	request *pb.ReadSourceCursorRequest,
) (*pb.ReadSourceCursorResponse, error) {
	cursor, found, err := server.store.ReadSourceCursor(ctx, contracts.SourceCursorRef{
		SourceKind: request.GetSourceKind(),
		SourceName: request.GetSourceName(),
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return &pb.ReadSourceCursorResponse{}, nil
	}

	converted, err := ToPBSourceCursor(cursor)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.ReadSourceCursorResponse{
		Cursor: converted,
		Found:  true,
	}, nil
}

func (server *FilestoreServer) Verify(
	ctx context.Context,
	_ *pb.VerifyRequest,
) (*pb.VerifyResponse, error) {
	report, err := server.store.Verify(ctx)
	if err != nil {
		return nil, err
	}

	findings := make([]*pb.VerifyFinding, 0, len(report.Findings))
	for _, finding := range report.Findings {
		findings = append(findings, &pb.VerifyFinding{
			Digest:  string(finding.Digest),
			Path:    finding.Path,
			Code:    finding.Code,
			Message: finding.Message,
		})
	}

	return &pb.VerifyResponse{
		Status:   string(report.Status),
		Checked:  int32(report.Checked),
		Findings: findings,
	}, nil
}

type putResult struct {
	err    error
	digest contracts.ObjectDigest
}
