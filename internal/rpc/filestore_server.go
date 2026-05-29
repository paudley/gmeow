// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

const (
	// notifyBatchMax flushes the change batch once it holds this many digests; a
	// bulk import then notifies the scheduler once per ~256 objects instead of
	// once per object, turning tens of thousands of per-write RPCs into a few
	// hundred batched ones.
	notifyBatchMax = 256
	// notifyFlushInterval bounds how long a partial batch waits before flushing,
	// so low-rate writes still notify promptly.
	notifyFlushInterval = 250 * time.Millisecond
	// notifyBufferSize is the change-channel depth. Enqueue blocks (bounded
	// backpressure) only if the batcher falls this far behind — which, given the
	// 256x RPC reduction, effectively never happens unless the scheduler is wedged.
	notifyBufferSize = 8192
	// notifyFlushTimeout caps a single batch RPC so a stalled scheduler cannot
	// wedge the batcher (and thus block writes) indefinitely.
	notifyFlushTimeout = 30 * time.Second
)

type FilestoreServer struct {
	pb.UnimplementedFilestoreServiceServer

	notifier  ObjectChangeNotifier
	changes   chan changeNotice
	batcherWG sync.WaitGroup
	store     filestore.Store
}

// changeNotice is one object-change signal awaiting batched delivery to the
// scheduler. projectionOnly distinguishes a full analysis-eligible change from a
// projection-only refresh (annotation/overlay writes), which the scheduler must
// not treat as new analyzer work.
type changeNotice struct {
	digest         contracts.ObjectDigest
	projectionOnly bool
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
		store: store,
	}
	for _, option := range options {
		option(server)
	}

	// Only run the batcher when a notifier is wired (production filestore-serve);
	// tests and offline tools leave it nil and enqueueChange becomes a no-op.
	if server.notifier != nil {
		server.changes = make(chan changeNotice, notifyBufferSize)
		server.batcherWG.Add(1)
		go server.runNotifyBatcher()
	}

	return server
}

// Stop flushes any buffered change notifications and waits for the batcher to
// finish. It is safe to call after the gRPC server has gracefully stopped (no
// handler is still enqueuing), and a no-op when no notifier is configured.
func (server *FilestoreServer) Stop() {
	if server.changes == nil {
		return
	}

	close(server.changes)
	server.batcherWG.Wait()
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

	server.enqueueObjectChanged(put.digest)

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

	server.enqueueObjectChanged(contracts.ObjectDigest(request.GetDigest()))

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

	server.enqueueObjectChanged(digest)

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
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Error(codes.NotFound, err.Error())
		}

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

	server.enqueueProjectionRefresh(annotation.ObjectDigest)

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

	server.enqueueProjectionRefresh(digest)

	return &pb.Empty{}, nil
}

// enqueueObjectChanged signals that an object was created or non-trivially
// changed, so the scheduler should consider it for analysis. enqueueProjectionRefresh
// signals an annotation/overlay-only change (no new analyzer work). Both hand
// off to the batcher without blocking on the scheduler — the write path no
// longer waits on a per-object RPC. The scheduler de-duplicates already-analyzed
// objects ("only missing analyzer work"), so re-notifying a dedup hit is cheap
// and harmless.
func (server *FilestoreServer) enqueueObjectChanged(digest contracts.ObjectDigest) {
	server.enqueueChange(changeNotice{digest: digest})
}

func (server *FilestoreServer) enqueueProjectionRefresh(digest contracts.ObjectDigest) {
	server.enqueueChange(changeNotice{digest: digest, projectionOnly: true})
}

func (server *FilestoreServer) enqueueChange(notice changeNotice) {
	if server.changes == nil {
		return
	}

	// Bounded backpressure: a full buffer blocks the writer rather than dropping
	// the notification. Because the batcher collapses ~256 changes into one RPC,
	// it stays far ahead of ingestion unless the scheduler is wedged.
	server.changes <- notice
}

// runNotifyBatcher coalesces buffered change notices and delivers them to the
// scheduler in batches — one NotifyObjectsChanged per up-to-notifyBatchMax
// digests or per notifyFlushInterval — collapsing a bulk import's tens of
// thousands of per-object notifications into a few hundred RPCs. It exits after
// a final flush when the channel is closed by Stop.
func (server *FilestoreServer) runNotifyBatcher() {
	defer server.batcherWG.Done()

	ticker := time.NewTicker(notifyFlushInterval)
	defer ticker.Stop()

	var changed, projection []contracts.ObjectDigest
	flush := func() {
		if len(changed) > 0 {
			server.flushChangeBatch(changed, false)
			changed = changed[:0]
		}
		if len(projection) > 0 {
			server.flushChangeBatch(projection, true)
			projection = projection[:0]
		}
	}

	for {
		select {
		case notice, ok := <-server.changes:
			if !ok {
				flush()

				return
			}
			if notice.projectionOnly {
				projection = append(projection, notice.digest)
			} else {
				changed = append(changed, notice.digest)
			}
			if len(changed) >= notifyBatchMax || len(projection) >= notifyBatchMax {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (server *FilestoreServer) flushChangeBatch(
	digests []contracts.ObjectDigest,
	projectionOnly bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), notifyFlushTimeout)
	defer cancel()

	reason := "object_changed"
	if projectionOnly {
		reason = "projection_refresh"
	}

	_, err := server.notifier.NotifyObjectsChanged(ctx, contracts.ObjectChangeRequest{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		ObjectDigests:  digests,
		RequestedBy:    "filestore",
		Reason:         reason,
		ProjectionOnly: projectionOnly,
	})
	if err != nil {
		log.Printf(
			"filestore notify batch failed reason=%s count=%d error=%v",
			reason, len(digests), err,
		)
	}
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
	report, err := server.store.Verify(ctx, filestore.VerifyRequest{})
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

func (server *FilestoreServer) StorageBreakdown(
	ctx context.Context,
	request *pb.StorageBreakdownRequest,
) (*pb.StorageBreakdownResponse, error) {
	report, err := server.store.StorageBreakdown(ctx, filestore.StorageBreakdownRequest{
		Digest:         contracts.ObjectDigest(request.GetDigest()),
		RecursiveParts: request.GetRecursiveParts(),
	})
	if err != nil {
		return nil, err
	}

	return ToPBStorageBreakdown(report), nil
}

func (server *FilestoreServer) ResolvePath(
	ctx context.Context,
	request *pb.ResolvePathRequest,
) (*pb.ResolvePathResponse, error) {
	report, err := server.store.ResolvePath(ctx, filestore.PathResolveRequest{
		Path:         request.GetPath(),
		RecordsLimit: int(request.GetRecordsLimit()),
	})
	if err != nil {
		return nil, err
	}

	return ToPBResolvePath(report)
}

func (server *FilestoreServer) DeleteObject(
	ctx context.Context,
	request *pb.DeleteObjectRequest,
) (*pb.Empty, error) {
	if err := server.store.DeleteObject(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
	); err != nil {
		return nil, err
	}

	return &pb.Empty{}, nil
}

func (server *FilestoreServer) DeleteImport(
	ctx context.Context,
	request *pb.DeleteImportRequest,
) (*pb.DeleteImportResponse, error) {
	report, err := server.store.DeleteImport(
		ctx,
		request.GetSourceKind(),
		request.GetSourceName(),
	)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteImportResponse{
		ObjectsScanned:     int64(report.ObjectsScanned),
		ObjectsDeleted:     int64(report.ObjectsDeleted),
		ProvenanceDetached: int64(report.ProvenanceDetached),
	}, nil
}

func (server *FilestoreServer) Gc(
	ctx context.Context,
	_ *pb.GcRequest,
) (*pb.GcResponse, error) {
	report, err := server.store.Gc(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.GcResponse{
		ScannedChunks:  int32(report.ScannedChunks),
		SweptChunks:    int32(report.SweptChunks),
		RetainedChunks: int32(report.RetainedChunks),
		SweptRecipes:   int32(report.SweptRecipes),
	}, nil
}

func (server *FilestoreServer) Repack(
	ctx context.Context,
	_ *pb.RepackRequest,
) (*pb.RepackResponse, error) {
	report, err := server.store.Repack(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.RepackResponse{
		PacksScanned:  int32(report.PacksScanned),
		PacksRepacked: int32(report.PacksRepacked),
		PacksRemoved:  int32(report.PacksRemoved),
		ChunksMoved:   int32(report.ChunksMoved),
		BytesBefore:   report.BytesBefore,
		BytesAfter:    report.BytesAfter,
	}, nil
}

func (server *FilestoreServer) TrainDictionary(
	ctx context.Context,
	request *pb.TrainDictionaryRequest,
) (*pb.TrainDictionaryResponse, error) {
	report, err := server.store.TrainDictionary(ctx, int(request.GetSampleLimit()))
	if err != nil {
		return nil, err
	}

	return &pb.TrainDictionaryResponse{
		DictionaryId:    report.DictionaryID,
		DictionaryBytes: report.DictionaryBytes,
		Samples:         int32(report.Samples),
	}, nil
}

type putResult struct {
	err    error
	digest contracts.ObjectDigest
}

func (server *FilestoreServer) GetProjectionObject(
	ctx context.Context,
	request *pb.ProjectionObjectRequest,
) (*pb.ProjectionObjectResponse, error) {
	object, found, err := server.store.ProjectionObject(
		ctx,
		contracts.ObjectDigest(request.GetDigest()),
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return &pb.ProjectionObjectResponse{Found: false}, nil
	}

	converted, err := ToPBProjectionObject(object)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.ProjectionObjectResponse{Object: converted, Found: true}, nil
}

func (server *FilestoreServer) WalkProjection(
	_ *pb.WalkProjectionRequest,
	stream pb.FilestoreService_WalkProjectionServer,
) error {
	return server.store.WalkProjection(
		stream.Context(),
		func(object filestore.ProjectionObject) error {
			converted, err := ToPBProjectionObject(object)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}

			return stream.Send(converted)
		},
	)
}

func (server *FilestoreServer) WalkChangedProjection(
	request *pb.WalkChangedProjectionRequest,
	stream pb.FilestoreService_WalkChangedProjectionServer,
) error {
	since, err := parseTime(request.GetSince())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return server.store.WalkChangedProjection(
		stream.Context(),
		since,
		func(object filestore.ProjectionObject) error {
			converted, err := ToPBProjectionObject(object)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}

			return stream.Send(converted)
		},
	)
}

func (server *FilestoreServer) WalkSourceCursors(
	_ *pb.WalkSourceCursorsRequest,
	stream pb.FilestoreService_WalkSourceCursorsServer,
) error {
	return server.store.WalkSourceCursors(
		stream.Context(),
		func(cursor contracts.SourceCursor) error {
			converted, err := ToPBSourceCursor(cursor)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}

			return stream.Send(converted)
		},
	)
}
