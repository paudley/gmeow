// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"blackcat.ca/gmeow/internal/cache"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

const (
	// clientContentCacheBytes bounds the client-side immutable object-content
	// cache (~64 MiB), and clientContentMaxObjectBytes caps which objects are
	// eligible so a large attachment is never fully buffered just to cache it.
	clientContentCacheBytes     = 64 << 20
	clientContentMaxObjectBytes = 1 << 20
)

type FilestoreClient struct {
	connection   *grpc.ClientConn
	client       pb.FilestoreServiceClient
	contentCache *cache.SizedLRU[string]
}

func NewFilestoreClient(
	ctx context.Context,
	endpoint Endpoint,
) (*FilestoreClient, error) {
	connection, err := dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return &FilestoreClient{
		connection:   connection,
		client:       pb.NewFilestoreServiceClient(connection),
		contentCache: cache.NewSizedLRU[string](clientContentCacheBytes),
	}, nil
}

func (client *FilestoreClient) Close() error {
	if client.connection == nil {
		return nil
	}

	return client.connection.Close()
}

func (client *FilestoreClient) ProjectionObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (filestore.ProjectionObject, bool, error) {
	response, err := client.client.GetProjectionObject(
		ctx,
		&pb.ProjectionObjectRequest{Digest: string(digest)},
	)
	if err != nil {
		return filestore.ProjectionObject{}, false, err
	}
	if !response.GetFound() {
		return filestore.ProjectionObject{}, false, nil
	}

	object, err := FromPBProjectionObject(response.GetObject())
	if err != nil {
		return filestore.ProjectionObject{}, false, err
	}

	return object, true, nil
}

// WalkProjection streams every object's projection from the FILESTORE service,
// letting QUERY rebuild without opening the FILESTORE root directly (the
// FILESTORE service is the sole owner of its metadata store).
func (client *FilestoreClient) WalkProjection(
	ctx context.Context,
	fn filestore.ProjectionFunc,
) error {
	stream, err := client.client.WalkProjection(ctx, &pb.WalkProjectionRequest{})
	if err != nil {
		return err
	}

	return receiveProjection(stream, fn)
}

func (client *FilestoreClient) WalkChangedProjection(
	ctx context.Context,
	since time.Time,
	fn filestore.ProjectionFunc,
) error {
	stream, err := client.client.WalkChangedProjection(
		ctx,
		&pb.WalkChangedProjectionRequest{Since: formatTime(since)},
	)
	if err != nil {
		return err
	}

	return receiveProjection(stream, fn)
}

func (client *FilestoreClient) WalkSourceCursors(
	ctx context.Context,
	fn filestore.SourceCursorProjectionFunc,
) error {
	stream, err := client.client.WalkSourceCursors(ctx, &pb.WalkSourceCursorsRequest{})
	if err != nil {
		return err
	}

	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		cursor, convErr := FromPBSourceCursor(message)
		if convErr != nil {
			return convErr
		}
		if err := fn(cursor); err != nil {
			return err
		}
	}
}

type projectionStream interface {
	Recv() (*pb.ProjectionObject, error)
}

func receiveProjection(stream projectionStream, fn filestore.ProjectionFunc) error {
	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		object, convErr := FromPBProjectionObject(message)
		if convErr != nil {
			return convErr
		}
		if err := fn(object); err != nil {
			return err
		}
	}
}

func (client *FilestoreClient) Open(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (io.ReadCloser, error) {
	// Object content is immutable by digest: a cache hit is always valid and
	// returns without a gRPC stream.
	if cached, ok := client.contentCache.Get(string(digest)); ok {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return io.NopCloser(bytes.NewReader(cached)), nil
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.client.Open(
		streamCtx,
		&pb.OpenRequest{Digest: string(digest)},
	)
	if err != nil {
		cancel()

		return nil, err
	}

	return &objectStreamReader{
		stream:    stream,
		cancel:    cancel,
		client:    client,
		digest:    string(digest),
		cacheable: true,
	}, nil
}

type objectStreamReader struct {
	stream    pb.FilestoreService_OpenClient
	cancel    context.CancelFunc
	client    *FilestoreClient
	digest    string
	accum     []byte
	cacheable bool
	closed    bool
	buffer    bytes.Buffer
}

func (reader *objectStreamReader) Read(target []byte) (int, error) {
	if reader.closed {
		return 0, io.ErrClosedPipe
	}
	if len(target) == 0 {
		return 0, nil
	}

	for reader.buffer.Len() == 0 {
		chunk, err := reader.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				reader.cacheOnComplete()
			}

			return 0, err
		}
		if len(chunk.GetData()) > 0 {
			reader.accumulate(chunk.GetData())
			_, _ = reader.buffer.Write(chunk.GetData())
		}
	}

	return reader.buffer.Read(target)
}

// accumulate buffers received bytes so a fully-read small object can be cached.
// Once the object exceeds the per-object cap, accumulation stops and the partial
// buffer is dropped so a large object is never held in memory just to cache it.
func (reader *objectStreamReader) accumulate(data []byte) {
	if !reader.cacheable {
		return
	}

	reader.accum = append(reader.accum, data...)
	if len(reader.accum) > clientContentMaxObjectBytes {
		reader.cacheable = false
		reader.accum = nil
	}
}

func (reader *objectStreamReader) cacheOnComplete() {
	if reader.cacheable && reader.client != nil && len(reader.accum) > 0 {
		reader.client.contentCache.Put(reader.digest, reader.accum)
	}
}

func (reader *objectStreamReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	reader.cancel()

	return nil
}

func (client *FilestoreClient) LookupSourceObject(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.ObjectDigest, bool, error) {
	response, err := client.client.LookupSourceObject(
		ctx,
		&pb.LookupSourceObjectRequest{Ref: ToPBSourceObjectRef(ref)},
	)
	if err != nil {
		return "", false, err
	}

	return contracts.ObjectDigest(response.GetDigest()), response.GetFound(), nil
}

func (client *FilestoreClient) TryAcquireSourceIngest(
	ctx context.Context,
	ref contracts.SourceObjectRef,
) (contracts.SourceIngestClaim, bool, error) {
	response, err := client.client.TryAcquireSourceIngest(
		ctx,
		&pb.TryAcquireSourceIngestRequest{Ref: ToPBSourceObjectRef(ref)},
	)
	if err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	claim, err := FromPBSourceIngestClaim(response.GetClaim())
	if err != nil {
		return contracts.SourceIngestClaim{}, false, err
	}

	return claim, response.GetAcquired(), nil
}

func (client *FilestoreClient) ReleaseSourceIngest(
	ctx context.Context,
	claim contracts.SourceIngestClaim,
) error {
	_, err := client.client.ReleaseSourceIngest(
		ctx,
		&pb.ReleaseSourceIngestRequest{Claim: ToPBSourceIngestClaim(claim)},
	)

	return err
}

func (client *FilestoreClient) Put(
	ctx context.Context,
	request PutRequest,
) (contracts.ObjectDigest, error) {
	stream, err := client.client.PutObject(ctx)
	if err != nil {
		return "", err
	}

	facets, err := ToPBFacets(request.Facets)
	if err != nil {
		return "", err
	}

	provenance, err := ToPBProvenance(request.Provenance)
	if err != nil {
		return "", err
	}

	if err := stream.Send(&pb.PutObjectFrame{Frame: &pb.PutObjectFrame_Start{
		Start: &pb.PutObjectStart{
			MediaType:     request.MediaType,
			SourceHint:    request.SourceHint,
			PriorityClass: request.PriorityClass,
			ContentRoles:  append([]string{}, request.ContentRoles...),
			Facets:        facets,
			Provenance:    provenance,
			Relationships: ToPBRelationships(request.Relationships),
		},
	}}); err != nil {
		return "", err
	}

	buffer := make([]byte, 1024*1024)
	for {
		n, readErr := request.Reader.Read(buffer)
		if n > 0 {
			if err := stream.Send(&pb.PutObjectFrame{
				Frame: &pb.PutObjectFrame_Data{Data: append([]byte{}, buffer[:n]...)},
			}); err != nil {
				return "", err
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}

		if readErr != nil {
			return "", readErr
		}
	}

	if err := stream.Send(&pb.PutObjectFrame{
		Frame: &pb.PutObjectFrame_Finish{Finish: &pb.PutObjectFinish{}},
	}); err != nil {
		return "", err
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}

	return contracts.ObjectDigest(response.GetDigest()), nil
}

func (client *FilestoreClient) AttachProvenance(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
) error {
	return client.AttachProvenanceWithPriority(ctx, digest, provenance, "")
}

func (client *FilestoreClient) AttachProvenanceWithPriority(
	ctx context.Context,
	digest contracts.ObjectDigest,
	provenance []contracts.Provenance,
	priorityClass string,
) error {
	converted, err := ToPBProvenance(provenance)
	if err != nil {
		return err
	}

	_, err = client.client.AttachProvenance(
		ctx,
		&pb.AttachProvenanceRequest{
			Digest:        string(digest),
			Provenance:    converted,
			PriorityClass: priorityClass,
		},
	)

	return err
}

func (client *FilestoreClient) PutCompound(
	ctx context.Context,
	request CompoundPutRequest,
) (contracts.ObjectDigest, error) {
	facets, err := ToPBFacets(request.Facets)
	if err != nil {
		return "", err
	}

	provenance, err := ToPBProvenance(request.Provenance)
	if err != nil {
		return "", err
	}

	parts, err := ToPBCompoundParts(request.Parts)
	if err != nil {
		return "", err
	}

	response, err := client.client.PutCompound(ctx, &pb.PutCompoundRequest{
		ObjectId:      request.ObjectID,
		MediaType:     request.MediaType,
		SourceHint:    request.SourceHint,
		PriorityClass: request.PriorityClass,
		ContentRoles:  append([]string{}, request.ContentRoles...),
		Facets:        facets,
		Provenance:    provenance,
		Relationships: ToPBRelationships(request.Relationships),
		Parts:         parts,
	})
	if err != nil {
		return "", err
	}

	return contracts.ObjectDigest(response.GetDigest()), nil
}

func (client *FilestoreClient) ReadManifest(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	response, err := client.client.ReadManifest(
		ctx,
		&pb.ReadManifestRequest{Digest: string(digest)},
	)
	if err != nil {
		// Translate a missing object into os.ErrNotExist so callers can detect
		// it uniformly regardless of whether the store is local or gRPC-backed.
		if status.Code(err) == codes.NotFound {
			return contracts.Manifest{}, fmt.Errorf("%w: %s", os.ErrNotExist, err.Error())
		}

		return contracts.Manifest{}, err
	}

	return FromPBManifest(response.GetManifest())
}

func (client *FilestoreClient) Verify(
	ctx context.Context,
	_ filestore.VerifyRequest,
) (filestore.VerifyReport, error) {
	// The gRPC Verify is read-only; --repair stays an offline command since it
	// mutates the store.
	response, err := client.client.Verify(ctx, &pb.VerifyRequest{})
	if err != nil {
		return filestore.VerifyReport{}, err
	}

	findings := make([]filestore.VerifyFinding, 0, len(response.GetFindings()))
	for _, finding := range response.GetFindings() {
		findings = append(findings, filestore.VerifyFinding{
			Digest:  contracts.ObjectDigest(finding.GetDigest()),
			Path:    finding.GetPath(),
			Code:    finding.GetCode(),
			Message: finding.GetMessage(),
		})
	}

	return filestore.VerifyReport{
		Status:   filestore.VerifyStatus(response.GetStatus()),
		Checked:  int(response.GetChecked()),
		Findings: findings,
	}, nil
}

func (client *FilestoreClient) StorageBreakdown(
	ctx context.Context,
	request filestore.StorageBreakdownRequest,
) (filestore.StorageBreakdownReport, error) {
	response, err := client.client.StorageBreakdown(ctx, &pb.StorageBreakdownRequest{
		Digest:         string(request.Digest),
		RecursiveParts: request.RecursiveParts,
	})
	if err != nil {
		return filestore.StorageBreakdownReport{}, err
	}

	return FromPBStorageBreakdown(response), nil
}

func (client *FilestoreClient) ResolvePath(
	ctx context.Context,
	request filestore.PathResolveRequest,
) (filestore.PathResolveReport, error) {
	response, err := client.client.ResolvePath(ctx, &pb.ResolvePathRequest{
		Path:         request.Path,
		RecordsLimit: int32(request.RecordsLimit),
	})
	if err != nil {
		return filestore.PathResolveReport{}, err
	}

	return FromPBResolvePath(response)
}

func (client *FilestoreClient) DeleteObject(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	_, err := client.client.DeleteObject(
		ctx,
		&pb.DeleteObjectRequest{Digest: string(digest)},
	)

	return err
}

func (client *FilestoreClient) DeleteImport(
	ctx context.Context,
	sourceKind, sourceName string,
) (filestore.DeleteImportReport, error) {
	response, err := client.client.DeleteImport(ctx, &pb.DeleteImportRequest{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return filestore.DeleteImportReport{}, err
	}

	return filestore.DeleteImportReport{
		ObjectsScanned:     int(response.GetObjectsScanned()),
		ObjectsDeleted:     int(response.GetObjectsDeleted()),
		ProvenanceDetached: int(response.GetProvenanceDetached()),
	}, nil
}

func (client *FilestoreClient) Gc(ctx context.Context) (filestore.GCReport, error) {
	response, err := client.client.Gc(ctx, &pb.GcRequest{})
	if err != nil {
		return filestore.GCReport{}, err
	}

	return filestore.GCReport{
		ScannedChunks:  int(response.GetScannedChunks()),
		SweptChunks:    int(response.GetSweptChunks()),
		RetainedChunks: int(response.GetRetainedChunks()),
		SweptRecipes:   int(response.GetSweptRecipes()),
	}, nil
}

func (client *FilestoreClient) Repack(
	ctx context.Context,
) (filestore.RepackReport, error) {
	response, err := client.client.Repack(ctx, &pb.RepackRequest{})
	if err != nil {
		return filestore.RepackReport{}, err
	}

	return filestore.RepackReport{
		PacksScanned:  int(response.GetPacksScanned()),
		PacksRepacked: int(response.GetPacksRepacked()),
		PacksRemoved:  int(response.GetPacksRemoved()),
		ChunksMoved:   int(response.GetChunksMoved()),
		BytesBefore:   response.GetBytesBefore(),
		BytesAfter:    response.GetBytesAfter(),
	}, nil
}

func (client *FilestoreClient) TrainDictionary(
	ctx context.Context,
	request filestore.TrainDictionaryRequest,
) (filestore.TrainDictionaryReport, error) {
	response, err := client.client.TrainDictionary(ctx, &pb.TrainDictionaryRequest{
		SampleLimit: int32(request.SampleLimit),
		Family:      request.Family,
		AllFamilies: request.AllFamilies,
	})
	if err != nil {
		return filestore.TrainDictionaryReport{}, err
	}
	results := make(
		[]filestore.TrainDictionaryFamilyReport,
		0,
		len(response.GetResults()),
	)
	for _, result := range response.GetResults() {
		results = append(results, filestore.TrainDictionaryFamilyReport{
			DictionaryID:    result.GetDictionaryId(),
			DictionaryBytes: result.GetDictionaryBytes(),
			Family:          result.GetFamily(),
			Samples:         int(result.GetSamples()),
		})
	}

	return filestore.TrainDictionaryReport{
		DictionaryID:    response.GetDictionaryId(),
		DictionaryBytes: response.GetDictionaryBytes(),
		Family:          response.GetFamily(),
		Samples:         int(response.GetSamples()),
		Results:         results,
	}, nil
}

func (client *FilestoreClient) GetStructure(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (contracts.Structure, error) {
	response, err := client.client.GetStructure(
		ctx,
		&pb.GetStructureRequest{Digest: string(digest)},
	)
	if err != nil {
		return contracts.Structure{}, err
	}

	return FromPBStructure(response.GetStructure())
}

func (client *FilestoreClient) HasAnalysisAnnotation(
	ctx context.Context,
	digest contracts.ObjectDigest,
	analyzerName string,
	analyzerVersion string,
) (bool, error) {
	response, err := client.client.HasAnalysisAnnotation(
		ctx,
		&pb.HasAnalysisAnnotationRequest{
			Digest:          string(digest),
			AnalyzerName:    analyzerName,
			AnalyzerVersion: analyzerVersion,
		},
	)
	if err != nil {
		return false, err
	}

	return response.GetFound(), nil
}

func (client *FilestoreClient) WriteSourceCursor(
	ctx context.Context,
	cursor contracts.SourceCursor,
) error {
	converted, err := ToPBSourceCursor(cursor)
	if err != nil {
		return err
	}

	_, err = client.client.WriteSourceCursor(
		ctx,
		&pb.WriteSourceCursorRequest{Cursor: converted},
	)

	return err
}

func (client *FilestoreClient) ReadSourceCursor(
	ctx context.Context,
	ref contracts.SourceCursorRef,
) (contracts.SourceCursor, bool, error) {
	response, err := client.client.ReadSourceCursor(
		ctx,
		&pb.ReadSourceCursorRequest{
			SourceKind: ref.SourceKind,
			SourceName: ref.SourceName,
		},
	)
	if err != nil {
		return contracts.SourceCursor{}, false, err
	}
	if !response.GetFound() {
		return contracts.SourceCursor{}, false, nil
	}

	cursor, err := FromPBSourceCursor(response.GetCursor())
	if err != nil {
		return contracts.SourceCursor{}, false, err
	}

	return cursor, true, nil
}

func (client *FilestoreClient) WriteAnnotation(
	ctx context.Context,
	annotation contracts.Annotation,
) error {
	converted, err := ToPBAnnotation(annotation)
	if err != nil {
		return err
	}

	_, err = client.client.WriteAnnotation(
		ctx,
		&pb.WriteAnnotationRequest{Annotation: converted},
	)

	return err
}

func (client *FilestoreClient) WriteOverlays(
	ctx context.Context,
	digest contracts.ObjectDigest,
	overlays map[string]any,
) error {
	encoded, err := encodeMap(overlays)
	if err != nil {
		return err
	}

	_, err = client.client.WriteOverlays(ctx, &pb.WriteOverlaysRequest{
		Digest:       string(digest),
		OverlaysJson: encoded,
	})

	return err
}

type PutRequest struct {
	Reader        io.Reader
	MediaType     string
	SourceHint    string
	PriorityClass string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
}

type CompoundPutRequest struct {
	ObjectID      string
	MediaType     string
	SourceHint    string
	PriorityClass string
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
	Parts         []contracts.CompoundPart
}

func dial(ctx context.Context, endpoint Endpoint) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(retryUnaryClientInterceptor),
	}

	target := endpoint.Address
	switch endpoint.Network {
	case "unix":
		target = "passthrough:///" + endpoint.Address

		options = append(
			options,
			grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", address)
			}),
		)
	case "tcp":
	default:
		return nil, fmt.Errorf("unsupported rpc network %q", endpoint.Network)
	}

	//nolint:staticcheck // SA1019: grpc.DialContext retained pending a tested migration to grpc.NewClient (lazy-dial semantics differ)
	connection, err := grpc.DialContext(ctx, target, options...)
	if err != nil {
		return nil, fmt.Errorf("dial grpc %s %s: %w", endpoint.Network, endpoint.Address, err)
	}

	return connection, nil
}

func DialForSource(ctx context.Context, endpoint Endpoint) (*grpc.ClientConn, error) {
	return dial(ctx, endpoint)
}
