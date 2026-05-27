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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"blackcat.ca/gmeow/internal/contracts"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

type FilestoreClient struct {
	connection *grpc.ClientConn
	client     pb.FilestoreServiceClient
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
		connection: connection,
		client:     pb.NewFilestoreServiceClient(connection),
	}, nil
}

func (client *FilestoreClient) Close() error {
	if client.connection == nil {
		return nil
	}

	return client.connection.Close()
}

func (client *FilestoreClient) Open(
	ctx context.Context,
	digest contracts.ObjectDigest,
) (io.ReadCloser, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.client.Open(
		streamCtx,
		&pb.OpenRequest{Digest: string(digest)},
	)
	if err != nil {
		cancel()

		return nil, err
	}

	return &objectStreamReader{stream: stream, cancel: cancel}, nil
}

type objectStreamReader struct {
	stream pb.FilestoreService_OpenClient
	cancel context.CancelFunc
	buffer bytes.Buffer
	closed bool
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
			return 0, err
		}
		if len(chunk.GetData()) > 0 {
			_, _ = reader.buffer.Write(chunk.GetData())
		}
	}

	return reader.buffer.Read(target)
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
	converted, err := ToPBProvenance(provenance)
	if err != nil {
		return err
	}

	_, err = client.client.AttachProvenance(
		ctx,
		&pb.AttachProvenanceRequest{
			Digest:     string(digest),
			Provenance: converted,
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
		return contracts.Manifest{}, err
	}

	return FromPBManifest(response.GetManifest())
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
	ContentRoles  []string
	Facets        []contracts.Facet
	Provenance    []contracts.Provenance
	Relationships []contracts.Relationship
}

type CompoundPutRequest struct {
	ObjectID      string
	MediaType     string
	SourceHint    string
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

	connection, err := grpc.DialContext(ctx, target, options...)
	if err != nil {
		return nil, fmt.Errorf("dial grpc %s %s: %w", endpoint.Network, endpoint.Address, err)
	}

	return connection, nil
}

func DialForSource(ctx context.Context, endpoint Endpoint) (*grpc.ClientConn, error) {
	return dial(ctx, endpoint)
}
