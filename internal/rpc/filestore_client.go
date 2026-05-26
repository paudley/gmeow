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
	stream, err := client.client.Open(ctx, &pb.OpenRequest{Digest: string(digest)})
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer

	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return io.NopCloser(bytes.NewReader(buffer.Bytes())), nil
		}

		if recvErr != nil {
			return nil, recvErr
		}

		if _, err := buffer.Write(chunk.GetData()); err != nil {
			return nil, err
		}
	}
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

func dial(ctx context.Context, endpoint Endpoint) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

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
