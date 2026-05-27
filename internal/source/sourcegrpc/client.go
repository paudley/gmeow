// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package sourcegrpc

import (
	"context"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
	"blackcat.ca/gmeow/internal/source"
)

type Client struct {
	connection   interface{ Close() error }
	client       pb.SourceServiceClient
	name         string
	kind         string
	capabilities []string
}

func NewClient(
	ctx context.Context,
	endpoint rpc.Endpoint,
	kind string,
	name string,
	capabilities []string,
) (*Client, error) {
	connection, err := rpc.DialForSource(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	return &Client{
		connection:   connection,
		client:       pb.NewSourceServiceClient(connection),
		kind:         kind,
		name:         name,
		capabilities: append([]string{}, capabilities...),
	}, nil
}

func (client *Client) Close() error {
	if client.connection == nil {
		return nil
	}

	return client.connection.Close()
}

func (client *Client) Name() string {
	return client.name
}

func (client *Client) Kind() string {
	return client.kind
}

func (client *Client) Capabilities() []string {
	return append([]string{}, client.capabilities...)
}

func (client *Client) LiveSearch(
	ctx context.Context,
	request source.LiveSearchRequest,
) ([]source.LiveSearchResult, error) {
	response, err := client.client.LiveSearch(ctx, &pb.SourceSearchRequest{
		Query: request.Query,
		Limit: int32(request.Limit),
	})
	if err != nil {
		return nil, err
	}

	return sourceHitsFromPB(response.GetHits()), nil
}

func (client *Client) SearchAndHydrate(
	ctx context.Context,
	_ source.IngestService,
	request source.LiveSearchRequest,
) ([]source.LiveSearchResult, error) {
	response, err := client.client.SearchAndHydrate(ctx, &pb.SourceSearchRequest{
		Query: request.Query,
		Limit: int32(request.Limit),
	})
	if err != nil {
		return nil, err
	}

	return sourceHitsFromPB(response.GetHits()), nil
}

func (client *Client) ApplyAction(
	ctx context.Context,
	request source.ActionRequest,
) (source.ActionResult, error) {
	parameters, err := rpc.EncodeMapForSource(request.Parameters)
	if err != nil {
		return source.ActionResult{}, err
	}
	response, err := client.client.ApplyAction(ctx, &pb.SourceActionRequest{
		ObjectDigest:   string(request.ObjectDigest),
		Action:         request.Action,
		ParametersJson: parameters,
	})
	if err != nil {
		return source.ActionResult{}, err
	}
	attributes, err := rpc.DecodeMapForSource(response.GetAttributesJson())
	if err != nil {
		return source.ActionResult{}, err
	}

	return source.ActionResult{
		Applied:    response.GetApplied(),
		Action:     response.GetAction(),
		Attributes: attributes,
	}, nil
}

func sourceHitsFromPB(hits []*pb.SourceSearchHit) []source.LiveSearchResult {
	results := make([]source.LiveSearchResult, 0, len(hits))
	for _, hit := range hits {
		results = append(results, source.LiveSearchResult{
			ObjectDigest:    contracts.ObjectDigest(hit.GetObjectDigest()),
			ExternalID:      hit.GetExternalId(),
			ExternalVersion: hit.GetExternalVersion(),
			Hydrated:        hit.GetHydrated(),
		})
	}

	return results
}
