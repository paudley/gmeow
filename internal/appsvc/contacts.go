// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"errors"

	"blackcat.ca/gmeow/internal/contracts"
)

func (services *Services) ContactAggregate(
	ctx context.Context,
	request contracts.ContactAggregateRequest,
) (contracts.ContactAggregate, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactAggregate{}, err
	}

	return reader.ContactAggregate(ctx, request)
}

func (services *Services) ContactSearch(
	ctx context.Context,
	request contracts.ContactSearchRequest,
) (contracts.ContactSearchResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactSearchResponse{}, err
	}

	return reader.ContactSearch(ctx, request)
}

func (services *Services) ResolveContactIdentity(
	ctx context.Context,
	request contracts.ContactIdentityResolveRequest,
) (contracts.ContactIdentityResolveResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactIdentityResolveResponse{}, err
	}

	return reader.ResolveContactIdentity(ctx, request)
}

func (services *Services) ContactFacts(
	ctx context.Context,
	request contracts.ContactFactRequest,
) (contracts.ContactFactResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactFactResponse{}, err
	}

	return reader.ContactFacts(ctx, request)
}

func (services *Services) ContactIdentityDetails(
	ctx context.Context,
	request contracts.ContactIdentityDetailRequest,
) (contracts.ContactIdentityDetailResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, err
	}

	return reader.ContactIdentityDetails(ctx, request)
}

func (services *Services) ContactNeighborhood(
	ctx context.Context,
	request contracts.ContactNeighborhoodRequest,
) (contracts.ContactNeighborhoodResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, err
	}

	return reader.ContactNeighborhood(ctx, request)
}

func (services *Services) ContactMessages(
	ctx context.Context,
	request contracts.ContactMessageRequest,
) (contracts.ContactMessageResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactMessageResponse{}, err
	}

	return reader.ContactMessages(ctx, request)
}

func (services *Services) ContactVectorSearch(
	ctx context.Context,
	request contracts.ContactVectorSearchRequest,
) (contracts.ContactVectorSearchResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.ContactVectorSearchResponse{}, err
	}

	return reader.ContactVectorSearch(ctx, request)
}

func (services *Services) SimilarContacts(
	ctx context.Context,
	request contracts.SimilarContactsRequest,
) (contracts.SimilarContactsResponse, error) {
	reader, err := services.contactQueryReader()
	if err != nil {
		return contracts.SimilarContactsResponse{}, err
	}

	return reader.SimilarContacts(ctx, request)
}

func (services *Services) contactQueryReader() (ContactQueryReader, error) {
	reader, ok := services.query.(ContactQueryReader)
	if !ok {
		return nil, errors.New("contact query reader is not configured")
	}

	return reader, nil
}
