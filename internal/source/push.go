// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type PushRecord struct {
	ObservedAt   time.Time
	Payload      []byte
	MediaType    string
	SourceName   string
	ExternalID   string
	ExternalVer  string
	FacetKind    string
	DisplayName  string
	ContentRoles []string
}

type PushIngestor struct {
	name string
	kind string
}

func NewPushIngestor(name, kind string) (*PushIngestor, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("push source name is required")
	}

	if strings.TrimSpace(kind) == "" {
		kind = "ringme"
	}

	return &PushIngestor{name: name, kind: kind}, nil
}

func (ingestor *PushIngestor) Name() string {
	return ingestor.name
}

func (ingestor *PushIngestor) Kind() string {
	return ingestor.kind
}

func (*PushIngestor) Capabilities() []string {
	return []string{CapabilityPush}
}

func (ingestor *PushIngestor) Object(record PushRecord) (IngestObject, error) {
	if len(record.Payload) == 0 {
		return IngestObject{}, errors.New("push payload is required")
	}

	if strings.TrimSpace(record.ExternalID) == "" {
		return IngestObject{}, errors.New("push external_id is required")
	}

	mediaType := strings.TrimSpace(record.MediaType)
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}

	facetKind := strings.TrimSpace(record.FacetKind)
	if facetKind == "" {
		facetKind = "phone"
	}

	sourceName := strings.TrimSpace(record.SourceName)
	if sourceName == "" {
		sourceName = ingestor.name
	}

	return IngestObject{
		ObservedAt:   record.ObservedAt,
		Reader:       bytes.NewReader(record.Payload),
		MediaType:    mediaType,
		SourceKind:   ingestor.kind,
		SourceName:   sourceName,
		ExternalID:   record.ExternalID,
		ExternalVer:  record.ExternalVer,
		SourceHint:   record.DisplayName,
		ContentRoles: append([]string{"source"}, record.ContentRoles...),
		Facets: []contracts.Facet{{
			Kind: facetKind,
			Metadata: map[string]any{
				"display_name": record.DisplayName,
			},
		}},
	}, nil
}

func (ingestor *PushIngestor) Ingest(
	ctx context.Context,
	service *Service,
	record PushRecord,
) (contracts.ObjectDigest, bool, error) {
	object, err := ingestor.Object(record)
	if err != nil {
		return "", false, err
	}

	return service.Ingest(ctx, object)
}
