// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type GmailBackend interface {
	Search(ctx context.Context, query string, limit int) ([]GmailSearchHit, error)
	GetMessage(ctx context.Context, messageID string) (GmailMessage, error)
	ModifyMessage(
		ctx context.Context,
		messageID, action string,
		parameters map[string]any,
	) (map[string]any, error)
}

type GmailAdapter struct {
	backend GmailBackend
	name    string
}

type GmailSearchHit struct {
	MessageID string
	Version   string
}

type GmailAttachment struct {
	Content   []byte
	FileName  string
	MediaType string
	ID        string
	Version   string
}

type GmailMessage struct {
	ObservedAt   time.Time
	Headers      map[string]string
	Metadata     map[string]any
	Body         []byte
	Attachments  []GmailAttachment
	MessageID    string
	Version      string
	ThreadID     string
	Subject      string
	Snippet      string
	BodyMediaTyp string
}

func NewGmailAdapter(name string, backend GmailBackend) (*GmailAdapter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("gmail source name is required")
	}

	if backend == nil {
		return nil, errors.New("gmail backend is required")
	}

	return &GmailAdapter{name: name, backend: backend}, nil
}

func (adapter *GmailAdapter) Name() string {
	return adapter.name
}

func (*GmailAdapter) Kind() string {
	return "gmail"
}

func (*GmailAdapter) Capabilities() []string {
	return []string{
		CapabilityBackfill,
		CapabilityHydrate,
		CapabilityLiveSearch,
		CapabilityLiveRetrieve,
		CapabilityActions,
	}
}

func (adapter *GmailAdapter) Hydrate(
	ctx context.Context,
	externalID string,
) (IngestObject, error) {
	message, err := adapter.backend.GetMessage(ctx, externalID)
	if err != nil {
		return IngestObject{}, err
	}

	return adapter.messageObject(ctx, nil, message)
}

func (adapter *GmailAdapter) LiveRetrieve(
	ctx context.Context,
	externalID string,
) (IngestObject, error) {
	return adapter.Hydrate(ctx, externalID)
}

func (adapter *GmailAdapter) LiveSearch(
	ctx context.Context,
	request LiveSearchRequest,
) ([]LiveSearchResult, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	hits, err := adapter.backend.Search(ctx, request.Query, limit)
	if err != nil {
		return nil, err
	}

	results := make([]LiveSearchResult, 0, len(hits))
	for _, hit := range hits {
		results = append(results, LiveSearchResult{
			ExternalID:      hit.MessageID,
			ExternalVersion: hit.Version,
		})
	}

	return results, nil
}

func (adapter *GmailAdapter) SearchAndHydrate(
	ctx context.Context,
	service *Service,
	request LiveSearchRequest,
) ([]LiveSearchResult, error) {
	results, err := adapter.LiveSearch(ctx, request)
	if err != nil {
		return nil, err
	}

	for index := range results {
		ref := contracts.SourceObjectRef{
			SourceKind:      adapter.Kind(),
			SourceName:      adapter.name,
			ExternalID:      results[index].ExternalID,
			ExternalVersion: results[index].ExternalVersion,
		}
		digest, found, err := service.store.LookupSourceObject(ctx, ref)
		if err != nil {
			return nil, err
		}

		if found {
			results[index].ObjectDigest = digest

			continue
		}

		message, err := adapter.backend.GetMessage(ctx, results[index].ExternalID)
		if err != nil {
			return nil, err
		}
		if message.Version == "" {
			message.Version = results[index].ExternalVersion
		}

		object, err := adapter.messageObject(ctx, service, message)
		if err != nil {
			return nil, err
		}

		digest, _, err = service.Ingest(ctx, object)
		if err != nil {
			return nil, err
		}

		results[index].ObjectDigest = digest
		results[index].Hydrated = true
	}

	return results, nil
}

func (adapter *GmailAdapter) ApplyAction(
	ctx context.Context,
	request ActionRequest,
) (ActionResult, error) {
	messageID := strings.TrimSpace(stringValue(request.Parameters["message_id"]))
	if messageID == "" {
		return ActionResult{}, errors.New("gmail action requires message_id parameter")
	}

	switch request.Action {
	case "apply_label", "remove_label", "archive", "mark_read", "star":
	default:
		return ActionResult{}, fmt.Errorf(
			"%w: gmail action %q",
			ErrUnsupportedOperation,
			request.Action,
		)
	}

	attributes, err := adapter.backend.ModifyMessage(
		ctx,
		messageID,
		request.Action,
		request.Parameters,
	)
	if err != nil {
		return ActionResult{}, err
	}

	return ActionResult{
		Action:     request.Action,
		Applied:    true,
		Attributes: attributes,
	}, nil
}

func (adapter *GmailAdapter) IngestMessage(
	ctx context.Context,
	service *Service,
	message GmailMessage,
) (contracts.ObjectDigest, bool, error) {
	object, err := adapter.messageObject(ctx, service, message)
	if err != nil {
		return "", false, err
	}

	return service.Ingest(ctx, object)
}

func (adapter *GmailAdapter) messageObject(
	ctx context.Context,
	service *Service,
	message GmailMessage,
) (IngestObject, error) {
	if strings.TrimSpace(message.MessageID) == "" {
		return IngestObject{}, errors.New("gmail message_id is required")
	}

	observed := message.ObservedAt
	if observed.IsZero() {
		observed = time.Now().UTC()
	}

	parts := []contracts.CompoundPart{}
	if service != nil {
		created, err := adapter.writeMessageParts(ctx, service, message, observed)
		if err != nil {
			return IngestObject{}, err
		}

		parts = created
	}

	objectID := "gmail:" + adapter.name + ":" + message.MessageID
	return IngestObject{
		ObservedAt:  observed,
		SourceKind:  adapter.Kind(),
		SourceName:  adapter.name,
		ExternalID:  message.MessageID,
		ExternalVer: message.Version,
		Compound: &CompoundObject{
			ObjectID:     objectID,
			MediaType:    "application/vnd.gmeow.gmail-message+json",
			SourceHint:   message.Subject,
			ContentRoles: []string{"source", "mail_message"},
			Facets: []contracts.Facet{
				{
					Kind: "mail_message",
					Metadata: map[string]any{
						"message_id": message.MessageID,
						"thread_id":  message.ThreadID,
						"subject":    firstNonEmpty(message.Subject, message.Headers["subject"]),
					},
				},
				{Kind: "container"},
			},
			Parts: parts,
		},
	}, nil
}

func (adapter *GmailAdapter) writeMessageParts(
	ctx context.Context,
	service *Service,
	message GmailMessage,
	observed time.Time,
) ([]contracts.CompoundPart, error) {
	headers, err := json.Marshal(sortedHeaders(message.Headers))
	if err != nil {
		return nil, err
	}

	metadata, err := json.Marshal(nonNilMap(message.Metadata))
	if err != nil {
		return nil, err
	}

	mimeStructure, err := json.Marshal(gmailMIMEStructure(message))
	if err != nil {
		return nil, err
	}

	partInputs := []struct {
		role      string
		mediaType string
		payload   []byte
		facets    []contracts.Facet
	}{
		{
			role:      "rfc822_headers",
			mediaType: "text/rfc822-headers",
			payload:   headers,
			facets:    []contracts.Facet{{Kind: "email_part"}},
		},
		{
			role:      "email_body",
			mediaType: firstNonEmpty(message.BodyMediaTyp, "text/plain"),
			payload:   message.Body,
			facets:    []contracts.Facet{{Kind: "email_part"}},
		},
		{
			role:      "gmail_data",
			mediaType: "application/json",
			payload:   metadata,
			facets:    []contracts.Facet{{Kind: "file"}},
		},
		{
			role:      "mime_structure",
			mediaType: "application/vnd.gmeow.mime-structure+json",
			payload:   mimeStructure,
			facets:    []contracts.Facet{{Kind: "email_part"}},
		},
	}

	parts := make([]contracts.CompoundPart, 0, len(partInputs)+len(message.Attachments))
	for index, input := range partInputs {
		digest, _, err := service.Ingest(ctx, IngestObject{
			ObservedAt:   observed,
			Reader:       bytes.NewReader(input.payload),
			MediaType:    input.mediaType,
			SourceKind:   adapter.Kind(),
			SourceName:   adapter.name,
			ExternalID:   message.MessageID + ":" + input.role,
			ExternalVer:  message.Version,
			SourceHint:   input.role,
			ContentRoles: []string{input.role},
			Facets:       input.facets,
		})
		if err != nil {
			return nil, err
		}

		parts = append(parts, contracts.CompoundPart{
			Digest: digest,
			Role:   input.role,
			Order:  index,
		})
	}

	for index, attachment := range message.Attachments {
		attachmentID := firstNonEmpty(
			attachment.ID,
			attachment.FileName,
			fmt.Sprintf("%d", index),
		)
		digest, _, err := service.Ingest(ctx, IngestObject{
			ObservedAt:   observed,
			Reader:       bytes.NewReader(attachment.Content),
			MediaType:    firstNonEmpty(attachment.MediaType, "application/octet-stream"),
			SourceKind:   adapter.Kind(),
			SourceName:   adapter.name,
			ExternalID:   message.MessageID + ":attachment:" + attachmentID,
			ExternalVer:  firstNonEmpty(attachment.Version, message.Version),
			SourceHint:   attachment.FileName,
			ContentRoles: []string{"attachment"},
			Facets: []contracts.Facet{{
				Kind: "file",
				Metadata: map[string]any{
					"display_name": attachment.FileName,
				},
			}},
		})
		if err != nil {
			return nil, err
		}

		parts = append(parts, contracts.CompoundPart{
			Digest: digest,
			Role:   "attachment",
			Order:  len(partInputs) + index,
			Metadata: map[string]any{
				"filename": attachment.FileName,
			},
		})
	}

	return parts, nil
}

func sortedHeaders(headers map[string]string) []map[string]string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]map[string]string, 0, len(names))
	for _, name := range names {
		out = append(out, map[string]string{
			"name":  name,
			"value": headers[name],
		})
	}

	return out
}

func gmailMIMEStructure(message GmailMessage) map[string]any {
	attachments := make([]map[string]any, 0, len(message.Attachments))
	for _, attachment := range message.Attachments {
		attachments = append(attachments, map[string]any{
			"id":         attachment.ID,
			"filename":   attachment.FileName,
			"media_type": attachment.MediaType,
			"size":       len(attachment.Content),
		})
	}

	return map[string]any{
		"body_media_type": firstNonEmpty(message.BodyMediaTyp, "text/plain"),
		"attachments":     attachments,
	}
}

func nonNilMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}

	return input
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}

	return fmt.Sprint(value)
}
