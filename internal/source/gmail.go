// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"strconv"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/mailmessage"
)

type GmailBackend interface {
	Search(ctx context.Context, query string, limit int) ([]GmailSearchHit, error)
	ListMessages(
		ctx context.Context,
		request GmailListRequest,
	) (GmailListPage, error)
	ListHistory(
		ctx context.Context,
		request GmailHistoryRequest,
	) (GmailHistoryPage, error)
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

const (
	concurrentGmailPartWaitTimeout = 5 * time.Second
	concurrentGmailPartPoll        = 100 * time.Millisecond
)

type GmailSearchHit struct {
	MessageID string
	Version   string
}

type GmailListRequest struct {
	Query     string
	PageToken string
	Limit     int
}

type GmailListPage struct {
	NextPageToken  string
	Hits           []GmailSearchHit
	ResultEstimate int
}

type GmailHistoryRequest struct {
	StartHistoryID string
	PageToken      string
	Limit          int
}

type GmailHistoryPage struct {
	NextPageToken   string
	LatestHistoryID string
	MessageIDs      []string
	Expired         bool
}

type GmailAttachment struct {
	FileName  string
	MediaType string
	ID        string
	Version   string
	Content   []byte
}

type GmailMessage struct {
	ObservedAt   time.Time
	Headers      map[string]string
	Metadata     map[string]any
	MessageID    string
	Version      string
	ThreadID     string
	Subject      string
	Snippet      string
	BodyMediaTyp string
	RawMessage   []byte
	Body         []byte
	Attachments  []GmailAttachment
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

func (adapter *GmailAdapter) Pull(
	ctx context.Context,
	service IngestService,
	request PullRequest,
) ([]IngestObject, contracts.SourceCursor, error) {
	cursor := cloneCursor(request.Cursor)
	mode := firstNonEmpty(stringValue(cursor["mode"]), "full")

	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}

	switch mode {
	case "full", "backfill":
		return adapter.pullFull(ctx, service, cursor, limit)
	case "history":
		return adapter.pullHistory(ctx, service, cursor, limit)
	default:
		return nil, contracts.SourceCursor{}, fmt.Errorf(
			"unsupported gmail backfill mode %q",
			mode,
		)
	}
}

func (adapter *GmailAdapter) pullFull(
	ctx context.Context,
	service IngestService,
	cursor map[string]any,
	limit int,
) ([]IngestObject, contracts.SourceCursor, error) {
	query := stringValue(cursor["query"])
	log.Printf(
		"source gmail list: started source=%s/%s mode=full query_set=%t page_token_set=%t limit=%d",
		adapter.Kind(),
		adapter.name,
		query != "",
		stringValue(cursor["page_token"]) != "",
		limit,
	)

	page, err := adapter.backend.ListMessages(ctx, GmailListRequest{
		Query:     query,
		PageToken: stringValue(cursor["page_token"]),
		Limit:     limit,
	})
	if err != nil {
		cursor["last_error"] = err.Error()

		return nil, adapter.cursor(cursor), err
	}

	log.Printf(
		"source gmail list: completed source=%s/%s hits=%d next_page=%t result_estimate=%d",
		adapter.Kind(),
		adapter.name,
		len(page.Hits),
		page.NextPageToken != "",
		page.ResultEstimate,
	)

	objects, failedID, err := adapter.hydrateHits(ctx, service, page.Hits)
	if err != nil {
		cursor["failed_message_ids"] = []string{failedID}
		cursor["failed_count"] = 1
		cursor["last_error"] = err.Error()

		return nil, adapter.cursor(cursor), err
	}

	cursor["mode"] = "full"
	cursor["query"] = query
	cursor["page_token"] = page.NextPageToken
	cursor["result_estimate"] = page.ResultEstimate
	cursor["completed"] = page.NextPageToken == ""
	cursor["last_error"] = ""

	if len(page.Hits) > 0 {
		last := page.Hits[len(page.Hits)-1]
		cursor["last_message_id"] = last.MessageID
		cursor["latest_history_id"] = last.Version
	}

	return objects, adapter.cursor(cursor), nil
}

func (adapter *GmailAdapter) pullHistory(
	ctx context.Context,
	service IngestService,
	cursor map[string]any,
	limit int,
) ([]IngestObject, contracts.SourceCursor, error) {
	anchor := stringValue(cursor["history_anchor"])
	if anchor == "" {
		return nil, contracts.SourceCursor{}, errors.New(
			"gmail history backfill requires history_anchor cursor",
		)
	}

	page, err := adapter.backend.ListHistory(ctx, GmailHistoryRequest{
		StartHistoryID: anchor,
		PageToken:      stringValue(cursor["page_token"]),
		Limit:          limit,
	})
	if err != nil {
		cursor["last_error"] = err.Error()

		return nil, adapter.cursor(cursor), err
	}

	if page.Expired {
		cursor["history_expired"] = true
		cursor["last_error"] = "gmail history cursor expired"

		return nil, adapter.cursor(cursor), errors.New(
			"gmail history cursor expired; run full backfill",
		)
	}

	objects, failedID, err := adapter.hydrateMessageIDs(ctx, service, page.MessageIDs)
	if err != nil {
		cursor["failed_message_ids"] = []string{failedID}
		cursor["failed_count"] = 1
		cursor["last_error"] = err.Error()

		return nil, adapter.cursor(cursor), err
	}

	cursor["mode"] = "history"
	cursor["page_token"] = page.NextPageToken
	cursor["latest_history_id"] = firstNonEmpty(page.LatestHistoryID, anchor)
	cursor["history_expired"] = false
	cursor["completed"] = page.NextPageToken == ""

	cursor["last_error"] = ""
	if page.NextPageToken == "" && page.LatestHistoryID != "" {
		cursor["history_anchor"] = page.LatestHistoryID
	}

	if len(page.MessageIDs) > 0 {
		cursor["last_message_id"] = page.MessageIDs[len(page.MessageIDs)-1]
	}

	return objects, adapter.cursor(cursor), nil
}

func (adapter *GmailAdapter) hydrateHits(
	ctx context.Context,
	service IngestService,
	hits []GmailSearchHit,
) ([]IngestObject, string, error) {
	objects := make([]IngestObject, 0, len(hits))
	for _, hit := range hits {
		log.Printf("source gmail hydrate: started message_id=%s", hit.MessageID)

		message, err := adapter.backend.GetMessage(ctx, hit.MessageID)
		if err != nil {
			return nil, hit.MessageID, err
		}

		if message.Version == "" {
			message.Version = hit.Version
		}

		object, err := adapter.messageObject(ctx, service, message)
		if err != nil {
			return nil, hit.MessageID, err
		}

		log.Printf("source gmail hydrate: completed message_id=%s", hit.MessageID)

		objects = append(objects, object)
	}

	return objects, "", nil
}

func (adapter *GmailAdapter) hydrateMessageIDs(
	ctx context.Context,
	service IngestService,
	messageIDs []string,
) ([]IngestObject, string, error) {
	objects := make([]IngestObject, 0, len(messageIDs))
	for _, messageID := range messageIDs {
		log.Printf("source gmail hydrate: started message_id=%s", messageID)

		message, err := adapter.backend.GetMessage(ctx, messageID)
		if err != nil {
			return nil, messageID, err
		}

		object, err := adapter.messageObject(ctx, service, message)
		if err != nil {
			return nil, messageID, err
		}

		log.Printf("source gmail hydrate: completed message_id=%s", messageID)

		objects = append(objects, object)
	}

	return objects, "", nil
}

func (adapter *GmailAdapter) cursor(cursor map[string]any) contracts.SourceCursor {
	cursor["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	return contracts.SourceCursor{
		SchemaVersion: contracts.SchemaVersionPhase00,
		SourceKind:    adapter.Kind(),
		SourceName:    adapter.name,
		Cursor:        cursor,
		UpdatedAt:     time.Now().UTC(),
	}
}

func (adapter *GmailAdapter) SearchAndHydrate(
	ctx context.Context,
	service IngestService,
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

		digest, found, err := service.LookupSourceObject(ctx, ref)
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
	service IngestService,
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
	service IngestService,
	message GmailMessage,
) (IngestObject, error) {
	if strings.TrimSpace(message.MessageID) == "" {
		return IngestObject{}, errors.New("gmail message_id is required")
	}

	observed := message.ObservedAt
	if observed.IsZero() {
		observed = time.Now().UTC()
	}

	canonical, err := gmailCanonicalMessage(message)
	if err != nil {
		return IngestObject{}, err
	}

	parts := []contracts.CompoundPart{}

	if service != nil {
		created, err := adapter.writeMessageParts(ctx, service, message, canonical, observed)
		if err != nil {
			return IngestObject{}, err
		}

		parts = created
	}

	objectID := "mail_message:" + canonical.MessageID

	return IngestObject{
		ObservedAt:  observed,
		SourceKind:  adapter.Kind(),
		SourceName:  adapter.name,
		ExternalID:  message.MessageID,
		ExternalVer: message.Version,
		Compound: &CompoundObject{
			ObjectID:   objectID,
			MediaType:  mailmessage.MediaType,
			SourceHint: firstNonEmpty(canonical.Subject, message.Subject),
			ContentRoles: []string{
				contracts.MailMessageContentRole,
				contracts.MailMessageContainerRole,
			},
			Facets: []contracts.Facet{
				{
					Kind:     contracts.MailMessageFacetKind,
					Metadata: gmailMailMetadata(message, canonical),
				},
				{Kind: "container"},
			},
			Parts: parts,
		},
	}, nil
}

func gmailCanonicalMessage(message GmailMessage) (mailmessage.Message, error) {
	if len(message.RawMessage) > 0 {
		canonical, err := mailmessage.Parse(message.RawMessage, "gmail:"+message.MessageID)
		if err != nil {
			return mailmessage.Message{}, fmt.Errorf("parse gmail mail message: %w", err)
		}

		return canonical, nil
	}

	headers := map[string]string{}
	for key, value := range message.Headers {
		headers[strings.ToLower(key)] = value
	}

	canonical := mailmessage.Message{
		Headers:       headers,
		Body:          append([]byte{}, message.Body...),
		BodyMediaType: firstNonEmpty(message.BodyMediaTyp, "text/plain"),
		Subject: firstNonEmpty(
			message.Subject,
			headerValue(message.Headers, "subject"),
		),
		Date: headerValue(message.Headers, "date"),
		From: headerValue(message.Headers, "from"),
		To:   headerValue(message.Headers, "to"),
	}
	canonical.MessageID = mailmessage.NormalizeMessageID(
		headerValue(message.Headers, "message-id"),
	)
	canonical.BodyLineHash = mailmessage.BodyLineFingerprint(canonical.Body)

	canonical.Fingerprint = mailmessage.Fingerprint(canonical)
	if canonical.MessageID == "" {
		canonical.GeneratedMessage = true
		canonical.MessageID = fmt.Sprintf(
			"<gmeow-generated-%s@%s>",
			canonical.Fingerprint,
			contracts.MailGeneratedMessageIDHost,
		)
	}

	return canonical, nil
}

func gmailMailMetadata(
	message GmailMessage,
	canonical mailmessage.Message,
) map[string]any {
	metadata := mailmessage.Metadata(
		canonical,
		false,
		contracts.VersionScaleMinor,
		1,
		canonical.Fingerprint,
	)

	metadata["gmail_message_id"] = message.MessageID
	if message.ThreadID != "" {
		metadata["thread_id"] = message.ThreadID
	}

	maps.Copy(metadata, nonNilMap(message.Metadata))

	return metadata
}

func (adapter *GmailAdapter) writeMessageParts(
	ctx context.Context,
	service IngestService,
	message GmailMessage,
	canonical mailmessage.Message,
	observed time.Time,
) ([]contracts.CompoundPart, error) {
	headers, err := json.Marshal(mailmessage.SortedHeaderRows(canonical.Headers))
	if err != nil {
		return nil, fmt.Errorf("marshal gmail message headers: %w", err)
	}

	mimeStructure, err := json.Marshal(mailmessage.MIMEStructure(
		firstNonEmpty(canonical.BodyMediaType, message.BodyMediaTyp),
		gmailMailMessageAttachments(message, canonical),
	))
	if err != nil {
		return nil, fmt.Errorf("marshal gmail MIME structure: %w", err)
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
			mediaType: firstNonEmpty(canonical.BodyMediaType, "text/plain"),
			payload:   canonical.Body,
			facets:    []contracts.Facet{{Kind: "email_part"}},
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
		log.Printf(
			"source gmail part write: started message_id=%s role=%s",
			message.MessageID,
			input.role,
		)
		ref := contracts.SourceObjectRef{
			SourceKind:      adapter.Kind(),
			SourceName:      adapter.name,
			ExternalID:      message.MessageID + ":" + input.role,
			ExternalVersion: message.Version,
		}

		digest, err := ingestGmailPart(ctx, service, ref, IngestObject{
			ObservedAt:   observed,
			Reader:       bytes.NewReader(input.payload),
			MediaType:    input.mediaType,
			SourceKind:   ref.SourceKind,
			SourceName:   ref.SourceName,
			ExternalID:   ref.ExternalID,
			ExternalVer:  message.Version,
			SourceHint:   input.role,
			ContentRoles: []string{input.role},
			Facets:       input.facets,
		})
		if err != nil {
			return nil, fmt.Errorf("ingest gmail message part %q: %w", input.role, err)
		}

		log.Printf(
			"source gmail part write: completed message_id=%s role=%s digest=%s",
			message.MessageID,
			input.role,
			digest,
		)

		parts = append(parts, contracts.CompoundPart{
			Digest: digest,
			Role:   input.role,
			Order:  index,
		})
	}

	for index, attachment := range gmailAttachments(message, canonical) {
		attachmentID := firstNonEmpty(
			attachment.ID,
			attachment.FileName,
			strconv.Itoa(index),
		)
		log.Printf(
			"source gmail part write: started message_id=%s role=attachment attachment_id=%s",
			message.MessageID,
			attachmentID,
		)
		ref := contracts.SourceObjectRef{
			SourceKind:      adapter.Kind(),
			SourceName:      adapter.name,
			ExternalID:      message.MessageID + ":attachment:" + attachmentID,
			ExternalVersion: firstNonEmpty(attachment.Version, message.Version),
		}

		digest, err := ingestGmailPart(ctx, service, ref, IngestObject{
			ObservedAt:   observed,
			Reader:       bytes.NewReader(attachment.Content),
			MediaType:    firstNonEmpty(attachment.MediaType, "application/octet-stream"),
			SourceKind:   ref.SourceKind,
			SourceName:   ref.SourceName,
			ExternalID:   ref.ExternalID,
			ExternalVer:  ref.ExternalVersion,
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
			return nil, fmt.Errorf("ingest gmail attachment %q: %w", attachmentID, err)
		}

		log.Printf(
			"source gmail part write: completed message_id=%s role=attachment attachment_id=%s digest=%s",
			message.MessageID,
			attachmentID,
			digest,
		)

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

func ingestGmailPart(
	ctx context.Context,
	service IngestService,
	ref contracts.SourceObjectRef,
	object IngestObject,
) (contracts.ObjectDigest, error) {
	digest, _, err := service.Ingest(ctx, object)
	if err == nil {
		return digest, nil
	}

	if !errors.Is(err, ErrSourceIngestInProgress) {
		return "", fmt.Errorf("ingest gmail part: %w", err)
	}

	deadline := time.Now().Add(concurrentGmailPartWaitTimeout)

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	ticker := time.NewTicker(concurrentGmailPartPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for concurrent gmail part ingest: %w", ctx.Err())
		case <-timer.C:
			return "", fmt.Errorf("wait for concurrent gmail part ingest: %w", err)
		case <-ticker.C:
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return "", fmt.Errorf("wait for concurrent gmail part ingest: %w", err)
			}

			lookupCtx, cancelLookup := context.WithTimeout(ctx, remaining)
			digest, found, lookupErr := service.LookupSourceObject(lookupCtx, ref)

			cancelLookup()

			if lookupErr != nil {
				return "", fmt.Errorf("lookup concurrent gmail part: %w", lookupErr)
			}

			if found {
				return digest, nil
			}
		}
	}
}

func gmailAttachments(
	message GmailMessage,
	canonical mailmessage.Message,
) []GmailAttachment {
	if len(canonical.Attachments) == 0 {
		return message.Attachments
	}

	attachments := make([]GmailAttachment, 0, len(canonical.Attachments))
	for index, attachment := range canonical.Attachments {
		attachments = append(attachments, GmailAttachment{
			ID:        fmt.Sprintf("raw:%d", index),
			FileName:  attachment.FileName,
			MediaType: attachment.MediaType,
			Content:   append([]byte{}, attachment.Content...),
			Version:   message.Version,
		})
	}

	return attachments
}

func headerValue(headers map[string]string, name string) string {
	for candidate, value := range headers {
		if strings.EqualFold(candidate, name) {
			return value
		}
	}

	return ""
}

func gmailMailMessageAttachments(
	message GmailMessage,
	canonical mailmessage.Message,
) []mailmessage.Attachment {
	if len(canonical.Attachments) > 0 {
		return canonical.Attachments
	}

	attachments := make([]mailmessage.Attachment, 0, len(message.Attachments))
	for _, attachment := range message.Attachments {
		attachments = append(attachments, mailmessage.Attachment{
			Content:   attachment.Content,
			ID:        attachment.ID,
			FileName:  attachment.FileName,
			MediaType: attachment.MediaType,
		})
	}

	return attachments
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

func stringSliceValue(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := stringValue(item); value != "" {
				values = append(values, value)
			}
		}

		return values
	default:
		return nil
	}
}
