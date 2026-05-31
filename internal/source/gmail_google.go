// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/oauth2/google"
	gmail "google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const defaultGmailUserID = "me"

type GoogleGmailBackend struct {
	service *gmail.Service
	userID  string
}

func NewGoogleGmailBackend(
	ctx context.Context,
	credentialsJSON []byte,
	userID string,
	delegatedSubject string,
) (*GoogleGmailBackend, error) {
	if len(credentialsJSON) == 0 {
		return nil, errors.New("gmail credentials JSON is required")
	}
	if strings.TrimSpace(userID) == "" {
		userID = defaultGmailUserID
	}

	options := []option.ClientOption{option.WithScopes(gmail.GmailModifyScope)}
	if strings.TrimSpace(delegatedSubject) == "" {
		options = append(options, option.WithCredentialsJSON(credentialsJSON))
	} else {
		config, err := google.JWTConfigFromJSON(credentialsJSON, gmail.GmailModifyScope)
		if err != nil {
			return nil, fmt.Errorf("parse delegated gmail credentials: %w", err)
		}
		config.Subject = strings.TrimSpace(delegatedSubject)
		options = append(options, option.WithHTTPClient(config.Client(ctx)))
	}

	service, err := gmail.NewService(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create gmail service: %w", err)
	}

	return &GoogleGmailBackend{service: service, userID: userID}, nil
}

func (backend *GoogleGmailBackend) Search(
	ctx context.Context,
	query string,
	limit int,
) ([]GmailSearchHit, error) {
	page, err := backend.ListMessages(ctx, GmailListRequest{
		Query: query,
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}

	return page.Hits, nil
}

func (backend *GoogleGmailBackend) ListMessages(
	ctx context.Context,
	request GmailListRequest,
) (GmailListPage, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 20
	}

	call := backend.service.Users.Messages.List(backend.userID).
		Q(request.Query).
		MaxResults(int64(limit)).
		Context(ctx)
	if strings.TrimSpace(request.PageToken) != "" {
		call.PageToken(request.PageToken)
	}
	response, err := call.Do()
	if err != nil {
		return GmailListPage{}, fmt.Errorf("gmail list messages: %w", err)
	}

	hits := make([]GmailSearchHit, 0, len(response.Messages))
	for _, message := range response.Messages {
		hits = append(hits, GmailSearchHit{
			MessageID: message.Id,
			Version:   fmt.Sprint(message.HistoryId),
		})
	}

	return GmailListPage{
		Hits:           hits,
		NextPageToken:  response.NextPageToken,
		ResultEstimate: int(response.ResultSizeEstimate),
	}, nil
}

func (backend *GoogleGmailBackend) ListHistory(
	ctx context.Context,
	request GmailHistoryRequest,
) (GmailHistoryPage, error) {
	startHistoryID, err := strconv.ParseUint(
		strings.TrimSpace(request.StartHistoryID),
		10,
		64,
	)
	if err != nil {
		return GmailHistoryPage{}, fmt.Errorf(
			"parse gmail history anchor %q: %w",
			request.StartHistoryID,
			err,
		)
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}

	call := backend.service.Users.History.List(backend.userID).
		StartHistoryId(startHistoryID).
		MaxResults(int64(limit)).
		Context(ctx)
	if strings.TrimSpace(request.PageToken) != "" {
		call.PageToken(request.PageToken)
	}
	response, err := call.Do()
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			return GmailHistoryPage{Expired: true}, nil
		}

		return GmailHistoryPage{}, fmt.Errorf("gmail list history: %w", err)
	}

	seen := map[string]bool{}
	messageIDs := []string{}
	for _, history := range response.History {
		for _, changed := range history.MessagesAdded {
			messageIDs = appendGmailHistoryMessageID(
				messageIDs,
				seen,
				changed.Message,
			)
		}
		for _, changed := range history.LabelsAdded {
			messageIDs = appendGmailHistoryMessageID(
				messageIDs,
				seen,
				changed.Message,
			)
		}
		for _, changed := range history.LabelsRemoved {
			messageIDs = appendGmailHistoryMessageID(
				messageIDs,
				seen,
				changed.Message,
			)
		}
		for _, message := range history.Messages {
			messageIDs = appendGmailHistoryMessageID(messageIDs, seen, message)
		}
	}

	return GmailHistoryPage{
		MessageIDs:      messageIDs,
		NextPageToken:   response.NextPageToken,
		LatestHistoryID: fmt.Sprint(response.HistoryId),
	}, nil
}

func appendGmailHistoryMessageID(
	messageIDs []string,
	seen map[string]bool,
	message *gmail.Message,
) []string {
	if message == nil || strings.TrimSpace(message.Id) == "" || seen[message.Id] {
		return messageIDs
	}
	seen[message.Id] = true

	return append(messageIDs, message.Id)
}

func (backend *GoogleGmailBackend) GetMessage(
	ctx context.Context,
	messageID string,
) (GmailMessage, error) {
	message, err := backend.service.Users.Messages.Get(backend.userID, messageID).
		Format("raw").
		Context(ctx).
		Do()
	if err != nil {
		return GmailMessage{}, fmt.Errorf("gmail get message %s: %w", messageID, err)
	}

	converted := GmailMessage{
		Headers:      map[string]string{},
		Metadata:     map[string]any{"label_ids": append([]string{}, message.LabelIds...)},
		MessageID:    message.Id,
		Version:      fmt.Sprint(message.HistoryId),
		ThreadID:     message.ThreadId,
		Snippet:      message.Snippet,
		BodyMediaTyp: "text/plain",
	}
	if message.Raw != "" {
		raw, decodeErr := decodeGmailData(message.Raw)
		if decodeErr != nil {
			return GmailMessage{}, fmt.Errorf(
				"decode gmail raw message %s: %w",
				messageID,
				decodeErr,
			)
		}

		converted.RawMessage = raw
	}
	collectGmailPart(message.Payload, &converted)
	if converted.Subject == "" {
		converted.Subject = converted.Headers["Subject"]
	}

	return converted, nil
}

func (backend *GoogleGmailBackend) ModifyMessage(
	ctx context.Context,
	messageID, action string,
	parameters map[string]any,
) (map[string]any, error) {
	request := &gmail.ModifyMessageRequest{}
	switch action {
	case "apply_label":
		request.AddLabelIds = stringList(parameters["label_ids"])
	case "remove_label":
		request.RemoveLabelIds = stringList(parameters["label_ids"])
	case "archive":
		request.RemoveLabelIds = []string{"INBOX"}
	case "mark_read":
		request.RemoveLabelIds = []string{"UNREAD"}
	case "star":
		request.AddLabelIds = []string{"STARRED"}
	default:
		return nil, fmt.Errorf("%w: gmail action %q", ErrUnsupportedOperation, action)
	}

	message, err := backend.service.Users.Messages.Modify(backend.userID, messageID, request).
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("gmail modify message %s: %w", messageID, err)
	}

	return map[string]any{
		"message_id": message.Id,
		"thread_id":  message.ThreadId,
		"label_ids":  append([]string{}, message.LabelIds...),
		"history_id": message.HistoryId,
	}, nil
}

func collectGmailPart(part *gmail.MessagePart, message *GmailMessage) {
	if part == nil {
		return
	}

	for _, header := range part.Headers {
		message.Headers[header.Name] = header.Value
	}

	if part.MimeType != "" && part.Body != nil && len(part.Body.Data) > 0 {
		body, err := decodeGmailData(part.Body.Data)
		if err == nil {
			if strings.HasPrefix(part.MimeType, "text/") && len(message.Body) == 0 {
				message.Body = body
				message.BodyMediaTyp = part.MimeType
			} else {
				message.Attachments = append(message.Attachments, GmailAttachment{
					Content:   body,
					FileName:  part.Filename,
					MediaType: part.MimeType,
					ID:        part.PartId,
					Version:   message.Version,
				})
			}
		}
	}

	for _, child := range part.Parts {
		collectGmailPart(child, message)
	}
}

func decodeGmailData(value string) ([]byte, error) {
	decoded, rawErr := base64.RawURLEncoding.DecodeString(value)
	if rawErr == nil {
		return decoded, nil
	}

	decoded, paddedErr := base64.URLEncoding.DecodeString(value)
	if paddedErr != nil {
		return nil, fmt.Errorf(
			"decode gmail base64 payload: %w",
			errors.Join(rawErr, paddedErr),
		)
	}

	return decoded, nil
}

func stringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				values = append(values, text)
			}
		}

		return values
	case string:
		if typed == "" {
			return nil
		}

		return []string{typed}
	default:
		return nil
	}
}
