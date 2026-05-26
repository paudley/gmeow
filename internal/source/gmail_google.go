// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	gmail "google.golang.org/api/gmail/v1"
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
) (*GoogleGmailBackend, error) {
	if len(credentialsJSON) == 0 {
		return nil, errors.New("gmail credentials JSON is required")
	}
	if strings.TrimSpace(userID) == "" {
		userID = defaultGmailUserID
	}

	service, err := gmail.NewService(
		ctx,
		option.WithCredentialsJSON(credentialsJSON),
		option.WithScopes(gmail.GmailModifyScope),
	)
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
	if limit <= 0 {
		limit = 20
	}

	response, err := backend.service.Users.Messages.List(backend.userID).
		Q(query).
		MaxResults(int64(limit)).
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("gmail search: %w", err)
	}

	hits := make([]GmailSearchHit, 0, len(response.Messages))
	for _, message := range response.Messages {
		hits = append(hits, GmailSearchHit{
			MessageID: message.Id,
			Version:   fmt.Sprint(message.HistoryId),
		})
	}

	return hits, nil
}

func (backend *GoogleGmailBackend) GetMessage(
	ctx context.Context,
	messageID string,
) (GmailMessage, error) {
	message, err := backend.service.Users.Messages.Get(backend.userID, messageID).
		Format("full").
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
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}

	return base64.URLEncoding.DecodeString(value)
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
