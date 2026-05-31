// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mailmessage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

type Message struct {
	Headers          map[string]string
	Subject          string
	BodyMediaType    string
	MessageID        string
	Fingerprint      string
	BodyLineHash     string
	Date             string
	From             string
	To               string
	Body             []byte
	Attachments      []Attachment
	Raw              []byte
	GeneratedMessage bool
}

type Attachment struct {
	ID        string
	FileName  string
	MediaType string
	Content   []byte
}

func Parse(raw []byte, sourceHint string) (Message, error) {
	parsed, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return Message{}, fmt.Errorf("parse mail %s: %w", sourceHint, err)
	}

	body, attachments, err := extractBodyAndAttachments(parsed.Header, parsed.Body)
	if err != nil {
		return Message{}, err
	}

	headers := HeaderMap(parsed.Header)
	message := Message{
		Raw:           raw,
		Headers:       headers,
		Body:          body,
		BodyMediaType: "text/plain",
		Attachments:   attachments,
		Subject:       headers["subject"],
		Date:          headers["date"],
		From:          headers["from"],
		To:            headers["to"],
	}
	message.MessageID = NormalizeMessageID(headers["message-id"])
	message.BodyLineHash = BodyLineFingerprint(message.Body)

	message.Fingerprint = Fingerprint(message)
	if message.MessageID == "" {
		message.GeneratedMessage = true
		message.MessageID = fmt.Sprintf(
			"<gmeow-generated-%s@%s>",
			message.Fingerprint,
			contracts.MailGeneratedMessageIDHost,
		)
	}

	return message, nil
}

func Metadata(
	message Message,
	collision bool,
	maxScale string,
	versionCount int,
	canonicalVersionID string,
) map[string]any {
	return map[string]any{
		"rfc_message_id":           message.MessageID,
		"subject":                  message.Subject,
		"date":                     message.Date,
		"from":                     message.From,
		"to":                       message.To,
		"generated_message_id":     message.GeneratedMessage,
		"canonical_fingerprint":    message.Fingerprint,
		"body_line_fingerprint":    message.BodyLineHash,
		"canonical_version_id":     canonicalVersionID,
		"version_count":            versionCount,
		"max_scale":                maxScale,
		"message_id_collision":     collision,
		"analysis_scope":           contracts.AnalysisScopeCanonical,
		"analysis_input_body_line": message.BodyLineHash,
	}
}

func MIMEStructure(bodyMediaType string, attachments []Attachment) map[string]any {
	attachmentRows := make([]map[string]any, 0, len(attachments))
	for index, attachment := range attachments {
		attachmentRows = append(attachmentRows, map[string]any{
			"id":         firstNonEmpty(attachment.ID, fmt.Sprintf("raw:%d", index)),
			"filename":   attachment.FileName,
			"media_type": attachment.MediaType,
			"size":       len(attachment.Content),
		})
	}

	return map[string]any{
		"body_media_type": firstNonEmpty(bodyMediaType, "text/plain"),
		"attachments":     attachmentRows,
	}
}

func HeaderMap(header mail.Header) map[string]string {
	result := map[string]string{}
	for key, values := range header {
		result[strings.ToLower(key)] = strings.Join(values, "\n")
	}

	return result
}

func SortedHeaderRows(headers map[string]string) []map[string]string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}

	sort.Strings(names)

	rows := make([]map[string]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]string{"name": name, "value": headers[name]})
	}

	return rows
}

func NormalizeMessageID(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	trimmed = strings.Trim(trimmed, "<>")
	if trimmed == "" {
		return ""
	}

	return "<" + strings.ToLower(trimmed) + ">"
}

func Fingerprint(message Message) string {
	parts := []string{
		NormalizeMessageID(message.Headers["message-id"]),
		strings.ToLower(strings.TrimSpace(message.Subject)),
		strings.ToLower(strings.TrimSpace(message.From)),
		strings.ToLower(strings.TrimSpace(message.To)),
		collapseWhitespace(string(message.Body)),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))

	return hex.EncodeToString(sum[:])[:32]
}

func BodyLineFingerprint(body []byte) string {
	lines := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")

	normalized := make([]string, 0, len(lines))
	for _, line := range lines {
		collapsed := collapseWhitespace(line)
		if collapsed != "" {
			normalized = append(normalized, collapsed)
		}
	}

	sum := sha256.Sum256([]byte(strings.Join(normalized, "\n")))

	return hex.EncodeToString(sum[:])[:32]
}

func extractBodyAndAttachments(
	headers mail.Header,
	body io.Reader,
) ([]byte, []Attachment, error) {
	textBodies := []string{}

	attachments := []Attachment{}

	err := extractPart(headers, body, &textBodies, &attachments)
	if err != nil {
		return nil, nil, err
	}

	return []byte(strings.Join(textBodies, "\n")), attachments, nil
}

func extractPart(
	headers mail.Header,
	body io.Reader,
	textBodies *[]string,
	attachments *[]Attachment,
) error {
	contentType := headers.Get("Content-Type")

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return extractLeafPart(headers, body, mediaType, textBodies, attachments)
	}

	reader := multipart.NewReader(body, params["boundary"])
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("read multipart mail part: %w", err)
		}

		err = extractPart(
			mail.Header(part.Header),
			part,
			textBodies,
			attachments,
		)
		if err != nil {
			return err
		}
	}
}

func extractLeafPart(
	headers mail.Header,
	body io.Reader,
	mediaType string,
	textBodies *[]string,
	attachments *[]Attachment,
) error {
	content, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("read mail part: %w", err)
	}

	disposition := ""

	parsedDisposition, _, err := mime.ParseMediaType(
		headers.Get("Content-Disposition"),
	)
	if err == nil {
		disposition = parsedDisposition
	}

	fileName := partFilename(headers)
	if disposition == "attachment" || fileName != "" {
		*attachments = append(*attachments, Attachment{
			Content:   content,
			FileName:  fileName,
			MediaType: firstNonEmpty(mediaType, "application/octet-stream"),
		})

		return nil
	}

	if mediaType == "text/plain" || (mediaType == "" && len(*textBodies) == 0) {
		*textBodies = append(*textBodies, string(content))
	}

	return nil
}

func partFilename(headers mail.Header) string {
	_, params, err := mime.ParseMediaType(headers.Get("Content-Disposition"))
	if err == nil && params["filename"] != "" {
		return params["filename"]
	}

	_, params, err = mime.ParseMediaType(headers.Get("Content-Type"))
	if err == nil {
		return params["name"]
	}

	return ""
}

func collapseWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}
