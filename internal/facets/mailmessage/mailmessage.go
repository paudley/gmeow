// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mailmessage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const MediaType = "application/vnd.gmeow.mail-message+json"

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

type Participant struct {
	Role        string
	DisplayName string
	Address     string
	RawValue    string
	Ordinal     int
}

func Parse(raw []byte, sourceHint string) (Message, error) {
	parsed, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return Message{}, fmt.Errorf("parse mail %s: %w", sourceHint, err)
	}

	body, bodyMediaType, attachments, err := extractBodyAndAttachments(
		parsed.Header,
		parsed.Body,
	)
	if err != nil {
		return Message{}, err
	}

	headers := HeaderMap(parsed.Header)
	message := Message{
		Raw:           raw,
		Headers:       headers,
		Body:          body,
		BodyMediaType: firstNonEmpty(bodyMediaType, "text/plain"),
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
		"sender":                   message.Headers["sender"],
		"reply_to":                 message.Headers["reply-to"],
		"to":                       message.To,
		"cc":                       message.Headers["cc"],
		"bcc":                      message.Headers["bcc"],
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

func ParticipantsFromMetadata(metadata map[string]any) []Participant {
	if metadata == nil {
		return nil
	}

	participants := []Participant{}
	for _, header := range []struct {
		key  string
		role string
	}{
		{key: "from", role: "from"},
		{key: "sender", role: "sender"},
		{key: "reply_to", role: "reply_to"},
		{key: "to", role: "to"},
		{key: "cc", role: "cc"},
		{key: "bcc", role: "bcc"},
	} {
		participants = append(
			participants,
			participantsFromHeader(header.role, stringMetadata(metadata, header.key))...,
		)
	}

	return participants
}

func participantsFromHeader(role, value string) []Participant {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	addresses, err := mail.ParseAddressList(value)
	if err != nil {
		addresses = parseAddressListSegments(value)
	}

	participants := make([]Participant, 0, len(addresses))
	for index, address := range addresses {
		if strings.TrimSpace(address.Address) == "" {
			continue
		}

		participants = append(participants, Participant{
			Role:        role,
			DisplayName: strings.TrimSpace(address.Name),
			Address:     strings.TrimSpace(address.Address),
			RawValue:    strings.TrimSpace(address.String()),
			Ordinal:     index,
		})
	}

	return participants
}

func parseAddressListSegments(value string) []*mail.Address {
	segments := splitAddressListSegments(value)
	addresses := make([]*mail.Address, 0, len(segments))

	for _, segment := range segments {
		address, err := mail.ParseAddress(segment)
		if err != nil {
			continue
		}

		addresses = append(addresses, address)
	}

	return addresses
}

func splitAddressListSegments(value string) []string {
	segments := []string{}
	state := addressListSplitState{}
	start := 0

	for index, character := range value {
		if state.segmentBoundary(character) {
			segments = appendAddressListSegment(segments, value[start:index])
			start = index + len(string(character))
		}
	}

	return appendAddressListSegment(segments, value[start:])
}

type addressListSplitState struct {
	quoted       bool
	escaped      bool
	commentDepth int
}

func (state *addressListSplitState) segmentBoundary(character rune) bool {
	switch {
	case state.consumeEscaped():
		return false
	case state.startEscape(character):
		return false
	case state.toggleQuote(character):
		return false
	case state.updateComment(character):
		return false
	default:
		return character == ',' && !state.quoted && state.commentDepth == 0
	}
}

func (state *addressListSplitState) consumeEscaped() bool {
	if !state.escaped {
		return false
	}

	state.escaped = false

	return true
}

func (state *addressListSplitState) startEscape(character rune) bool {
	if character != '\\' {
		return false
	}

	state.escaped = true

	return true
}

func (state *addressListSplitState) toggleQuote(character rune) bool {
	if character != '"' || state.commentDepth != 0 {
		return false
	}

	state.quoted = !state.quoted

	return true
}

func (state *addressListSplitState) updateComment(character rune) bool {
	if state.quoted {
		return false
	}

	switch {
	case character == '(':
		state.commentDepth++

		return true
	case character == ')' && state.commentDepth > 0:
		state.commentDepth--

		return true
	default:
		return false
	}
}

func appendAddressListSegment(segments []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return segments
	}

	return append(segments, value)
}

func stringMetadata(metadata map[string]any, key string) string {
	value, ok := metadata[key].(string)
	if !ok {
		return ""
	}

	return value
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
) ([]byte, string, []Attachment, error) {
	textBodies := []string{}
	bodyMediaType := ""

	attachments := []Attachment{}

	err := extractPart(headers, body, &bodyMediaType, &textBodies, &attachments)
	if err != nil {
		return nil, "", nil, err
	}

	return []byte(strings.Join(textBodies, "\n")), bodyMediaType, attachments, nil
}

func extractPart(
	headers mail.Header,
	body io.Reader,
	bodyMediaType *string,
	textBodies *[]string,
	attachments *[]Attachment,
) error {
	contentType := headers.Get("Content-Type")

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return extractLeafPart(
			headers,
			body,
			mediaType,
			bodyMediaType,
			textBodies,
			attachments,
		)
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
			bodyMediaType,
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
	bodyMediaType *string,
	textBodies *[]string,
	attachments *[]Attachment,
) error {
	content, err := readDecodedPart(headers, body)
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

	if mediaType == "text/plain" || mediaType == "" {
		if *bodyMediaType != "text/plain" {
			*textBodies = nil
		}

		*bodyMediaType = "text/plain"
		*textBodies = append(*textBodies, string(content))

		return nil
	}

	if strings.HasPrefix(mediaType, "text/") && *bodyMediaType != "text/plain" {
		if *bodyMediaType == "" {
			*bodyMediaType = mediaType
		}

		if *bodyMediaType == mediaType {
			*textBodies = append(*textBodies, string(content))
		}
	}

	return nil
}

func readDecodedPart(headers mail.Header, body io.Reader) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(headers.Get("Content-Transfer-Encoding"))) {
	case "", "7bit", "8bit", "binary":
		return readAllPart(body, "read unencoded mail part")
	case "base64":
		return readAllPart(
			base64.NewDecoder(base64.StdEncoding, body),
			"decode base64 mail part",
		)
	case "quoted-printable":
		return readAllPart(
			quotedprintable.NewReader(body),
			"decode quoted-printable mail part",
		)
	default:
		return readAllPart(body, "read unknown-encoded mail part")
	}
}

func readAllPart(body io.Reader, context string) ([]byte, error) {
	content, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", context, err)
	}

	return content, nil
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
