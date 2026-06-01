// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

const contactPreviewByteLimit = 240

type ContactEmbeddingStore interface {
	ContactAnalysisInputs(
		ctx context.Context,
		request contracts.ContactAnalysisInputRequest,
	) (contracts.ContactAnalysisInputResponse, error)
	ContactAnalysisStatus(
		ctx context.Context,
		request contracts.ContactAnalysisStatusRequest,
	) (contracts.ContactAnalysisStatusResponse, error)
	StoreContactEmbedding(
		ctx context.Context,
		record contracts.ContactEmbeddingUpsert,
	) error
}

func AnalyzeContacts(
	ctx context.Context,
	store ContactEmbeddingStore,
	analyzer *EmbeddingAnalyzer,
	request contracts.ContactAnalysisRequest,
) (contracts.ContactAnalysisResponse, error) {
	inputs, err := store.ContactAnalysisInputs(ctx, contracts.ContactAnalysisInputRequest{
		ContactIDs: request.ContactIDs,
		FactKinds:  request.FactKinds,
		Limit:      request.Limit,
		Offset:     request.Offset,
	})
	if err != nil {
		return contracts.ContactAnalysisResponse{}, err
	}

	results := make([]contracts.ContactAnalysisResult, 0, len(inputs.Results))
	for _, input := range inputs.Results {
		result, err := analyzeContactInput(ctx, store, analyzer, input, request.Forced)
		if err != nil {
			return contracts.ContactAnalysisResponse{}, err
		}
		results = append(results, result)
	}

	response := contracts.ContactAnalysisResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         inputs.Total,
		Limit:         inputs.Limit,
		Offset:        inputs.Offset,
	}
	for _, result := range results {
		if result.Status == "complete" {
			response.Analyzed++
		} else {
			response.Skipped++
		}
	}

	return response, nil
}

func analyzeContactInput(
	ctx context.Context,
	store ContactEmbeddingStore,
	analyzer *EmbeddingAnalyzer,
	input contracts.ContactAnalysisInputResult,
	forced bool,
) (contracts.ContactAnalysisResult, error) {
	inputText := strings.TrimSpace(input.InputText)
	text, truncated := truncateRunes(inputText, embeddingInputRuneLimit)
	inputHash := contactInputHash(text)

	if !forced {
		current, err := currentContactAnalysis(
			ctx,
			store,
			input.ContactID,
			analyzer,
			inputHash,
		)
		if err != nil {
			return contracts.ContactAnalysisResult{}, err
		}
		if current.Status != "" {
			return contactAnalysisResultFromStatus(current), nil
		}
	}

	if inputText == "" {
		record := contactEmbeddingRecord(
			input,
			analyzer,
			inputHash,
			"skipped",
			nil,
			"",
			false,
		)
		if err := store.StoreContactEmbedding(ctx, record); err != nil {
			return contracts.ContactAnalysisResult{}, err
		}

		return contactAnalysisResult(record), nil
	}

	vector, embeddedText, embeddedTruncated, err := analyzer.EmbedText(ctx, inputText)
	if err != nil {
		return contracts.ContactAnalysisResult{}, err
	}
	record := contactEmbeddingRecord(
		input,
		analyzer,
		contactInputHash(embeddedText),
		"complete",
		float64VectorToFloat32(vector),
		embeddedText,
		truncated || embeddedTruncated,
	)
	if err := store.StoreContactEmbedding(ctx, record); err != nil {
		return contracts.ContactAnalysisResult{}, err
	}

	return contactAnalysisResult(record), nil
}

func currentContactAnalysis(
	ctx context.Context,
	store ContactEmbeddingStore,
	contactID string,
	analyzer *EmbeddingAnalyzer,
	inputHash string,
) (contracts.ContactAnalysisStatusResult, error) {
	response, err := store.ContactAnalysisStatus(
		ctx,
		contracts.ContactAnalysisStatusRequest{
			ContactIDs:      []string{contactID},
			InputHashes:     []string{inputHash},
			AnalyzerName:    EmbeddingName,
			AnalyzerVersion: Phase04Version,
			Model:           analyzer.Model(),
			Limit:           1,
		},
	)
	if err != nil {
		return contracts.ContactAnalysisStatusResult{}, err
	}
	if len(response.Results) == 0 {
		return contracts.ContactAnalysisStatusResult{}, nil
	}

	return response.Results[0], nil
}

func contactEmbeddingRecord(
	input contracts.ContactAnalysisInputResult,
	analyzer *EmbeddingAnalyzer,
	inputHash string,
	status string,
	vector []float32,
	text string,
	truncated bool,
) contracts.ContactEmbeddingUpsert {
	return contracts.ContactEmbeddingUpsert{
		GeneratedAt:     time.Now().UTC(),
		ContactID:       input.ContactID,
		AnalyzerName:    EmbeddingName,
		AnalyzerVersion: Phase04Version,
		Status:          status,
		Model:           analyzer.Model(),
		InputHash:       inputHash,
		InputBytes:      len(input.InputText),
		TextPreview:     previewContactInput(text),
		Vector:          vector,
		Metadata: map[string]any{
			"truncated":         truncated,
			"fact_count":        input.FactCount,
			"message_count":     input.MessageCount,
			"participant_count": input.ParticipantCount,
		},
	}
}

func contactAnalysisResult(
	record contracts.ContactEmbeddingUpsert,
) contracts.ContactAnalysisResult {
	return contracts.ContactAnalysisResult{
		ContactID:  record.ContactID,
		Status:     record.Status,
		InputHash:  record.InputHash,
		Model:      record.Model,
		Dimensions: len(record.Vector),
	}
}

func contactAnalysisResultFromStatus(
	status contracts.ContactAnalysisStatusResult,
) contracts.ContactAnalysisResult {
	return contracts.ContactAnalysisResult{
		ContactID: status.ContactID,
		Status:    "current",
		InputHash: status.InputHash,
		Model:     status.Model,
	}
}

func contactInputHash(text string) string {
	sum := sha256.Sum256([]byte(text))

	return hex.EncodeToString(sum[:])
}

func float64VectorToFloat32(values []float64) []float32 {
	converted := make([]float32, len(values))
	for index, value := range values {
		converted[index] = float32(value)
	}

	return converted
}

func previewContactInput(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= contactPreviewByteLimit {
		return text
	}

	limit := 0
	for index := range text {
		if index > contactPreviewByteLimit {
			break
		}
		limit = index
	}
	if limit == 0 {
		return ""
	}

	return text[:limit]
}
