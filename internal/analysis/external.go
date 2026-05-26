// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type ExternalCommandAnalyzer struct {
	spec    contracts.AnalyzerSpec
	command string
	args    []string
	timeout time.Duration
}

type ExternalCommandConfig struct {
	Spec    contracts.AnalyzerSpec
	Command string
	Args    []string
	Timeout time.Duration
}

type ExternalCommandRequest struct {
	Manifest      contracts.Manifest      `json:"manifest"`
	Job           contracts.AnalyzerJob   `json:"job"`
	Text          string                  `json:"text,omitempty"`
	SchemaVersion contracts.SchemaVersion `json:"schema_version"`
}

func NewExternalCommandAnalyzer(
	config ExternalCommandConfig,
) (*ExternalCommandAnalyzer, error) {
	err := ValidateSpec(config.Spec)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(config.Command) == "" {
		return nil, errors.New("external analyzer command is required")
	}

	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Minute
	}

	spec := config.Spec
	if strings.TrimSpace(spec.WorkerKind) == "" {
		spec.WorkerKind = "external"
	}

	return &ExternalCommandAnalyzer{
		spec:    spec,
		command: config.Command,
		args:    append([]string(nil), config.Args...),
		timeout: config.Timeout,
	}, nil
}

func (analyzer *ExternalCommandAnalyzer) Spec() contracts.AnalyzerSpec {
	return analyzer.spec
}

func (analyzer *ExternalCommandAnalyzer) Analyze(
	ctx context.Context,
	store ObjectStore,
	job contracts.AnalyzerJob,
) (contracts.Annotation, error) {
	manifest, err := store.ReadManifest(ctx, job.ObjectDigest)
	if err != nil {
		return contracts.Annotation{}, err
	}

	request := ExternalCommandRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Job:           job,
		Manifest:      manifest,
	}
	if text, ok := readTextInput(ctx, store, job.ObjectDigest, manifest.MediaType); ok {
		request.Text = text
	}

	input, err := json.Marshal(request)
	if err != nil {
		return contracts.Annotation{}, err
	}

	runCtx, cancel := context.WithTimeout(ctx, analyzer.timeout)
	defer cancel()

	command := exec.CommandContext(runCtx, analyzer.command, analyzer.args...)
	command.Stdin = bytes.NewReader(input)

	var stderr bytes.Buffer

	command.Stderr = &stderr

	output, err := command.Output()
	if err != nil {
		return contracts.Annotation{}, fmt.Errorf(
			"run external analyzer %s: %w: %s",
			analyzer.spec.Name,
			err,
			strings.TrimSpace(stderr.String()),
		)
	}

	var annotation contracts.Annotation
	if err := json.Unmarshal(output, &annotation); err != nil {
		return contracts.Annotation{}, fmt.Errorf(
			"decode external analyzer %s annotation: %w",
			analyzer.spec.Name,
			err,
		)
	}

	if annotation.Kind != "" && annotation.Kind != "analysis" {
		return contracts.Annotation{}, fmt.Errorf(
			"external analyzer %s emitted unsupported annotation kind %q",
			analyzer.spec.Name,
			annotation.Kind,
		)
	}

	return annotation, nil
}

func readTextInput(
	ctx context.Context,
	store ObjectStore,
	digest contracts.ObjectDigest,
	mediaType string,
) (string, bool) {
	if !strings.HasPrefix(mediaType, "text/") && mediaType != "application/json" {
		return "", false
	}

	reader, err := store.Open(ctx, digest)
	if err != nil {
		return "", false
	}
	defer reader.Close()

	content, err := io.ReadAll(io.LimitReader(reader, 10*1024*1024))
	if err != nil {
		return "", false
	}

	return extractText(content, mediaType), true
}

var _ Analyzer = (*ExternalCommandAnalyzer)(nil)
