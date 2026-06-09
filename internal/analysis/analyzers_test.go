// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestTextExtractUsesCompoundMailParts(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)

	annotation, err := TextExtractAnalyzer{}.Analyze(
		ctx,
		store,
		analyzerJob(digest, TextExtractAnalyzer{}.Spec()),
	)
	if err != nil {
		t.Fatal(err)
	}

	text, _ := annotation.Data["text"].(string)
	for _, expected := range []string{
		"Subject: Production Pipeline Verification",
		"Pipeline body mentions Athena",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected extracted text to contain %q, got %q", expected, text)
		}
	}
	if strings.Contains(text, "application/vnd.gmeow.gmail-message+json") {
		t.Fatalf("expected text from mail parts, got container content: %q", text)
	}
}

func TestRFC822HeadersUsesCompoundHeaderPart(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)

	annotation, err := RFC822HeaderAnalyzer{}.Analyze(
		ctx,
		store,
		analyzerJob(digest, RFC822HeaderAnalyzer{}.Spec()),
	)
	if err != nil {
		t.Fatal(err)
	}

	if annotation.Data["subject"] != "Production Pipeline Verification" {
		t.Fatalf("expected subject from header part, got %#v", annotation.Data)
	}
	if annotation.Data["from"] != "sender@example.test" {
		t.Fatalf("expected from from header part, got %#v", annotation.Data)
	}
}

func TestRFC822HeadersSkipsNonHeaderObject(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("<p>not headers</p>"),
		MediaType: "text/html",
		Facets:    []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := RFC822HeaderAnalyzer{}.Analyze(
		ctx,
		store,
		analyzerJob(digest, RFC822HeaderAnalyzer{}.Spec()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["status"] != "skipped" ||
		annotation.Data["reason"] != "not_rfc822_headers" {
		t.Fatalf("expected skipped non-header annotation, got %#v", annotation.Data)
	}
}

func TestExternalTextInputUsesCompoundMailParts(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	manifest, err := store.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}

	text, ok := readTextInput(ctx, store, digest, manifest)
	if !ok {
		t.Fatal("expected compound mail object to provide text input")
	}
	if !strings.Contains(text, "Pipeline body mentions Athena") {
		t.Fatalf("expected external analyzer text from body part, got %q", text)
	}
}

func TestEmbeddingUsesCompoundMailBodyOnly(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	service := &recordingEmbeddingService{}

	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}

	if annotation.Data["status"] != "complete" {
		t.Fatalf("expected complete embedding annotation, got %#v", annotation.Data)
	}
	embeddings, ok := annotation.Data["embeddings"].([]any)
	if !ok || len(embeddings) != 1 {
		t.Fatalf("expected one body embedding annotation row, got %#v", annotation.Data)
	}
	if _, exists := annotation.Data["embedding"]; exists {
		t.Fatalf(
			"embedding annotation must use projected embeddings list, got %#v",
			annotation.Data,
		)
	}
	if !containsInput(service.texts, "Pipeline body mentions Athena") {
		t.Fatalf("expected embedding input from body part, got %#v", service.texts)
	}
	if containsInput(service.texts, "application/vnd.gmeow.gmail-message+json") {
		t.Fatalf("expected embedding input to avoid container JSON, got %#v", service.texts)
	}
	if containsInput(service.texts, "gmail:primary:test-message") {
		t.Fatalf("expected embedding input to avoid object ids, got %#v", service.texts)
	}
	if containsInput(service.texts, "Production Pipeline Verification") ||
		containsInput(service.texts, "sender@example.test") {
		t.Fatalf("expected embedding input to avoid headers, got %#v", service.texts)
	}
}

func TestEmbeddingDeHTMLsBodyAndIgnoresAttachments(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	headerDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(`[
			{"name":"From","value":"sender@example.test"},
			{"name":"Subject","value":"Ignored Header"}
		]`),
		MediaType:    "text/rfc822-headers",
		ContentRoles: []string{"rfc822_headers"},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	bodyDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(
			`<html><head><style>.hidden{display:none}</style><script>alert("x")</script></head>` +
				`<body><p>Hello &amp; welcome.</p><div>Visible body text.</div></body></html>`,
		),
		MediaType:    "text/html; charset=utf-8",
		ContentRoles: []string{"email_body"},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	attachmentDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(
			"QmFzZTY0IGF0dGFjaG1lbnQgdGhhdCBtdXN0IG5vdCBiZSBlbWJlZGRlZA==",
		),
		MediaType:    "application/octet-stream",
		ContentRoles: []string{"attachment"},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:     "gmail:primary:html-message",
		MediaType:    "application/vnd.gmeow.gmail-message+json",
		SourceHint:   "HTML message",
		ContentRoles: []string{"source", "mail_message"},
		Facets:       []contracts.Facet{{Kind: "mail_message"}},
		Parts: []contracts.CompoundPart{
			{Digest: headerDigest, Role: "rfc822_headers"},
			{Digest: bodyDigest, Role: "email_body", Order: 1},
			{Digest: attachmentDigest, Role: "attachment", Order: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &recordingEmbeddingService{}
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["status"] != "complete" {
		t.Fatalf("expected complete embedding annotation, got %#v", annotation.Data)
	}
	if len(service.texts) != 1 {
		t.Fatalf("expected one body embedding input, got %#v", service.texts)
	}
	input := service.texts[0]
	for _, expected := range []string{"Hello & welcome.", "Visible body text."} {
		if !strings.Contains(input, expected) {
			t.Fatalf("expected de-HTML body text to contain %q, got %q", expected, input)
		}
	}
	for _, forbidden := range []string{
		"<p>", "display:none", "alert(", "Ignored Header", "sender@example.test", "QmFzZTY0",
	} {
		if strings.Contains(input, forbidden) {
			t.Fatalf("embedding body input contained forbidden %q: %q", forbidden, input)
		}
	}
}

func TestEmbeddingUsesServiceNamespace(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("A message body with enough natural language content."),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &recordingEmbeddingService{}
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:     "test-embed",
		Namespace: "email_segment",
		Service:   service,
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}

	if annotation.Data["status"] != "complete" {
		t.Fatalf("expected complete embedding annotation, got %#v", annotation.Data)
	}
	if len(service.namespaces) != 1 || service.namespaces[0] != "email_segment" {
		t.Fatalf("service namespaces = %#v, want email_segment", service.namespaces)
	}
	if len(service.texts) != 1 ||
		!strings.Contains(service.texts[0], "natural language content") {
		t.Fatalf("service texts = %#v", service.texts)
	}
}

func TestEmbeddingSkipsEmptyText(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(""),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: &recordingEmbeddingService{},
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["status"] != "skipped" ||
		annotation.Data["reason"] != "empty_text" {
		t.Fatalf("expected skipped empty text annotation, got %#v", annotation.Data)
	}
}

func TestEmbeddingTruncatesLongInput(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(strings.Repeat("a", embeddingInputRuneLimit+100)),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &recordingEmbeddingService{}
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if len(service.texts) < 2 {
		t.Fatalf("expected long embedding input to be chunked, got %#v", service.texts)
	}
	for _, input := range service.texts {
		if len([]rune(input)) > embeddingInputRuneLimit {
			t.Fatalf("expected bounded endpoint input, got %d runes", len([]rune(input)))
		}
	}
}

func TestEmbeddingNormalizesInvalidUTF8BeforeServiceCall(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(
			"valid prefix " + string([]byte{0xff, 0xfe}) + " valid suffix",
		),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &recordingEmbeddingService{}
	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := analyzer.Analyze(
		ctx,
		store,
		analyzerJob(digest, analyzer.Spec()),
	); err != nil {
		t.Fatal(err)
	}
	if len(service.texts) != 1 {
		t.Fatalf("service texts = %#v, want one", service.texts)
	}
	if !utf8.ValidString(service.texts[0]) {
		t.Fatalf("service text is not valid UTF-8: %q", service.texts[0])
	}
	if strings.ContainsRune(service.texts[0], utf8.RuneError) {
		t.Fatalf("service text retained replacement runes: %q", service.texts[0])
	}
}

func TestSummaryModelUsesCompoundMailParts(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	var endpointInput string
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			var payload struct {
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			endpointInput = payload.Messages[len(payload.Messages)-1].Content
			_, _ = writer.Write(
				[]byte(
					`{"choices":[{"message":{"content":"{\"summary\":\"The email verifies production pipeline analyzer coverage.\",\"bullets\":[\"Mentions Athena\",\"Requests full coverage\"]}"}}]}`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}

	if annotation.Data["status"] != "complete" {
		t.Fatalf("expected complete summary annotation, got %#v", annotation.Data)
	}
	if !strings.Contains(endpointInput, "Pipeline body mentions Athena") {
		t.Fatalf("expected summary input from body part, got %q", endpointInput)
	}
	if strings.Contains(endpointInput, "application/vnd.gmeow.gmail-message+json") {
		t.Fatalf("expected summary input to avoid container JSON, got %q", endpointInput)
	}
}

func TestSummarySkipsEmptyText(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(""),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: "http://127.0.0.1:1/v1/chat/completions",
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["status"] != "skipped" ||
		annotation.Data["reason"] != "empty_text" {
		t.Fatalf("expected skipped empty text annotation, got %#v", annotation.Data)
	}
}

func TestSummaryTruncatesLongInput(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader(strings.Repeat("a", summaryInputRuneLimit+100)),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var endpointInput string
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			var payload struct {
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			endpointInput = payload.Messages[len(payload.Messages)-1].Content
			_, _ = writer.Write([]byte(
				`{"choices":[{"message":{"content":"{\"summary\":\"short\",\"bullets\":[\"one\"]}"}}]}`,
			))
		}),
	)
	t.Cleanup(server.Close)
	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(endpointInput, strings.Repeat("a", summaryInputRuneLimit)) ||
		strings.Contains(endpointInput, strings.Repeat("a", summaryInputRuneLimit+1)) {
		t.Fatalf("summary endpoint input was not truncated to expected size")
	}
	if annotation.Data["truncated"] != true {
		t.Fatalf("expected truncated annotation flag, got %#v", annotation.Data)
	}
}

func TestSummaryModelRejectsNonJSONEcho(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write(
				[]byte(
					`{"choices":[{"message":{"content":"Subject: Flight booking confirmed.\nBody: Your flight to Edmonton departs Monday."}}]}`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "bad-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err == nil {
		t.Fatal("expected non-json echo response to fail closed")
	}
}

func TestSummaryModelAcceptsFencedJSON(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write(
				[]byte(
					"{\"choices\":[{\"message\":{\"content\":\"```json\\n{\\\"summary\\\":\\\"The email verifies analyzer coverage.\\\",\\\"bullets\\\":[\\\"Coverage requested\\\"]}\\n```\"}}]}",
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["summary"] != "The email verifies analyzer coverage." {
		t.Fatalf("expected fenced JSON summary to parse, got %#v", annotation.Data)
	}
}

func TestSummaryModelAcceptsJSONAfterModelPreamble(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write(
				[]byte(
					`{"choices":[{"message":{"content":"(drafting a concise operator summary)\n{\"summary\":\"The email verifies analyzer coverage.\",\"bullets\":[\"Coverage requested\"]}"}}]}`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	annotation, err := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
	if err != nil {
		t.Fatal(err)
	}
	if annotation.Data["summary"] != "The email verifies analyzer coverage." {
		t.Fatalf("expected prefaced JSON summary to parse, got %#v", annotation.Data)
	}
}

func TestSummaryModelSerializesEndpointCalls(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	var active int32
	var maxActive int32
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			for {
				observed := atomic.LoadInt32(&maxActive)
				if current <= observed ||
					atomic.CompareAndSwapInt32(&maxActive, observed, current) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
			_, _ = writer.Write(
				[]byte(
					`{"choices":[{"message":{"content":"{\"summary\":\"The email verifies analyzer coverage.\",\"bullets\":[\"Coverage requested\"]}"}}]}`,
				),
			)
		}),
	)
	t.Cleanup(server.Close)

	analyzer, err := NewSummaryAnalyzer(SummaryConfig{
		Endpoint: server.URL,
		Model:    "test-summary",
	})
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, analyzeErr := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
			errs <- analyzeErr
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if maxActive != 1 {
		t.Fatalf("expected summary endpoint calls to be serialized, max active=%d", maxActive)
	}
}

func TestEmbeddingSerializesServiceCalls(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest := putCompoundMailObject(t, ctx, store)
	service := &recordingEmbeddingService{delay: 25 * time.Millisecond}

	analyzer, err := NewEmbeddingAnalyzer(EmbeddingConfig{
		Model:   "test-embed",
		Service: service,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, analyzeErr := analyzer.Analyze(ctx, store, analyzerJob(digest, analyzer.Spec()))
			errs <- analyzeErr
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if service.maxActive.Load() != 1 {
		t.Fatalf(
			"expected embedding service calls to be serialized, max active=%d",
			service.maxActive.Load(),
		)
	}
}

func putCompoundMailObject(
	t *testing.T,
	ctx context.Context,
	store filestore.Store,
) contracts.ObjectDigest {
	t.Helper()

	headerDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(`[
			{"name":"From","value":"sender@example.test"},
			{"name":"To","value":"recipient@example.test"},
			{"name":"Subject","value":"Production Pipeline Verification"},
			{"name":"Message-Id","value":"<pipeline@example.test>"}
		]`),
		MediaType:    "text/rfc822-headers",
		ContentRoles: []string{"rfc822_headers"},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	bodyDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader: strings.NewReader(
			"Pipeline body mentions Athena and full analyzer coverage.",
		),
		MediaType:    "text/plain",
		ContentRoles: []string{"email_body"},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	parent, err := store.PutCompound(ctx, filestore.CompoundPutRequest{
		ObjectID:     "gmail:primary:test-message",
		MediaType:    "application/vnd.gmeow.gmail-message+json",
		SourceHint:   "Production Pipeline Verification",
		ContentRoles: []string{"source", "mail_message"},
		Facets: []contracts.Facet{{
			Kind: "mail_message",
			Metadata: map[string]any{
				"message_id": "test-message",
				"subject":    "Production Pipeline Verification",
			},
		}},
		Parts: []contracts.CompoundPart{
			{Digest: headerDigest, Role: "rfc822_headers"},
			{Digest: bodyDigest, Role: "email_body", Order: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return parent
}

func containsInput(inputs []string, needle string) bool {
	for _, input := range inputs {
		if strings.Contains(input, needle) {
			return true
		}
	}

	return false
}

type recordingEmbeddingService struct {
	namespaces []string
	texts      []string
	delay      time.Duration
	active     atomic.Int32
	maxActive  atomic.Int32
}

func (service *recordingEmbeddingService) EmbedNamespace(
	_ context.Context,
	namespace string,
	texts []string,
) ([][]float32, int, error) {
	current := service.active.Add(1)
	defer service.active.Add(-1)
	for {
		observed := service.maxActive.Load()
		if current <= observed || service.maxActive.CompareAndSwap(observed, current) {
			break
		}
	}
	if service.delay > 0 {
		time.Sleep(service.delay)
	}
	service.namespaces = append(service.namespaces, namespace)
	service.texts = append(service.texts, texts...)

	return [][]float32{{0.1, 0.2, 0.3}}, 1, nil
}
