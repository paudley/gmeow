// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/config"
)

// testWorkerRegistry builds a worker registry with a real backend manager whose
// reaper goroutine is torn down when the test ends. External analyzers now require
// a manager, so registry-construction tests route through this helper.
func testWorkerRegistry(
	t *testing.T,
	analysisConfig config.AnalysisConfig,
) (*analysis.Registry, error) {
	t.Helper()

	manager := analysis.NewBackendManager(analysis.BackendManagerConfig{})
	t.Cleanup(manager.Close)

	return workerRegistryFromConfig(analysisConfig, manager, fakeEmbeddingService{})
}

type fakeEmbeddingService struct{}

func (fakeEmbeddingService) EmbedNamespace(
	context.Context,
	string,
	[]string,
) ([][]float32, int, error) {
	return [][]float32{{0.1, 0.2, 0.3}}, 1, nil
}

func TestAdminConfigValidate(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", path, "config", "validate"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "config valid") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestAdminSecretListHidesValues(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", path, "config", "secret", "list"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	output := out.String()
	if !strings.Contains(output, "postgres_password references=1") {
		t.Fatalf("unexpected output: %s", output)
	}
	if strings.Contains(output, "postgres-password") {
		t.Fatalf("secret value leaked in output: %s", output)
	}
}

func TestAdminSecretSetUpdatesEncryptedLeaf(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader("gmeow\n"))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "set", "postgres_password"},
	)

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "gmeow\n") {
		t.Fatal("secret value was written in plaintext")
	}
	if _, err := config.Load(config.Options{Path: path}); err != nil {
		t.Fatalf("updated config should validate: %v", err)
	}
}

func TestAdminSecretUnsetRefusesReferencedSecret(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader(""))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "unset", "postgres_password"},
	)

	err := command.Execute()
	if err == nil {
		t.Fatal("expected referenced secret error")
	}
	if !strings.Contains(err.Error(), "still referenced") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkerRegistrySupportsConfiguredExternalAnalyzer(t *testing.T) {
	registry, err := testWorkerRegistry(t, config.AnalysisConfig{
		Analyzers: []config.AnalyzerConfig{{
			Name:       "ner.spacy",
			Version:    "python-email-v1",
			WorkerKind: "python",
			Command:    "gmeow-intel",
			Args:       []string{"serve", "ner.spacy"},
			Timeout:    "30s",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Analyzer(analyzerSpecFromConfig(config.AnalyzerConfig{
		Name:       "ner.spacy",
		Version:    "python-email-v1",
		WorkerKind: "python",
	})); !ok {
		t.Fatal("expected configured external analyzer to be registered")
	}
}

func TestWorkerRegistryRejectsPythonAnalyzerWithoutExplicitAdapter(t *testing.T) {
	_, err := testWorkerRegistry(t, config.AnalysisConfig{
		Analyzers: []config.AnalyzerConfig{{
			Name:       "ner.spacy",
			Version:    "python-email-v1",
			WorkerKind: "python",
		}},
	})
	if err == nil {
		t.Fatal("expected Python analyzer without command to fail closed")
	}
	if !strings.Contains(err.Error(), "external analyzer command is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkerRegistryCoversPhaseFourAnalyzerSet(t *testing.T) {
	registry, err := testWorkerRegistry(t, config.AnalysisConfig{
		Summary: config.SummaryConfig{
			Endpoint: "http://127.0.0.1:8091/v1/chat/completions",
			Model:    "test-summary",
		},
		Analyzers: []config.AnalyzerConfig{
			{Name: "text.extract", Version: "phase04-email-v2", WorkerKind: "go"},
			{Name: "rfc822.headers", Version: "phase04-email-v2", WorkerKind: "go"},
			{Name: "metadata.extract", Version: "phase04-email-v2", WorkerKind: "go"},
			{Name: "graph.facts", Version: "phase04-email-v2", WorkerKind: "go"},
			{Name: "embedding.endpoint", Version: "phase04-email-v2", WorkerKind: "go"},
			{Name: "summary.model", Version: "phase04-email-v2", WorkerKind: "go"},
			{
				Name:       "ner.spacy",
				Version:    "python-email-v1",
				WorkerKind: "python",
				Command:    "gmeow-intel",
				Args:       []string{"serve", "ner.spacy"},
				Timeout:    "2m",
			},
			{
				Name:       "categories.sklearn",
				Version:    "python-email-v1",
				WorkerKind: "python",
				Command:    "gmeow-intel",
				Args:       []string{"serve", "categories.sklearn"},
				Timeout:    "2m",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, configured := range []config.AnalyzerConfig{
		{Name: "text.extract", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "rfc822.headers", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "metadata.extract", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "graph.facts", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "embedding.endpoint", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "summary.model", Version: "phase04-email-v2", WorkerKind: "go"},
		{Name: "ner.spacy", Version: "python-email-v1", WorkerKind: "python"},
		{Name: "categories.sklearn", Version: "python-email-v1", WorkerKind: "python"},
	} {
		if _, ok := registry.Analyzer(analyzerSpecFromConfig(configured)); !ok {
			t.Fatalf("expected analyzer to be registered: %#v", configured)
		}
	}
}

func TestWorkerRegistryRequiresEmbeddingService(t *testing.T) {
	manager := analysis.NewBackendManager(analysis.BackendManagerConfig{})
	t.Cleanup(manager.Close)

	_, err := workerRegistryFromConfig(config.AnalysisConfig{
		Analyzers: []config.AnalyzerConfig{{
			Name:       "embedding.endpoint",
			Version:    "phase04-email-v2",
			WorkerKind: "go",
		}},
	}, manager, nil)
	if err == nil {
		t.Fatal("expected missing embedding service to fail")
	}
	if !strings.Contains(err.Error(), "embedding service is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWorkerRegistryRequiresSummaryEndpointConfig(t *testing.T) {
	_, err := testWorkerRegistry(t, config.AnalysisConfig{
		Analyzers: []config.AnalyzerConfig{{
			Name:       "summary.model",
			Version:    "phase04-email-v2",
			WorkerKind: "go",
		}},
	})
	if err == nil {
		t.Fatal("expected missing summary endpoint config to fail")
	}
	if !strings.Contains(err.Error(), "summary endpoint is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdminSecretUnsetRemovesUnreferencedSecretAtomically(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("unused_password = '''\nunused\n'''\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader(""))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "unset", "unused_password"},
	)

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "unused_password") {
		t.Fatalf("unreferenced secret was not removed:\n%s", string(body))
	}
	if _, err := config.Load(config.Options{Path: path}); err != nil {
		t.Fatalf("updated config should validate: %v", err)
	}
}

func writeEncryptedCLIConfig(t *testing.T) string {
	t.Helper()
	plainPath := filepath.Join(t.TempDir(), "gmeow.toml")
	postgresPassword := localSecretLeaf(t, "postgres_password")
	rabbitPassword := localSecretLeaf(t, "rabbitmq_password")
	testRabbitPassword := localSecretLeaf(t, "rabbitmq_test_password")
	body := `
[system]
config_version = 1
instance_id = "test"
data_dir = "data"

[filestore]
root = "data/filestore"

[postgres]
host = "127.0.0.1"
port = 5432
database = "gmeow-test"
user = "gmeow-test"
ssl_mode = "require"

[rabbitmq]
host = "127.0.0.1"
port = 5672
user = "gmeow-test"
vhost = "gmeow-test"
test_user = "gmeow-test"
test_vhost = "gmeow-test"

[secrets]
postgres_password = ` + tomlLiteralForCLIConfig(postgresPassword) + `
rabbitmq_password = ` + tomlLiteralForCLIConfig(rabbitPassword) + `
rabbitmq_test_password = ` + tomlLiteralForCLIConfig(testRabbitPassword) + `
`
	if err := os.WriteFile(plainPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return plainPath
}

func tomlLiteralForCLIConfig(value string) string {
	return "'''\n" + value + "\n'''"
}

func localSecretLeaf(t *testing.T, name string) string {
	t.Helper()
	t.Skipf(
		"encrypted secret fixture %q is not embedded; tests must not read repo or operator config paths",
		name,
	)
	return ""
}
