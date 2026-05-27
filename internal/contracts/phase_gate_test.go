// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoImportsUseDeclaredModulePath(t *testing.T) {
	root := repoRoot(t)
	misspelled := "black" + "at.ca/gmeow"
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "coding-ethos":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), misspelled) {
			t.Fatalf("%s imports misspelled module path %s", path, misspelled)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFilestoreDoesNotImportQueryOrPostgres(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "filestore")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		for _, forbidden := range []string{
			"internal/query",
			"jackc/pgx",
			"database/sql",
			"pressly/goose",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s imports forbidden dependency %q", path, forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnalysisRuntimeDoesNotImportConcreteFilestore(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "analysis")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "internal/filestore") {
			t.Fatalf(
				"%s imports concrete FILESTORE; ANALYSIS must use FilestoreService gRPC",
				path,
			)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSourceRuntimeUsesRPCFilestoreBoundary(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "source")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		if strings.Contains(text, "internal/filestore") {
			t.Fatalf(
				"%s imports concrete FILESTORE; SOURCE must use FilestoreService gRPC",
				path,
			)
		}
		for _, required := range []string{
			"LookupSourceObject",
			"TryAcquireSourceIngest",
		} {
			if path == filepath.Join(root, "interfaces.go") &&
				!strings.Contains(text, required) {
				t.Fatalf("SOURCE service contract is missing %s", required)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceOwnershipBoundariesStayEnforced(t *testing.T) {
	root := repoRoot(t)
	module := "blackcat.ca/gmeow/"
	rules := []struct {
		name      string
		dir       string
		forbidden []string
	}{
		{
			name: "ANALYSIS only talks to FILESTORE through its ObjectStore contract",
			dir:  "internal/analysis",
			forbidden: []string{
				"github.com/rabbitmq/amqp091-go",
				module + "internal/scheduler/rabbitmq",
				module + "internal/query",
				module + "internal/interface",
				module + "internal/source",
				"google.golang.org/api/gmail",
				"github.com/jackc/pgx",
				"database/sql",
			},
		},
		{
			name: "INTERFACE talks through app services instead of owning storage or queue transports",
			dir:  "internal/interface",
			forbidden: []string{
				"github.com/rabbitmq/amqp091-go",
				module + "internal/scheduler/rabbitmq",
				module + "internal/filestore",
				module + "internal/query/postgres",
				"github.com/jackc/pgx",
				"database/sql",
			},
		},
		{
			name: "QUERY owns PostgreSQL and reads FILESTORE, but never talks to backends or RabbitMQ",
			dir:  "internal/query",
			forbidden: []string{
				"github.com/rabbitmq/amqp091-go",
				module + "internal/scheduler",
				module + "internal/interface",
				module + "internal/source",
				"google.golang.org/api/gmail",
			},
		},
		{
			name: "BACKENDS talk only to their backend APIs and FILESTORE-facing contracts",
			dir:  "internal/source",
			forbidden: []string{
				"github.com/rabbitmq/amqp091-go",
				module + "internal/scheduler",
				module + "internal/query",
				module + "internal/interface",
				"github.com/jackc/pgx",
				"database/sql",
			},
		},
		{
			name: "RabbitMQ is owned by SCHEDULER",
			dir:  "internal",
			forbidden: []string{
				"github.com/rabbitmq/amqp091-go",
			},
		},
		{
			name: "PostgreSQL is owned by QUERY",
			dir:  "internal",
			forbidden: []string{
				"github.com/jackc/pgx",
				"database/sql",
				"pressly/goose",
			},
		},
	}

	for _, rule := range rules {
		t.Run(rule.name, func(t *testing.T) {
			err := filepath.WalkDir(filepath.Join(root, rule.dir), func(
				path string,
				entry os.DirEntry,
				err error,
			) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					if shouldSkipArchitectureDir(root, path) {
						return filepath.SkipDir
					}
					return nil
				}
				if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				if architectureRuleAllows(path, rule.forbidden) {
					return nil
				}
				content, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				text := string(content)
				for _, forbidden := range rule.forbidden {
					if strings.Contains(text, forbidden) {
						t.Fatalf("%s violates %s with dependency %q", path, rule.name, forbidden)
					}
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPhaseZeroToThreePythonRetiredPathsStayRetired(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow/config.py",
		"src/gmeow/object_store.py",
		"src/gmeow/db.py",
		"src/gmeow/pg_cache.py",
		"src/gmeow/pg_cache_archive.py",
		"src/gmeow/pg_cache_helpers.py",
		"src/gmeow/pg_cache_imap.py",
		"src/gmeow/pg_cache_jobs.py",
		"src/gmeow/semantic_pg.py",
		"src/gmeow/maintenance.py",
		"tests/_test_config.py",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired phase 0-3 Python path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestPhaseFourToFivePythonRetiredPathsStayRetired(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow/attachment_analysis.py",
		"src/gmeow/categories.py",
		"src/gmeow/semantic.py",
		"src/gmeow/text_index.py",
		"src/gmeow/headers.py",
		"src/gmeow/gmail.py",
		"src/gmeow/sync.py",
		"src/gmeow/sync_backfill.py",
		"src/gmeow/gmail_actions.py",
		"src/gmeow/provision.py",
		"tests/test_gmail_actions.py",
		"tests/test_sync_backfill.py",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired phase 4-5 Python path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestPhaseSixPythonPresentationHelpersStayRetired(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow/http_json.py",
		"src/gmeow/toon.py",
		"src/gmeow/markdown.py",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired phase 6 Python presentation helper is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestPhaseSevenOldPythonApplicationScaffoldingIsRemoved(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow",
		"main.py",
		"tests",
		"typings",
		"uv.lock",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired old Python application path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestPhaseSixInterfacesDependOnAppServices(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "interface")
	forbidden := []string{
		"internal/filestore",
		"internal/query/postgres",
		"internal/query/memory",
		"internal/scheduler/rabbitmq",
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		if !strings.Contains(text, "internal/appsvc") &&
			!strings.HasSuffix(path, "interfaces.go") {
			t.Fatalf("%s does not import appsvc", path)
		}
		for _, dependency := range forbidden {
			if strings.Contains(text, dependency) {
				t.Fatalf("%s imports forbidden concrete implementation %q", path, dependency)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTransitionalPythonSyncDoesNotOwnAnalysisScheduling(t *testing.T) {
	path := filepath.Join(repoRoot(t), "src", "gmeow", "sync.py")
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"_enqueue_missing_message_analysis",
		"maintenance_scheduler",
	} {
		if strings.Contains(string(content), forbidden) {
			t.Fatalf("transitional Python sync still contains scheduler path %q", forbidden)
		}
	}
}

func TestDocsDoNotReintroduceGmailRawPayloadDuplication(t *testing.T) {
	for _, relative := range []string{
		"docs/GO_MIGRATION.md",
		"docs/GO_PHASE_05_SOURCE.md",
	} {
		content, err := os.ReadFile(filepath.Join(repoRoot(t), relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{
			"raw_rfc822",
			"body/raw/attachment",
		} {
			if strings.Contains(string(content), forbidden) {
				t.Fatalf(
					"%s reintroduced duplicate Gmail raw payload wording %q",
					relative,
					forbidden,
				)
			}
		}
	}
}

func TestDocsDoNotPrescribeFakingInRepoPhaseSixCode(t *testing.T) {
	root := repoRoot(t)
	for _, relative := range []string{
		"README.md",
		"docs/GO_MIGRATION.md",
		"docs/GO_PHASE_02_QUERY.md",
		"docs/GO_PHASE_06_INTERFACE.md",
	} {
		content, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, stale := range []string{
			"fake QUERY",
			"fake Gmail",
			"fake FILESTORE",
		} {
			if strings.Contains(string(content), stale) {
				t.Fatalf("%s still prescribes faking in-repo code with %q", relative, stale)
			}
		}
	}
}

func TestPublicDocsDescribePhaseZeroThroughSixRuntime(t *testing.T) {
	root := repoRoot(t)
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(readme)
	for _, required := range []string{
		"Phases 0-6",
		"Go ANALYSIS worker runtime",
		"Go SOURCE adapters",
		"Gmail SOURCE adapter",
		"gmeow mcp-serve",
		"gmeow rest-serve",
		"read-only `gmeow imap-serve`",
		"gmeow-intel",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf(
				"README.md does not describe completed phase 0-6 runtime surface %q",
				required,
			)
		}
	}
	for _, stale := range []string{
		"Phases 0-3 provide",
		"MCP returns in the Go INTERFACE phase",
		"Archive and IMAP behavior return in the Go INTERFACE phase",
		"Gmail SOURCE adapters, and ANALYSIS workers return in later Go migration phases",
		"Gmail provisioning and source runtime behavior move in later Go migration phases",
	} {
		if strings.Contains(text, stale) {
			t.Fatalf("README.md still contains stale phase wording %q", stale)
		}
	}
}

func TestPythonIntelDocsDescribeExternalAnalyzerAdapters(t *testing.T) {
	root := repoRoot(t)
	for _, relative := range []string{
		"python/gmeow_intel/__init__.py",
		"python/gmeow_intel/analyzers/MODULE.md",
		"python/gmeow_intel/analyzers/__init__.py",
		"python/tests/MODULE.md",
		"python/tests/__init__.py",
		"python/tests/test_contracts.py",
		"docs/SOURCE_DOCS.md",
	} {
		content, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, stale := range []string{
			"Phase 00 keeps",
			"Phase 00 modules are placeholders",
			"Phase 00 `gmeow-intel` package skeleton",
			"Phase 00 only defines contracts",
			"later ANALYSIS work",
		} {
			if strings.Contains(string(content), stale) {
				t.Fatalf("%s still contains stale external analyzer wording %q", relative, stale)
			}
		}
	}
}

func TestRuntimeAnalyzersDoNotContainFallbackOrPlaceholderOutputs(t *testing.T) {
	root := repoRoot(t)
	paths := []string{
		filepath.Join(root, "internal", "analysis"),
		filepath.Join(root, "python", "gmeow_intel"),
	}
	for _, scanRoot := range paths {
		err := filepath.WalkDir(
			scanRoot,
			func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				switch filepath.Ext(path) {
				case ".go", ".py":
				default:
					return nil
				}
				content, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				text := string(content)
				for _, forbidden := range []string{
					"regex_fallback",
					"keyword_fallback",
					`"placeholder"`,
					"status = \"placeholder\"",
				} {
					if strings.Contains(text, forbidden) {
						t.Fatalf("%s contains forbidden runtime analyzer marker %q", path, forbidden)
					}
				}
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPhaseSevenOperationalDocsAndMetricsStayAligned(t *testing.T) {
	root := repoRoot(t)
	backupDoc, err := os.ReadFile(
		filepath.Join(root, "docs", "FILESTORE_BACKUP_RESTORE.md"),
	)
	if err != nil {
		t.Fatal(err)
	}
	backupText := string(backupDoc)
	for _, required := range []string{
		"temporary target",
		"filestore verify",
		"query migrate",
		"query rebuild --confirm-instance <instance_id>",
		"scheduler scan",
		"search <synthetic-term>",
		"Do not use `recovery.json` sidecars for normal rebuilds",
	} {
		if !strings.Contains(backupText, required) {
			t.Fatalf("FILESTORE backup/restore docs are missing phase-7 step %q", required)
		}
	}

	metricSources := []string{
		"internal/analysis/worker.go",
		"internal/appsvc/services.go",
		"internal/filestore/verify.go",
		"internal/interface/rest/server.go",
		"internal/query/postgres/index.go",
	}
	metricText := strings.Builder{}
	for _, relative := range metricSources {
		content, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		metricText.Write(content)
	}
	for _, metric := range []string{
		"gmeow_queue_depth_pending",
		"gmeow_queue_depth_retry",
		"gmeow_analysis_latency",
		"gmeow_projection_lag",
		"gmeow_corrupt_objects",
		"gmeow_failed_analyzers",
		"gmeow_source_errors",
		"gmeow_interface_latency",
	} {
		if !strings.Contains(metricText.String(), metric) {
			t.Fatalf("phase-7 metric %q is not instrumented", metric)
		}
	}
}

func TestTestingArchitectureForbidsInternalDoublesAndConcreteSeamBypasses(
	t *testing.T,
) {
	root := repoRoot(t)
	internalRoot := filepath.Join(root, "internal")
	self := filepath.Join(internalRoot, "contracts", "phase_gate_test.go")
	forbiddenTestSnippets := []string{
		"NewMemoryBroker",
		"MemoryBroker",
		"memoryIndex",
		"internal/query/memory",
		"internal/query/querytest",
		"internal/scheduler/schedulertest",
		"mock",
		"Mock",
		"fake",
		"Fake",
		"stub",
		"Stub",
	}
	err := filepath.WalkDir(
		internalRoot,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" ||
				!strings.HasSuffix(path, "_test.go") || path == self {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(content)
			for _, forbidden := range forbiddenTestSnippets {
				if strings.Contains(text, forbidden) {
					t.Fatalf(
						"%s contains forbidden in-repo test double marker %q; only fully external APIs/services may be replaced",
						path,
						forbidden,
					)
				}
			}
			if strings.Contains(text, "InMemory") &&
				!strings.Contains(text, "mcp.NewInMemoryTransports") {
				t.Fatalf("%s contains forbidden in-memory test transport/backend", path)
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, relative := range []string{
		filepath.Join("internal", "query", "memory"),
		filepath.Join("internal", "query", "querytest"),
		filepath.Join("internal", "scheduler", "schedulertest"),
	} {
		path := filepath.Join(root, relative)
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("forbidden internal testing/backend path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat forbidden testing/backend path %s: %v", relative, err)
		}
	}

	for _, relativeRoot := range []string{
		filepath.Join("internal", "appsvc"),
		filepath.Join("internal", "contracts"),
		filepath.Join("internal", "interface"),
		filepath.Join("internal", "source"),
	} {
		err := filepath.WalkDir(filepath.Join(root, relativeRoot), func(
			path string,
			entry os.DirEntry,
			err error,
		) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(content)
			for _, forbidden := range []string{
				"\"blackcat.ca/gmeow/internal/filestore\"",
				"\"blackcat.ca/gmeow/internal/query/postgres\"",
				"\"blackcat.ca/gmeow/internal/scheduler/rabbitmq\"",
			} {
				if strings.Contains(text, forbidden) {
					t.Fatalf(
						"%s imports concrete component implementation %s; major component seams must use gRPC",
						path,
						forbidden,
					)
				}
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func shouldSkipArchitectureDir(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	switch relative {
	case filepath.Join("internal", "testsupport"):
		return true
	default:
		return false
	}
}

func architectureRuleAllows(path string, forbidden []string) bool {
	text := filepath.ToSlash(path)
	for _, dependency := range forbidden {
		switch {
		case dependency == "github.com/rabbitmq/amqp091-go":
			return strings.Contains(text, "/internal/scheduler/rabbitmq/") ||
				strings.Contains(text, "/internal/config/")
		case dependency == "github.com/jackc/pgx",
			dependency == "database/sql",
			dependency == "pressly/goose":
			return strings.Contains(text, "/internal/query/postgres/") ||
				strings.Contains(text, "/internal/config/")
		default:
		}
	}

	return false
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
