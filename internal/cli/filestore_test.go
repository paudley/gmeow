// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestAdminFilestoreVerifyReportsCleanStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "filestore")
	configPath := writePlainCLIConfig(t, root)
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", configPath, "filestore", "verify"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "status=ok") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestAdminFilestoreVerifyResolvesRelativeRootFromConfigPath(t *testing.T) {
	configDir := t.TempDir()
	root := filepath.Join(configDir, "data", "filestore")
	store := filestore.NewFilesystemStore(root)
	if _, err := store.Put(context.Background(), filestore.PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	}); err != nil {
		t.Fatal(err)
	}
	configPath := writePlainCLIConfigAt(t, configDir, "data/filestore")
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	})
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", configPath, "filestore", "verify"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "status=ok checked=1") {
		t.Fatalf("relative root was not resolved from config path: %s", out.String())
	}
}

func TestAdminFilestoreMaintenanceCommandsRunAgainstTempConfig(t *testing.T) {
	configDir := t.TempDir()
	root := filepath.Join(configDir, "data", "filestore")
	store := filestore.NewFilesystemStore(root)
	digest, err := store.Put(context.Background(), filestore.PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := writePlainCLIConfigAt(t, configDir, "data/filestore")

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "cleanup-locks",
			args: []string{"--config", configPath, "filestore", "cleanup-locks"},
			want: "filestore cleanup-locks:",
		},
		{
			name: "compact-dry-run",
			args: []string{"--config", configPath, "filestore", "compact", "--dry-run"},
			want: `"dry_run": true`,
		},
		{
			name: "export-recovery",
			args: []string{
				"--config",
				configPath,
				"filestore",
				"export-recovery",
				"--digest",
				string(digest),
			},
			want: string(digest),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			command := NewAdminCommand(&out, strings.NewReader(""))
			command.SetArgs(tc.args)
			if err := command.Execute(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("unexpected output: %s", out.String())
			}
		})
	}
}

func TestAdminFilestoreStorageReportsDigestAndSourceTargets(t *testing.T) {
	configDir := t.TempDir()
	root := filepath.Join(configDir, "data", "filestore")
	store := filestore.NewFilesystemStore(root)
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	digest, err := store.Put(context.Background(), filestore.PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      ref.SourceKind,
			SourceName:      ref.SourceName,
			ExternalID:      ref.ExternalID,
			ExternalVersion: ref.ExternalVersion,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := writePlainCLIConfigAt(t, configDir, "data/filestore")

	t.Run("digest-human", func(t *testing.T) {
		var out bytes.Buffer
		command := NewAdminCommand(&out, strings.NewReader(""))
		command.SetArgs([]string{
			"--config",
			configPath,
			"filestore",
			"storage",
			"--digest",
			string(digest),
		})
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{
			"filestore storage:",
			"allocated_bytes=",
			"FAMILY",
			"blob",
			"manifest",
		} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("storage output missing %q: %s", want, out.String())
			}
		}
	})

	t.Run("source-json", func(t *testing.T) {
		var out bytes.Buffer
		command := NewAdminCommand(&out, strings.NewReader(""))
		command.SetArgs([]string{
			"--config",
			configPath,
			"filestore",
			"storage",
			"--source-kind",
			ref.SourceKind,
			"--source-name",
			ref.SourceName,
			"--external-id",
			ref.ExternalID,
			"--external-version",
			ref.ExternalVersion,
			"--json",
		})
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
		var report filestore.StorageBreakdownReport
		if err := json.Unmarshal(out.Bytes(), &report); err != nil {
			t.Fatalf("decode storage JSON: %v\n%s", err, out.String())
		}
		if report.RootDigest != digest || report.FileCount == 0 {
			t.Fatalf("unexpected storage report: %#v", report)
		}
	})
}

func TestAdminFilestorePathReportsObjectAndShardPaths(t *testing.T) {
	configDir := t.TempDir()
	root := filepath.Join(configDir, "data", "filestore")
	store := filestore.NewFilesystemStore(root)
	ref := contracts.SourceObjectRef{
		SourceKind:      "gmail",
		SourceName:      "primary",
		ExternalID:      "message-1",
		ExternalVersion: "history-1",
	}
	digest, err := store.Put(context.Background(), filestore.PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind:      ref.SourceKind,
			SourceName:      ref.SourceName,
			ExternalID:      ref.ExternalID,
			ExternalVersion: ref.ExternalVersion,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := writePlainCLIConfigAt(t, configDir, "data/filestore")
	blobPath := filepath.Join(
		root,
		"objects",
		"blake3",
		string(digest)[:2],
		string(digest)[2:4],
		string(digest),
		"blob.zstd",
	)
	t.Run("absolute-human", func(t *testing.T) {
		var out bytes.Buffer
		command := NewAdminCommand(&out, strings.NewReader(""))
		command.SetArgs([]string{
			"--config",
			configPath,
			"filestore",
			"path",
			blobPath,
		})
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"filestore path:", "kind=object", "role=blob", string(digest)} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("path output missing %q: %s", want, out.String())
			}
		}
	})

	t.Run("relative-json", func(t *testing.T) {
		relativeShard := filepath.ToSlash(filepath.Join(
			"source-index-v2",
			sourceObjectRefKeyForTest(ref)[:2],
			sourceObjectRefKeyForTest(ref)[2:4],
			"records.jsonl",
		))
		var out bytes.Buffer
		command := NewAdminCommand(&out, strings.NewReader(""))
		command.SetArgs([]string{
			"--config",
			configPath,
			"filestore",
			"path",
			relativeShard,
			"--json",
			"--records-limit",
			"1",
		})
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
		var report filestore.PathResolveReport
		if err := json.Unmarshal(out.Bytes(), &report); err != nil {
			t.Fatalf("decode path JSON: %v\n%s", err, out.String())
		}
		if report.Kind != "packed_source_index" ||
			report.RecordCount != 1 ||
			len(report.Records) != 1 {
			t.Fatalf("unexpected path JSON report: %#v", report)
		}
	})
}

func TestAdminFilestoreHelpListsMaintenanceCommands(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"filestore", "--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"cleanup-locks", "compact", "export-recovery", "path", "storage"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("filestore help missing %q: %s", want, out.String())
		}
	}
}

func sourceObjectRefKeyForTest(ref contracts.SourceObjectRef) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		ref.SourceKind,
		ref.SourceName,
		ref.ExternalID,
		ref.ExternalVersion,
	}, "\x00")))

	return hex.EncodeToString(sum[:])
}

func writePlainCLIConfig(t *testing.T, filestoreRoot string) string {
	t.Helper()
	return writePlainCLIConfigAt(t, t.TempDir(), filestoreRoot)
}

func writePlainCLIConfigAt(t *testing.T, dir, filestoreRoot string) string {
	t.Helper()
	postgresPassword := localSecretLeaf(t, "postgres_password")
	rabbitPassword := localSecretLeaf(t, "rabbitmq_password")
	testRabbitPassword := localSecretLeaf(t, "rabbitmq_test_password")
	path := filepath.Join(dir, "gmeow.toml")
	body := `
[system]
config_version = 1
instance_id = "test"
data_dir = "data"

[filestore]
root = "` + filestoreRoot + `"

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
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
