// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestServiceCommandIncludesAppServiceWorkflows(t *testing.T) {
	var out bytes.Buffer
	command := NewServiceCommand("gmeow", "Gmeow service", &out)
	command.SetOut(&out)
	command.SetArgs([]string{"--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := out.String()
	for _, commandName := range []string{
		"search",
		"mail-search",
		"retrieve",
		"ops-status",
		"force-analysis",
		"jmap-serve",
	} {
		if !strings.Contains(output, commandName) {
			t.Fatalf("help output missing %s:\n%s", commandName, output)
		}
	}
}

func TestAdminCommandIncludesSourceBackfill(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"source", "--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, commandName := range []string{"backfill", "import"} {
		if !strings.Contains(out.String(), commandName) {
			t.Fatalf("help output missing source %s:\n%s", commandName, out.String())
		}
	}
}

func TestAdminCommandIncludesMailMissingGmailReport(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"query", "--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "mail-missing-gmail") {
		t.Fatalf("help output missing mail missing Gmail report:\n%s", out.String())
	}
}

func TestAdminCommandIncludesContactQuerySurface(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"query", "contact", "--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := out.String()
	for _, commandName := range []string{
		"search",
		"aggregate",
		"resolve",
		"identities",
		"facts",
		"neighborhood",
		"messages",
		"similar",
		"vector-search",
		"analysis-inputs",
		"analysis-status",
		"analyze",
		"import",
		"export",
	} {
		if !strings.Contains(output, commandName) {
			t.Fatalf("help output missing contact %s:\n%s", commandName, output)
		}
	}
}

func TestParseFloat32CSV(t *testing.T) {
	vector, err := parseFloat32CSV("1, 2.5,-3")
	if err != nil {
		t.Fatal(err)
	}

	expected := []float32{1, 2.5, -3}
	if len(vector) != len(expected) {
		t.Fatalf("unexpected vector length: %#v", vector)
	}
	for index := range expected {
		if vector[index] != expected[index] {
			t.Fatalf("unexpected vector at %d: %#v", index, vector)
		}
	}
}

func TestParseFloat32CSVRejectsEmptyAndMalformedValues(t *testing.T) {
	for _, raw := range []string{"", " ", "1,nope"} {
		if _, err := parseFloat32CSV(raw); err == nil {
			t.Fatalf("expected %q to fail", raw)
		}
	}
}

func TestContactVectorSearchRequiresVectorFlag(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"query", "contact", "vector-search"})

	err := command.Execute()
	if err == nil {
		t.Fatal("expected missing vector flag to fail")
	}
	if !strings.Contains(err.Error(), `required flag(s) "vector" not set`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSourceImportCommandIncludesLowNoise(t *testing.T) {
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetOut(&out)
	command.SetArgs([]string{"source", "import", "--help"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "low-noise") {
		t.Fatalf("help output missing low-noise flag:\n%s", out.String())
	}
	for _, flag := range []string{"state-dir", "concurrency"} {
		if !strings.Contains(out.String(), flag) {
			t.Fatalf("help output missing %s flag:\n%s", flag, out.String())
		}
	}
	for _, removed := range []string{"resume", "queue-high-water"} {
		if strings.Contains(out.String(), removed) {
			t.Fatalf(
				"help output still exposes queued import flag %s:\n%s",
				removed,
				out.String(),
			)
		}
	}
}
