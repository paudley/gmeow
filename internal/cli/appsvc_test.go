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
	for _, flag := range []string{"state-dir", "resume", "queue-high-water"} {
		if !strings.Contains(out.String(), flag) {
			t.Fatalf("help output missing %s flag:\n%s", flag, out.String())
		}
	}
}
