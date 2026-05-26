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
	} {
		if !strings.Contains(output, commandName) {
			t.Fatalf("help output missing %s:\n%s", commandName, output)
		}
	}
}
