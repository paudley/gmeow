// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"path/filepath"
	"testing"
)

func TestSelectedPathPrecedence(t *testing.T) {
	if got := selectedPath("/explicit/gmeow.toml"); got != "/explicit/gmeow.toml" {
		t.Fatalf("explicit path should win, got %q", got)
	}

	t.Setenv(configEnvName, "/env/gmeow.toml")
	if got := selectedPath(""); got != "/env/gmeow.toml" {
		t.Fatalf("GMEOW_CONFIG should win over the search, got %q", got)
	}
}

func TestDefaultConfigSearchPathsOrder(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg-config")

	paths := defaultConfigSearchPaths()
	want := []string{
		systemConfigPath,
		filepath.Join("/xdg-config", "gmeow", "gmeow.toml"),
		defaultConfigPath,
	}
	if len(paths) != len(want) {
		t.Fatalf("expected %d search paths, got %v", len(want), paths)
	}
	for i, expected := range want {
		if paths[i] != expected {
			t.Fatalf("search path %d = %q, want %q", i, paths[i], expected)
		}
	}
}
