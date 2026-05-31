// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProductionRabbitMQConsumersDoNotPoll(t *testing.T) {
	t.Parallel()

	forbidden := []string{
		".Get(",
		".ConsumeWithContext(",
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		content, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatal(readErr)
		}

		source := string(content)
		for _, pattern := range forbidden {
			if strings.Contains(source, pattern) {
				t.Fatalf(
					"%s contains RabbitMQ polling consumer pattern %q; use a blocking Consumer stream instead",
					file,
					pattern,
				)
			}
		}
	}
}
