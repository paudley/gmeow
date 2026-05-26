// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"

	"blackcat.ca/gmeow/internal/cli"
)

func main() {
	cli.Execute(cli.NewAdminCommand(os.Stdout, os.Stdin))
}
