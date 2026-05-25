// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package iface

import "context"

type Server interface {
	Start(ctx context.Context) error
}
