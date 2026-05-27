// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/rpc"
)

func rpcEndpoint(endpoint config.ResolvedRPCEndpoint) rpc.Endpoint {
	return rpc.Endpoint{
		Network: endpoint.Network,
		Address: endpoint.Address,
	}
}
