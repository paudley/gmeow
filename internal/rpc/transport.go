// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
)

type Endpoint struct {
	Network string
	Address string
}

func Serve(
	ctx context.Context,
	endpoint Endpoint,
	register func(*grpc.Server),
) error {
	listener, err := listen(endpoint)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	register(server)

	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		server.GracefulStop()
		close(done)
	}()

	err = server.Serve(listener)
	if err == nil {
		<-done
	}
	if err != nil {
		return fmt.Errorf("serve grpc %s %s: %w", endpoint.Network, endpoint.Address, err)
	}
	return nil
}

func listen(endpoint Endpoint) (net.Listener, error) {
	switch endpoint.Network {
	case "unix":
		if err := os.MkdirAll(filepath.Dir(endpoint.Address), 0o755); err != nil {
			return nil, fmt.Errorf("create unix socket directory: %w", err)
		}
		if err := os.Remove(endpoint.Address); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove stale unix socket: %w", err)
		}
	case "tcp":
	default:
		return nil, fmt.Errorf("unsupported rpc network %q", endpoint.Network)
	}
	listener, err := net.Listen(endpoint.Network, endpoint.Address)
	if err != nil {
		return nil, fmt.Errorf(
			"listen grpc %s %s: %w",
			endpoint.Network,
			endpoint.Address,
			err,
		)
	}
	if endpoint.Network == "unix" {
		if err := os.Chmod(endpoint.Address, 0o660); err != nil {
			listener.Close()
			return nil, fmt.Errorf("chmod unix socket: %w", err)
		}
	}
	return listener, nil
}
