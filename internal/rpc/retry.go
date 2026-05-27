// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const grpcRetryAttempts = 5

func retryUnaryClientInterceptor(
	ctx context.Context,
	method string,
	request any,
	reply any,
	connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	var err error
	backoff := 50 * time.Millisecond
	for attempt := 1; attempt <= grpcRetryAttempts; attempt++ {
		err = invoker(ctx, method, request, reply, connection, options...)
		if err == nil || attempt == grpcRetryAttempts || !retryableGRPCError(err) {
			return err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
	}

	return err
}

func retryableGRPCError(err error) bool {
	if err == nil {
		return false
	}

	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition,
		codes.OutOfRange, codes.Unimplemented:
		return false
	default:
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	return false
}
