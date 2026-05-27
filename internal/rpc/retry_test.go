// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryUnaryClientInterceptorRetriesUnavailable(t *testing.T) {
	attempts := 0
	err := retryUnaryClientInterceptor(
		context.Background(),
		"/gmeow.v1.Test/Method",
		nil,
		nil,
		nil,
		func(
			context.Context,
			string,
			any,
			any,
			*grpc.ClientConn,
			...grpc.CallOption,
		) error {
			attempts++
			if attempts < 3 {
				return status.Error(codes.Unavailable, "backend restarting")
			}

			return nil
		},
	)
	if err != nil {
		t.Fatalf("retry interceptor returned error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryUnaryClientInterceptorDoesNotRetryInvalidArgument(t *testing.T) {
	attempts := 0
	err := retryUnaryClientInterceptor(
		context.Background(),
		"/gmeow.v1.Test/Method",
		nil,
		nil,
		nil,
		func(
			context.Context,
			string,
			any,
			any,
			*grpc.ClientConn,
			...grpc.CallOption,
		) error {
			attempts++

			return status.Error(codes.InvalidArgument, "bad request")
		},
	)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
