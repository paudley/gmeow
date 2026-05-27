// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener
}

func New(address string, services *appsvc.Services) (*Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("REST address is required")
	}
	if services == nil {
		return nil, errors.New("REST app services are required")
	}

	mux := NewHandler(services)

	return &Server{httpServer: &http.Server{Addr: address, Handler: mux}}, nil
}

func NewHandler(services *appsvc.Services) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/object_search", handleJSON(services.ObjectSearch))
	mux.HandleFunc("POST /v1/mail_search", handleJSON(services.MailSearch))
	mux.HandleFunc("POST /v1/object_retrieve", handleRetrieve(services))
	mux.HandleFunc("POST /v1/get_structure", handleDigest(services.Structure))
	mux.HandleFunc("POST /v1/get_provenance", handleDigest(services.Provenance))
	mux.HandleFunc("POST /v1/get_facets", handleDigest(services.Facets))
	mux.HandleFunc("POST /v1/compound_expand", handleDigest(services.Compound))
	mux.HandleFunc("POST /v1/graph_explore", handleJSON(services.GraphExplore))
	mux.HandleFunc("POST /v1/analysis_status", handleJSON(services.AnalysisStatus))
	mux.HandleFunc("POST /v1/force_analysis", handleJSON(services.ForceAnalysis))
	mux.HandleFunc("POST /v1/source_action", handleJSON(services.SourceAction))
	mux.HandleFunc("POST /v1/operation_status", handleJSON(services.OperationStatus))
	mux.HandleFunc("POST /v1/operation_result", handleJSON(services.OperationResult))
	mux.HandleFunc(
		"GET /v1/ops_status",
		func(writer http.ResponseWriter, request *http.Request) {
			started := time.Now()
			defer func() {
				observability.DefaultMetrics().ObserveDuration(
					"gmeow_interface_latency",
					time.Since(started),
				)
			}()

			output, err := services.OpsStatus(request.Context())
			writeJSON(writer, output, err)
		},
	)
	mux.HandleFunc(
		"GET /metrics",
		func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = writer.Write([]byte(observability.DefaultMetrics().PrometheusText()))
		},
	)

	return mux
}

func (server *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", server.httpServer.Addr)
	if err != nil {
		return err
	}
	server.listener = listener

	errc := make(chan error, 1)
	go func() {
		errc <- server.httpServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := server.httpServer.Shutdown(shutdownCtx)
		if err != nil {
			return err
		}

		return ctx.Err()
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	}
}

func (server *Server) Addr() string {
	if server.listener == nil {
		return server.httpServer.Addr
	}

	return server.listener.Addr().String()
}

type digestRequest struct {
	Digest contracts.ObjectDigest `json:"digest"`
}

type retrieveRequest struct {
	Digest         contracts.ObjectDigest `json:"digest"`
	IncludeContent bool                   `json:"include_content,omitempty"`
}

func handleJSON[In, Out any](
	handler func(context.Context, In) (Out, error),
) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		defer func() {
			observability.DefaultMetrics().ObserveDuration(
				"gmeow_interface_latency",
				time.Since(started),
			)
		}()

		var input In
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			http.Error(writer, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)

			return
		}

		output, err := handler(request.Context(), input)
		writeJSON(writer, output, err)
	}
}

func handleDigest[Out any](
	handler func(context.Context, contracts.ObjectDigest) (Out, error),
) http.HandlerFunc {
	return handleJSON(func(ctx context.Context, request digestRequest) (Out, error) {
		return handler(ctx, request.Digest)
	})
}

func handleRetrieve(services *appsvc.Services) http.HandlerFunc {
	return handleJSON(
		func(ctx context.Context, request retrieveRequest) (appsvc.RetrieveResponse, error) {
			return services.Retrieve(ctx, request.Digest, request.IncludeContent)
		},
	)
}

func writeJSON[Out any](writer http.ResponseWriter, output Out, err error) {
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(output); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}
