// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package mcpiface

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
)

const (
	defaultHTTPSessionTimeout = 30 * time.Minute
	streamableEndpoint        = "/mcp"
)

type Server struct {
	server *mcp.Server
}

type HTTPOptions struct {
	SessionTimeout time.Duration
}

type HTTPServer struct {
	httpServer *http.Server
	listener   net.Listener
}

type digestInput struct {
	Digest contracts.ObjectDigest `json:"digest" jsonschema:"FILESTORE object digest"`
}

type retrieveInput struct {
	Digest         contracts.ObjectDigest `json:"digest"                    jsonschema:"FILESTORE object digest"`
	IncludeContent bool                   `json:"include_content,omitempty" jsonschema:"include up to 1MiB of object content"`
}

type provenanceOutput struct {
	Provenance []contracts.Provenance `json:"provenance"`
}

type facetsOutput struct {
	Facets []contracts.Facet `json:"facets"`
}

func New(services *appsvc.Services) (*Server, error) {
	if services == nil {
		return nil, errors.New("MCP app services are required")
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "gmeow", Version: "phase-6"}, nil)
	addTool(server, "object_search", "search projected objects", services.ObjectSearch)
	addTool(
		server,
		"mail_search",
		"search mail messages across index and live sources",
		services.MailSearch,
	)
	addTool(server, "object_retrieve", "retrieve an object manifest and optional content",
		func(ctx context.Context, input retrieveInput) (appsvc.RetrieveResponse, error) {
			return services.Retrieve(ctx, input.Digest, input.IncludeContent)
		},
	)
	addDigestTool(server, "get_structure", "get object structure", services.Structure)
	addDigestTool(server, "get_provenance", "get object provenance",
		func(ctx context.Context, digest contracts.ObjectDigest) (provenanceOutput, error) {
			provenance, err := services.Provenance(ctx, digest)
			return provenanceOutput{Provenance: provenance}, err
		},
	)
	addDigestTool(server, "get_facets", "get object facets",
		func(ctx context.Context, digest contracts.ObjectDigest) (facetsOutput, error) {
			facets, err := services.Facets(ctx, digest)
			return facetsOutput{Facets: facets}, err
		},
	)
	addDigestTool(
		server,
		"compound_expand",
		"expand compound object parts",
		services.Compound,
	)
	addTool(
		server,
		"graph_explore",
		"explore projected graph facts",
		services.GraphExplore,
	)
	addTool(server, "analysis_status", "inspect analysis status", services.AnalysisStatus)
	addTool(server, "force_analysis", "force analyzer scheduling", services.ForceAnalysis)
	addTool(
		server,
		"source_action",
		"apply a source-specific action",
		services.SourceAction,
	)
	addTool(server, "ops_status", "inspect operational status",
		func(ctx context.Context, _ map[string]any) (appsvc.OpsStatusResponse, error) {
			return services.OpsStatus(ctx)
		},
	)

	return &Server{server: server}, nil
}

func (server *Server) Start(ctx context.Context) error {
	return server.server.Run(ctx, &mcp.StdioTransport{})
}

func NewHTTP(
	address string,
	services *appsvc.Services,
	options HTTPOptions,
) (*HTTPServer, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("MCP HTTP address is required")
	}
	if services == nil {
		return nil, errors.New("MCP app services are required")
	}

	return &HTTPServer{
		httpServer: &http.Server{
			Addr:    address,
			Handler: NewStreamableHandler(services, options),
		},
	}, nil
}

func NewStreamableHandler(services *appsvc.Services, options HTTPOptions) http.Handler {
	sessionTimeout := options.SessionTimeout
	if sessionTimeout == 0 {
		sessionTimeout = defaultHTTPSessionTimeout
	}

	mux := http.NewServeMux()
	mcpServer, err := New(services)
	mux.HandleFunc(
		"GET /healthz",
		func(writer http.ResponseWriter, _ *http.Request) {
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)

				return
			}
			writer.WriteHeader(http.StatusNoContent)
		},
	)
	mux.HandleFunc(
		"GET /metrics",
		func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = writer.Write([]byte(observability.DefaultMetrics().PrometheusText()))
		},
	)
	if err != nil {
		mux.HandleFunc(
			streamableEndpoint,
			func(writer http.ResponseWriter, _ *http.Request) {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
			},
		)

		return mux
	}

	streamable := mcp.NewStreamableHTTPHandler(
		func(request *http.Request) *mcp.Server {
			if request.URL.Path != streamableEndpoint {
				return nil
			}

			return mcpServer.server
		},
		&mcp.StreamableHTTPOptions{
			EventStore:     mcp.NewMemoryEventStore(nil),
			SessionTimeout: sessionTimeout,
		},
	)
	mux.Handle(streamableEndpoint, observeHTTP(streamable))

	return mux
}

func observeHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		defer func() {
			observability.DefaultMetrics().ObserveDuration(
				"gmeow_interface_latency",
				time.Since(started),
			)
		}()

		next.ServeHTTP(writer, request)
	})
}

func (server *HTTPServer) Start(ctx context.Context) error {
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func (server *HTTPServer) Addr() string {
	if server.listener == nil {
		return server.httpServer.Addr
	}

	return server.listener.Addr().String()
}

func addDigestTool[Out any](
	server *mcp.Server,
	name string,
	description string,
	handler func(context.Context, contracts.ObjectDigest) (Out, error),
) {
	addTool(
		server,
		name,
		description,
		func(ctx context.Context, input digestInput) (Out, error) {
			return handler(ctx, input.Digest)
		},
	)
}

func addTool[In, Out any](
	server *mcp.Server,
	name string,
	description string,
	handler func(context.Context, In) (Out, error),
) {
	mcp.AddTool(
		server,
		&mcp.Tool{Name: name, Description: description},
		func(ctx context.Context, _ *mcp.CallToolRequest, input In) (*mcp.CallToolResult, Out, error) {
			output, err := handler(ctx, input)

			return nil, output, err
		},
	)
}
