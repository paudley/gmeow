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

type messageSummaryInput struct {
	MessageID string `json:"message_id" jsonschema:"RFC Message-ID value"`
}

func New(services *appsvc.Services) (*Server, error) {
	if services == nil {
		return nil, errors.New("MCP app services are required")
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "gmeow", Version: "phase-6"}, nil)
	addToonTool(server, "object_search", "search projected objects",
		func(ctx context.Context, input appsvc.SearchOptions) (any, error) {
			response, err := services.ObjectSearch(ctx, input)
			if err != nil {
				return nil, err
			}

			return toonSearch("object_search", input, response), nil
		},
	)
	addToonToolWithRequest(
		server,
		"mail_search",
		"search mail messages across index and live sources",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input appsvc.SearchOptions,
		) (any, error) {
			response, err := services.MailSearchOperation(
				ctx,
				input,
				mcpProgressSink(ctx, request),
			)
			if err != nil {
				return nil, err
			}

			return toonOperationForTool("mail_search", response), nil
		},
	)
	addToonTool(
		server,
		"message_summary",
		"display the canonical message-list view for an RFC Message-ID",
		func(ctx context.Context, input messageSummaryInput) (any, error) {
			response, err := services.MessageSummary(ctx, appsvc.MessageSummaryRequest{
				MessageID: input.MessageID,
			})
			if err != nil {
				return nil, err
			}

			return toonMessageSummary(response), nil
		},
	)
	addToonTool(
		server,
		"summary_search",
		"search mail and return a compact three-line summary per message",
		func(ctx context.Context, input appsvc.SearchOptions) (any, error) {
			response, err := services.SummarySearch(ctx, input)
			if err != nil {
				return nil, err
			}

			return toonSummarySearch(response), nil
		},
	)
	addToonToolWithRequest(
		server,
		"object_retrieve",
		"retrieve an object manifest and optional content",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input retrieveInput,
		) (any, error) {
			response, err := services.ObjectRetrieveOperation(
				ctx,
				string(input.Digest),
				input.IncludeContent,
				mcpProgressSink(ctx, request),
			)
			if err != nil {
				return nil, err
			}

			return toonOperationForTool("object_retrieve", response), nil
		},
	)
	addToonDigestTool(server, "get_structure", "get object structure",
		func(ctx context.Context, digest contracts.ObjectDigest) (any, error) {
			response, err := services.Structure(ctx, digest)
			if err != nil {
				return nil, err
			}

			return toonStructure(response), nil
		},
	)
	addToonDigestTool(server, "get_provenance", "get object provenance",
		func(ctx context.Context, digest contracts.ObjectDigest) (any, error) {
			provenance, err := services.Provenance(ctx, digest)
			if err != nil {
				return nil, err
			}

			return toonProvenanceResult(provenance), nil
		},
	)
	addToonDigestTool(server, "get_facets", "get object facets",
		func(ctx context.Context, digest contracts.ObjectDigest) (any, error) {
			facets, err := services.Facets(ctx, digest)
			if err != nil {
				return nil, err
			}

			return toonFacetsResult(facets), nil
		},
	)
	addToonDigestTool(
		server,
		"compound_expand",
		"expand compound object parts",
		func(ctx context.Context, digest contracts.ObjectDigest) (any, error) {
			compound, err := services.Compound(ctx, digest)
			if err != nil {
				return nil, err
			}

			return toonCompound(compound, true), nil
		},
	)
	addToonToolWithRequest(
		server,
		"graph_explore",
		"explore projected graph facts",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input contracts.GraphRequest,
		) (any, error) {
			response, err := services.GraphExploreOperation(
				ctx,
				input,
				mcpProgressSink(ctx, request),
			)
			if err != nil {
				return nil, err
			}

			return toonOperationForTool("graph_explore", response), nil
		},
	)
	addToonToolWithRequest(server, "analysis_status", "inspect analysis status",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input contracts.AnalysisStatusRequest,
		) (any, error) {
			response, err := services.AnalysisStatusOperation(
				ctx,
				input,
				mcpProgressSink(ctx, request),
			)
			if err != nil {
				return nil, err
			}

			return toonOperationForTool("analysis_status", response), nil
		},
	)
	addToonToolWithRequest(server, "force_analysis", "force analyzer scheduling",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input appsvc.ForceAnalysisRequest,
		) (any, error) {
			response, err := services.ForceAnalysisOperation(
				ctx,
				input,
				mcpProgressSink(ctx, request),
			)
			if err != nil {
				return nil, err
			}

			return toonOperationForTool("force_analysis", response), nil
		},
	)
	addToonToolWithRequest(
		server,
		"operation_status",
		"inspect a durable interface operation",
		func(
			ctx context.Context,
			_ *mcp.CallToolRequest,
			input contracts.OperationStatusRequest,
		) (any, error) {
			response, err := services.OperationStatus(ctx, input)
			if err != nil {
				return nil, err
			}

			return toonOperation(response), nil
		},
	)
	addToonToolWithRequest(
		server,
		"operation_result",
		"retrieve a completed durable interface operation result",
		func(
			ctx context.Context,
			_ *mcp.CallToolRequest,
			input contracts.OperationResultRequest,
		) (any, error) {
			response, err := services.OperationResult(ctx, input)
			if err != nil {
				return nil, err
			}

			return toonOperationResult(response), nil
		},
	)
	addToonToolWithRequest(
		server,
		"operation_resume",
		"resume waiting for a durable interface operation",
		func(
			ctx context.Context,
			request *mcp.CallToolRequest,
			input contracts.OperationResultRequest,
		) (any, error) {
			response, err := services.OperationResume(ctx, input, mcpProgressSink(ctx, request))
			if err != nil {
				return nil, err
			}

			return toonOperationResume(response), nil
		},
	)
	addToonTool(
		server,
		"source_action",
		"apply a source-specific action",
		func(ctx context.Context, input appsvc.SourceActionRequest) (any, error) {
			response, err := services.SourceAction(ctx, input)
			if err != nil {
				return nil, err
			}

			return toonSourceAction(response), nil
		},
	)
	addToonTool(server, "ops_status", "inspect operational status",
		func(ctx context.Context, _ map[string]any) (any, error) {
			response, err := services.OpsStatus(ctx)
			if err != nil {
				return nil, err
			}

			return toonOpsStatus(response), nil
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

func addToonDigestTool(
	server *mcp.Server,
	name string,
	description string,
	handler func(context.Context, contracts.ObjectDigest) (any, error),
) {
	addToonTool(
		server,
		name,
		description,
		func(ctx context.Context, input digestInput) (any, error) {
			return handler(ctx, input.Digest)
		},
	)
}

func addToonTool[In any](
	server *mcp.Server,
	name string,
	description string,
	handler func(context.Context, In) (any, error),
) {
	addToonToolWithRequest(
		server,
		name,
		description,
		func(ctx context.Context, _ *mcp.CallToolRequest, input In) (any, error) {
			return handler(ctx, input)
		},
	)
}

func addToonToolWithRequest[In any](
	server *mcp.Server,
	name string,
	description string,
	handler func(context.Context, *mcp.CallToolRequest, In) (any, error),
) {
	mcp.AddTool[In, any](
		server,
		&mcp.Tool{Name: name, Description: description},
		func(ctx context.Context, request *mcp.CallToolRequest, input In) (*mcp.CallToolResult, any, error) {
			output, err := handler(ctx, request, input)
			if err != nil {
				result, encodeErr := toonToolError(name, err)

				return result, nil, encodeErr
			}
			result, err := toonToolResult(output)

			return result, nil, err
		},
	)
}

func mcpProgressSink(
	ctx context.Context,
	request *mcp.CallToolRequest,
) appsvc.OperationProgressSink {
	if request == nil || request.Session == nil ||
		request.Params.GetProgressToken() == nil {
		return nil
	}

	progressToken := request.Params.GetProgressToken()

	return func(event contracts.OperationProgressEvent) {
		message := event.Message
		if message == "" {
			message = event.Stage
		}
		_ = request.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
			ProgressToken: progressToken,
			Progress:      event.Progress,
			Total:         event.Total,
			Message:       message,
		})
	}
}
