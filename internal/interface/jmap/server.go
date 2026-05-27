// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package jmap

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
	"blackcat.ca/gmeow/internal/observability"
)

const (
	capabilityBlob  = "urn:ietf:params:jmap:blob"
	capabilityCore  = "urn:ietf:params:jmap:core"
	capabilityMail  = "urn:ietf:params:jmap:mail"
	capabilityQuota = "urn:ietf:params:jmap:quota"
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener
}

type Options struct {
	BearerToken string
}

type sessionResource struct {
	Capabilities    map[string]any             `json:"capabilities"`
	Accounts        map[string]accountResource `json:"accounts"`
	PrimaryAccounts map[string]string          `json:"primaryAccounts"`
	Username        string                     `json:"username"`
	APIURL          string                     `json:"apiUrl"`
	DownloadURL     string                     `json:"downloadUrl"`
	UploadURL       string                     `json:"uploadUrl"`
	EventSourceURL  string                     `json:"eventSourceUrl"`
	State           string                     `json:"state"`
}

type accountResource struct {
	AccountCapabilities map[string]any `json:"accountCapabilities"`
	Name                string         `json:"name"`
	IsPersonal          bool           `json:"isPersonal"`
	IsReadOnly          bool           `json:"isReadOnly"`
}

type apiRequest struct {
	CreatedIDs  map[string]string `json:"createdIds,omitempty"`
	MethodCalls []methodCall      `json:"methodCalls"`
	Using       []string          `json:"using"`
}

type apiResponse struct {
	CreatedIDs      map[string]string `json:"createdIds,omitempty"`
	MethodResponses []methodResponse  `json:"methodResponses"`
	SessionState    string            `json:"sessionState"`
}

type methodCall struct {
	Name      string
	Arguments json.RawMessage
	ClientID  string
}

type methodResponse struct {
	Name      string
	Arguments any
	ClientID  string
}

type jmapError struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

func New(
	address string,
	services *appsvc.Services,
	options Options,
) (*Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("JMAP address is required")
	}
	if services == nil {
		return nil, errors.New("JMAP app services are required")
	}
	if strings.TrimSpace(options.BearerToken) == "" {
		return nil, errors.New("JMAP bearer token is required")
	}

	return &Server{
		httpServer: &http.Server{
			Addr:    address,
			Handler: NewHandler(services, options),
		},
	}, nil
}

func NewHandler(services *appsvc.Services, options Options) http.Handler {
	mux := http.NewServeMux()
	server := handler{
		services:    services,
		bearerToken: strings.TrimSpace(options.BearerToken),
	}
	mux.HandleFunc("GET /.well-known/jmap", server.requireBearer(server.handleSession))
	mux.HandleFunc("GET /jmap/session", server.requireBearer(server.handleSession))
	mux.HandleFunc("POST /jmap/api", server.requireBearer(server.handleAPI))
	mux.HandleFunc(
		"GET /jmap/download/{accountId}/{blobId}/{name}",
		server.requireBearer(server.handleDownload),
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

type handler struct {
	services    *appsvc.Services
	bearerToken string
}

func (handler handler) requireBearer(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if handler.bearerToken == "" || bearerToken(request) != handler.bearerToken {
			writer.Header().Set("WWW-Authenticate", `Bearer realm="gmeow-jmap"`)
			http.Error(writer, "unauthorized", http.StatusUnauthorized)

			return
		}

		next(writer, request)
	}
}

func (handler handler) handleSession(
	writer http.ResponseWriter,
	request *http.Request,
) {
	started := time.Now()
	defer observeLatency(started)

	writeJSON(writer, handler.session(request), nil)
}

func (handler handler) handleAPI(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	defer observeLatency(started)

	var input apiRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		writeJSON(writer, apiResponse{
			MethodResponses: []methodResponse{{
				Name:      "error",
				Arguments: jmapError{Type: "invalidArguments", Description: err.Error()},
				ClientID:  "0",
			}},
			SessionState: "0",
		}, nil)

		return
	}

	output := apiResponse{
		CreatedIDs:      map[string]string{},
		MethodResponses: make([]methodResponse, 0, len(input.MethodCalls)),
		SessionState:    "0",
	}
	for _, call := range input.MethodCalls {
		output.MethodResponses = append(output.MethodResponses, handler.dispatch(call))
	}

	writeJSON(writer, output, nil)
}

func (handler handler) handleDownload(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	started := time.Now()
	defer observeLatency(started)

	writeJSON(writer, jmapError{
		Type:        "notFound",
		Description: "blob download is not implemented yet",
	}, nil)
}

func (handler handler) dispatch(call methodCall) methodResponse {
	switch call.Name {
	case "Core/echo":
		var arguments map[string]any
		if len(call.Arguments) == 0 {
			arguments = map[string]any{}
		} else if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
			return methodResponse{
				Name:      "error",
				Arguments: jmapError{Type: "invalidArguments", Description: err.Error()},
				ClientID:  call.ClientID,
			}
		}

		return methodResponse{Name: call.Name, Arguments: arguments, ClientID: call.ClientID}
	default:
		return methodResponse{
			Name: "error",
			Arguments: jmapError{
				Type:        "unknownMethod",
				Description: fmt.Sprintf("method %q is not implemented", call.Name),
			},
			ClientID: call.ClientID,
		}
	}
}

func (handler handler) session(request *http.Request) sessionResource {
	base := requestBaseURL(request)

	return sessionResource{
		Capabilities: map[string]any{
			capabilityBlob:  map[string]any{},
			capabilityCore:  map[string]any{},
			capabilityMail:  map[string]any{},
			capabilityQuota: map[string]any{},
		},
		Accounts: map[string]accountResource{
			"gmeow": {
				Name:       "gmeow",
				IsPersonal: true,
				IsReadOnly: false,
				AccountCapabilities: map[string]any{
					capabilityBlob:  map[string]any{},
					capabilityQuota: map[string]any{},
					capabilityMail: map[string]any{
						"maxMailboxesPerEmail":       nil,
						"maxMailboxDepth":            nil,
						"maxSizeMailboxName":         255,
						"maxSizeAttachmentsPerEmail": nil,
						"emailQuerySortOptions": []string{
							"receivedAt",
						},
						"mayCreateTopLevelMailbox": true,
					},
				},
			},
		},
		PrimaryAccounts: map[string]string{
			capabilityBlob:  "gmeow",
			capabilityMail:  "gmeow",
			capabilityQuota: "gmeow",
		},
		Username:       "gmeow",
		APIURL:         base + "/jmap/api",
		DownloadURL:    base + "/jmap/download/{accountId}/{blobId}/{name}",
		UploadURL:      "",
		EventSourceURL: "",
		State:          "0",
	}
}

func (call *methodCall) UnmarshalJSON(data []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(data, &tuple); err != nil {
		return err
	}
	if len(tuple) != 3 {
		return fmt.Errorf("JMAP method call must have 3 elements")
	}
	if err := json.Unmarshal(tuple[0], &call.Name); err != nil {
		return err
	}
	call.Arguments = append(json.RawMessage{}, tuple[1]...)

	return json.Unmarshal(tuple[2], &call.ClientID)
}

func (response methodResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{response.Name, response.Arguments, response.ClientID})
}

func bearerToken(request *http.Request) string {
	header := strings.TrimSpace(request.Header.Get("Authorization"))
	value, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

func requestBaseURL(request *http.Request) string {
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	if forwarded := strings.TrimSpace(
		request.Header.Get("X-Forwarded-Proto"),
	); forwarded != "" {
		scheme = forwarded
	}

	return scheme + "://" + request.Host
}

func writeJSON(writer http.ResponseWriter, output any, err error) {
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)

		return
	}

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(output); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func observeLatency(started time.Time) {
	observability.DefaultMetrics().ObserveDuration(
		"gmeow_interface_latency",
		time.Since(started),
	)
}
