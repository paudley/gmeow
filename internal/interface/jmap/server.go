// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package jmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
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

type getArguments struct {
	AccountID string   `json:"accountId"`
	IDs       []string `json:"ids"`
}

type blobLookupArguments struct {
	AccountID string   `json:"accountId"`
	TypeNames []string `json:"typeNames"`
	IDs       []string `json:"ids"`
}

type queryArguments struct {
	Filter    emailQueryFilter `json:"filter"`
	AccountID string           `json:"accountId"`
	Limit     int              `json:"limit"`
	Position  int              `json:"position"`
}

type searchSnippetArguments struct {
	Filter    json.RawMessage `json:"filter"`
	AccountID string          `json:"accountId"`
	EmailIDs  []string        `json:"emailIds"`
}

type emailQueryFilter struct {
	Text       string `json:"text"`
	InMailbox  string `json:"inMailbox"`
	HasKeyword string `json:"hasKeyword"`
	NotKeyword string `json:"notKeyword"`
}

type mailboxGetResponse struct {
	AccountID string        `json:"accountId"`
	State     string        `json:"state"`
	List      []jmapMailbox `json:"list"`
	NotFound  []string      `json:"notFound,omitempty"`
}

type mailboxQueryResponse struct {
	AccountID           string   `json:"accountId"`
	QueryState          string   `json:"queryState"`
	CanCalculateChanges bool     `json:"canCalculateChanges"`
	IDs                 []string `json:"ids"`
	Position            int      `json:"position"`
	Total               int      `json:"total"`
}

type jmapMailbox struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	ParentID  *string `json:"parentId"`
	Role      *string `json:"role"`
	SortOrder int     `json:"sortOrder"`
}

type emailQueryResponse struct {
	AccountID           string   `json:"accountId"`
	QueryState          string   `json:"queryState"`
	CanCalculateChanges bool     `json:"canCalculateChanges"`
	Position            int      `json:"position"`
	IDs                 []string `json:"ids"`
	Total               int      `json:"total"`
}

type emailGetResponse struct {
	AccountID string      `json:"accountId"`
	State     string      `json:"state"`
	List      []jmapEmail `json:"list"`
	NotFound  []string    `json:"notFound,omitempty"`
}

type threadGetResponse struct {
	AccountID string       `json:"accountId"`
	State     string       `json:"state"`
	List      []jmapThread `json:"list"`
	NotFound  []string     `json:"notFound,omitempty"`
}

type blobGetResponse struct {
	AccountID string     `json:"accountId"`
	State     string     `json:"state"`
	List      []jmapBlob `json:"list"`
	NotFound  []string   `json:"notFound,omitempty"`
}

type blobLookupResponse struct {
	AccountID string         `json:"accountId"`
	List      []jmapBlobInfo `json:"list"`
	NotFound  []string       `json:"notFound,omitempty"`
}

type quotaGetResponse struct {
	AccountID string      `json:"accountId"`
	State     string      `json:"state"`
	List      []jmapQuota `json:"list"`
	NotFound  []string    `json:"notFound,omitempty"`
}

type quotaQueryResponse struct {
	AccountID           string   `json:"accountId"`
	QueryState          string   `json:"queryState"`
	CanCalculateChanges bool     `json:"canCalculateChanges"`
	IDs                 []string `json:"ids"`
	Position            int      `json:"position"`
	Total               int      `json:"total"`
}

type quotaChangesArguments struct {
	AccountID  string `json:"accountId"`
	SinceState string `json:"sinceState"`
}

type quotaChangesResponse struct {
	AccountID      string   `json:"accountId"`
	OldState       string   `json:"oldState"`
	NewState       string   `json:"newState"`
	HasMoreChanges bool     `json:"hasMoreChanges"`
	Created        []string `json:"created"`
	Updated        []string `json:"updated"`
	Destroyed      []string `json:"destroyed"`
}

type quotaQueryChangesArguments struct {
	AccountID       string `json:"accountId"`
	SinceQueryState string `json:"sinceQueryState"`
}

type quotaQueryChangesResponse struct {
	AccountID     string                `json:"accountId"`
	OldQueryState string                `json:"oldQueryState"`
	NewQueryState string                `json:"newQueryState"`
	Removed       []string              `json:"removed"`
	Added         []quotaQueryAddedItem `json:"added"`
	Total         int                   `json:"total"`
}

type quotaQueryAddedItem struct {
	ID    string `json:"id"`
	Index int    `json:"index"`
}

type jmapQuota struct {
	ID           string   `json:"id"`
	ResourceType string   `json:"resourceType"`
	Scope        string   `json:"scope"`
	Name         string   `json:"name"`
	Types        []string `json:"types"`
	Used         uint64   `json:"used"`
	HardLimit    uint64   `json:"hardLimit"`
}

type searchSnippetGetResponse struct {
	AccountID string              `json:"accountId"`
	List      []jmapSearchSnippet `json:"list"`
	NotFound  []string            `json:"notFound,omitempty"`
}

type jmapThread struct {
	ID       string   `json:"id"`
	EmailIDs []string `json:"emailIds"`
}

type jmapSearchSnippet struct {
	EmailID string  `json:"emailId"`
	Subject *string `json:"subject"`
	Preview *string `json:"preview"`
}

type jmapBlob struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

type jmapBlobInfo struct {
	MatchedIDs map[string][]string `json:"matchedIds"`
	ID         string              `json:"id"`
}

type setArguments struct {
	Update    map[string]map[string]json.RawMessage `json:"update,omitempty"`
	AccountID string                                `json:"accountId"`
}

type emailSetResponse struct {
	AccountID  string               `json:"accountId"`
	OldState   string               `json:"oldState"`
	NewState   string               `json:"newState"`
	Updated    map[string]any       `json:"updated,omitempty"`
	NotUpdated map[string]jmapError `json:"notUpdated,omitempty"`
}

type jmapEmail struct {
	MailboxIDs map[string]bool `json:"mailboxIds"`
	Keywords   map[string]bool `json:"keywords"`
	ID         string          `json:"id"`
	BlobID     string          `json:"blobId"`
	ThreadID   string          `json:"threadId,omitempty"`
	MessageID  []string        `json:"messageId,omitempty"`
	From       []emailAddress  `json:"from,omitempty"`
	To         []emailAddress  `json:"to,omitempty"`
	Subject    string          `json:"subject,omitempty"`
	ReceivedAt string          `json:"receivedAt,omitempty"`
	Preview    string          `json:"preview,omitempty"`
	Size       int64           `json:"size"`
}

type emailAddress struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email"`
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
		output.MethodResponses = append(
			output.MethodResponses,
			handler.dispatch(request.Context(), call),
		)
	}

	writeJSON(writer, output, nil)
}

func (handler handler) handleDownload(
	writer http.ResponseWriter,
	request *http.Request,
) {
	started := time.Now()
	defer observeLatency(started)

	if err := validateAccountID(request.PathValue("accountId")); err != nil {
		writeJSONStatus(
			writer,
			http.StatusBadRequest,
			jmapError{Type: "invalidArguments", Description: err.Error()},
		)

		return
	}
	if handler.services == nil {
		writeJSONStatus(
			writer,
			http.StatusInternalServerError,
			jmapError{
				Type:        "serverFail",
				Description: "JMAP app services are not configured",
			},
		)

		return
	}

	blob, reader, err := handler.services.JMAPBlobOpen(
		request.Context(),
		request.PathValue("blobId"),
	)
	if err != nil {
		writeJSONStatus(
			writer,
			http.StatusNotFound,
			jmapError{Type: "notFound", Description: err.Error()},
		)

		return
	}
	defer reader.Close()

	writer.Header().Set("Content-Type", blob.Type)
	if blob.Size >= 0 {
		writer.Header().Set("Content-Length", fmt.Sprintf("%d", blob.Size))
	}
	if filename := downloadFilename(request.PathValue("name")); filename != "" {
		writer.Header().Set(
			"Content-Disposition",
			mime.FormatMediaType("attachment", map[string]string{"filename": filename}),
		)
	}
	if _, err := io.Copy(writer, reader); err != nil {
		observability.Logger(request.Context()).Warn(
			"jmap download stream failed",
			"error", err,
			"blob_id", blob.ID,
		)
	}
}

func (handler handler) dispatch(ctx context.Context, call methodCall) methodResponse {
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
	case "Blob/get":
		return handler.handleBlobGet(ctx, call)
	case "Blob/lookup":
		return handler.handleBlobLookup(ctx, call)
	case "Quota/get":
		return handler.handleQuotaGet(call)
	case "Quota/query":
		return handler.handleQuotaQuery(call)
	case "Quota/changes":
		return handler.handleQuotaChanges(call)
	case "Quota/queryChanges":
		return handler.handleQuotaQueryChanges(call)
	case "Mailbox/get":
		return handler.handleMailboxGet(ctx, call)
	case "Mailbox/query":
		return handler.handleMailboxQuery(ctx, call)
	case "Email/query":
		return handler.handleEmailQuery(ctx, call)
	case "Email/get":
		return handler.handleEmailGet(ctx, call)
	case "SearchSnippet/get":
		return handler.handleSearchSnippetGet(ctx, call)
	case "Email/set":
		return handler.handleEmailSet(ctx, call)
	case "Thread/get":
		return handler.handleThreadGet(ctx, call)
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

func (handler handler) handleBlobLookup(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments blobLookupArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateBlobLookupTypeNames(arguments.TypeNames); err != nil {
		return methodResponse{
			Name:      "error",
			Arguments: jmapError{Type: "unknownDataType", Description: err.Error()},
			ClientID:  call.ClientID,
		}
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	blobIDs := make([]contracts.ObjectDigest, 0, len(arguments.IDs))
	for _, id := range arguments.IDs {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			blobIDs = append(blobIDs, contracts.ObjectDigest(trimmed))
		}
	}
	response, err := handler.services.JMAPBlobLookup(
		ctx,
		contracts.JMAPBlobLookupRequest{
			TypeNames: append([]string{}, arguments.TypeNames...),
			BlobIDs:   blobIDs,
		},
	)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}

	list := make([]jmapBlobInfo, 0, len(arguments.IDs))
	for _, id := range arguments.IDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		references := response.Blobs[contracts.ObjectDigest(trimmed)]
		list = append(list, toJMAPBlobInfo(trimmed, arguments.TypeNames, references))
	}

	return methodResponse{
		Name: "Blob/lookup",
		Arguments: blobLookupResponse{
			AccountID: "gmeow",
			List:      list,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleQuotaGet(call methodCall) methodResponse {
	var arguments getArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}

	notFound := []string{}
	for _, id := range arguments.IDs {
		if strings.TrimSpace(id) != "" {
			notFound = append(notFound, id)
		}
	}

	return methodResponse{
		Name: "Quota/get",
		Arguments: quotaGetResponse{
			AccountID: "gmeow",
			State:     "0",
			List:      []jmapQuota{},
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleQuotaQuery(call methodCall) methodResponse {
	var arguments queryArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}

	return methodResponse{
		Name: "Quota/query",
		Arguments: quotaQueryResponse{
			AccountID:           "gmeow",
			QueryState:          "0",
			CanCalculateChanges: false,
			IDs:                 []string{},
			Position:            arguments.Position,
			Total:               0,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleQuotaChanges(call methodCall) methodResponse {
	var arguments quotaChangesArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}

	return methodResponse{
		Name: "Quota/changes",
		Arguments: quotaChangesResponse{
			AccountID:      "gmeow",
			OldState:       arguments.SinceState,
			NewState:       "0",
			HasMoreChanges: false,
			Created:        []string{},
			Updated:        []string{},
			Destroyed:      []string{},
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleQuotaQueryChanges(call methodCall) methodResponse {
	var arguments quotaQueryChangesArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}

	return methodResponse{
		Name: "Quota/queryChanges",
		Arguments: quotaQueryChangesResponse{
			AccountID:     "gmeow",
			OldQueryState: arguments.SinceQueryState,
			NewQueryState: "0",
			Removed:       []string{},
			Added:         []quotaQueryAddedItem{},
			Total:         0,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleBlobGet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments getArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	blobs, notFound, err := handler.services.JMAPBlobGet(ctx, arguments.IDs)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}
	list := make([]jmapBlob, 0, len(arguments.IDs))
	for _, id := range arguments.IDs {
		blob, ok := blobs[id]
		if !ok {
			continue
		}
		list = append(list, toJMAPBlob(blob))
	}

	return methodResponse{
		Name: "Blob/get",
		Arguments: blobGetResponse{
			AccountID: "gmeow",
			State:     "0",
			List:      list,
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleSearchSnippetGet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments searchSnippetArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	filterText := searchSnippetFilterText(arguments.Filter)
	list := make([]jmapSearchSnippet, 0, len(arguments.EmailIDs))
	notFound := []string{}
	for _, id := range arguments.EmailIDs {
		digest := contracts.ObjectDigest(strings.TrimSpace(id))
		if digest == "" {
			continue
		}
		retrieved, err := handler.services.Retrieve(ctx, digest, false)
		if err != nil || retrieved.Message == nil {
			notFound = append(notFound, id)
			continue
		}
		list = append(list, toJMAPSearchSnippet(id, *retrieved.Message, filterText))
	}

	return methodResponse{
		Name: "SearchSnippet/get",
		Arguments: searchSnippetGetResponse{
			AccountID: "gmeow",
			List:      list,
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleThreadGet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments getArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	threads, err := handler.services.JMAPThreads(ctx, arguments.IDs)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}
	list := make([]jmapThread, 0, len(arguments.IDs))
	notFound := []string{}
	for _, id := range arguments.IDs {
		thread, ok := threads[id]
		if !ok {
			notFound = append(notFound, id)
			continue
		}
		emailIDs := make([]string, 0, len(thread.EmailIDs))
		for _, emailID := range thread.EmailIDs {
			emailIDs = append(emailIDs, string(emailID))
		}
		list = append(list, jmapThread{ID: thread.ID, EmailIDs: emailIDs})
	}

	return methodResponse{
		Name: "Thread/get",
		Arguments: threadGetResponse{
			AccountID: "gmeow",
			State:     "0",
			List:      list,
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleEmailSet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments setArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	updated := map[string]any{}
	notUpdated := map[string]jmapError{}
	for id, patches := range arguments.Update {
		mutation, err := mutationFromJMAPPatch(id, patches)
		if err != nil {
			notUpdated[id] = jmapError{Type: "invalidPatch", Description: err.Error()}
			continue
		}
		if _, err := handler.services.UpdateJMAPEmailState(ctx, mutation); err != nil {
			notUpdated[id] = jmapError{Type: "serverFail", Description: err.Error()}
			continue
		}
		updated[id] = nil
	}

	return methodResponse{
		Name: "Email/set",
		Arguments: emailSetResponse{
			AccountID:  "gmeow",
			OldState:   "0",
			NewState:   "0",
			Updated:    emptyMapAsNil(updated),
			NotUpdated: emptyJMAPErrorMapAsNil(notUpdated),
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleMailboxQuery(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments queryArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	mailboxes, err := handler.services.JMAPMailboxes(ctx)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}
	ids := make([]string, 0, len(mailboxes))
	for _, mailbox := range mailboxes {
		ids = append(ids, mailbox.MailboxID)
	}

	return methodResponse{
		Name: "Mailbox/query",
		Arguments: mailboxQueryResponse{
			AccountID:           "gmeow",
			QueryState:          "0",
			CanCalculateChanges: false,
			IDs:                 ids,
			Position:            0,
			Total:               len(ids),
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleMailboxGet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments getArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	mailboxes, err := handler.services.JMAPMailboxes(ctx)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}
	wanted := map[string]bool{}
	for _, id := range arguments.IDs {
		wanted[id] = true
	}
	list := make([]jmapMailbox, 0, len(mailboxes))
	found := map[string]bool{}
	for _, mailbox := range mailboxes {
		if len(wanted) > 0 && !wanted[mailbox.MailboxID] {
			continue
		}
		found[mailbox.MailboxID] = true
		list = append(list, toJMAPMailbox(mailbox))
	}
	notFound := []string{}
	for _, id := range arguments.IDs {
		if !found[id] {
			notFound = append(notFound, id)
		}
	}

	return methodResponse{
		Name: "Mailbox/get",
		Arguments: mailboxGetResponse{
			AccountID: "gmeow",
			State:     "0",
			List:      list,
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleEmailQuery(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments queryArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	response, err := handler.services.JMAPEmailQuery(
		ctx,
		appsvc.JMAPEmailQueryRequest{
			Text:       arguments.Filter.Text,
			InMailbox:  arguments.Filter.InMailbox,
			HasKeyword: arguments.Filter.HasKeyword,
			NotKeyword: arguments.Filter.NotKeyword,
			Offset:     arguments.Position,
			Limit:      arguments.Limit,
		},
	)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}
	ids := make([]string, 0, len(response.IDs))
	for _, id := range response.IDs {
		ids = append(ids, string(id))
	}

	return methodResponse{
		Name: "Email/query",
		Arguments: emailQueryResponse{
			AccountID:           "gmeow",
			QueryState:          "0",
			CanCalculateChanges: false,
			Position:            response.Offset,
			IDs:                 ids,
			Total:               response.Total,
		},
		ClientID: call.ClientID,
	}
}

func (handler handler) handleEmailGet(
	ctx context.Context,
	call methodCall,
) methodResponse {
	var arguments getArguments
	if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if err := validateAccountID(arguments.AccountID); err != nil {
		return invalidArguments(call.ClientID, err)
	}
	if handler.services == nil {
		return serverFail(call.ClientID, "JMAP app services are not configured")
	}

	digests := make([]contracts.ObjectDigest, 0, len(arguments.IDs))
	for _, id := range arguments.IDs {
		if strings.TrimSpace(id) != "" {
			digests = append(digests, contracts.ObjectDigest(id))
		}
	}
	states, err := handler.services.JMAPEmailStates(ctx, digests)
	if err != nil {
		return serverFail(call.ClientID, err.Error())
	}

	list := make([]jmapEmail, 0, len(digests))
	notFound := []string{}
	for _, digest := range digests {
		retrieved, err := handler.services.Retrieve(ctx, digest, false)
		if err != nil || retrieved.Message == nil {
			notFound = append(notFound, string(digest))
			continue
		}
		state, ok := states[digest]
		if !ok {
			notFound = append(notFound, string(digest))
			continue
		}
		list = append(list, toJMAPEmail(digest, retrieved, state))
	}

	return methodResponse{
		Name: "Email/get",
		Arguments: emailGetResponse{
			AccountID: "gmeow",
			State:     "0",
			List:      list,
			NotFound:  notFound,
		},
		ClientID: call.ClientID,
	}
}

func invalidArguments(clientID string, err error) methodResponse {
	return methodResponse{
		Name:      "error",
		Arguments: jmapError{Type: "invalidArguments", Description: err.Error()},
		ClientID:  clientID,
	}
}

func serverFail(clientID, description string) methodResponse {
	return methodResponse{
		Name:      "error",
		Arguments: jmapError{Type: "serverFail", Description: description},
		ClientID:  clientID,
	}
}

func validateAccountID(accountID string) error {
	if accountID == "" || accountID == "gmeow" {
		return nil
	}

	return fmt.Errorf("unknown accountId %q", accountID)
}

func validateBlobLookupTypeNames(typeNames []string) error {
	for _, typeName := range typeNames {
		switch typeName {
		case "Email", "Thread", "Mailbox":
		default:
			return fmt.Errorf("Blob/lookup data type %q is not supported", typeName)
		}
	}

	return nil
}

func toJMAPMailbox(mailbox contracts.JMAPMailbox) jmapMailbox {
	var parentID *string
	if mailbox.ParentID != "" {
		parentID = &mailbox.ParentID
	}
	var role *string
	if mailbox.Role != "" {
		role = &mailbox.Role
	}

	return jmapMailbox{
		ID:        mailbox.MailboxID,
		Name:      mailbox.Name,
		ParentID:  parentID,
		Role:      role,
		SortOrder: mailbox.SortOrder,
	}
}

func toJMAPSearchSnippet(
	emailID string,
	message appsvc.CanonicalMessage,
	filterText string,
) jmapSearchSnippet {
	subject := markedSnippet(message.SelectedHeaders.Subject, filterText, 255)
	preview := markedSnippet(message.Body, filterText, 255)
	if preview == nil {
		preview = markedSnippet(message.Summary, filterText, 255)
	}

	return jmapSearchSnippet{
		EmailID: emailID,
		Subject: subject,
		Preview: preview,
	}
}

func toJMAPBlob(blob appsvc.JMAPBlob) jmapBlob {
	return jmapBlob{
		ID:   blob.ID,
		Type: blob.Type,
		Size: blob.Size,
	}
}

func toJMAPBlobInfo(
	blobID string,
	typeNames []string,
	references contracts.JMAPBlobReferences,
) jmapBlobInfo {
	matchedIDs := make(map[string][]string, len(typeNames))
	for _, typeName := range typeNames {
		switch typeName {
		case "Email":
			matchedIDs[typeName] = objectDigestStrings(references.EmailIDs)
		case "Thread":
			matchedIDs[typeName] = append([]string{}, references.ThreadIDs...)
		case "Mailbox":
			matchedIDs[typeName] = append([]string{}, references.MailboxIDs...)
		}
		if matchedIDs[typeName] == nil {
			matchedIDs[typeName] = []string{}
		}
	}

	return jmapBlobInfo{
		ID:         blobID,
		MatchedIDs: matchedIDs,
	}
}

func toJMAPEmail(
	digest contracts.ObjectDigest,
	retrieved appsvc.RetrieveResponse,
	state contracts.JMAPEmailState,
) jmapEmail {
	message := retrieved.Message
	email := jmapEmail{
		MailboxIDs: boolSet(state.MailboxIDs),
		Keywords:   boolSet(state.Keywords),
		ID:         string(digest),
		BlobID:     string(digest),
		ThreadID:   state.ThreadID,
		Size:       retrieved.Manifest.Size,
	}
	if message == nil {
		return email
	}

	email.Subject = message.SelectedHeaders.Subject
	email.Preview = message.Summary
	email.MessageID = messageIDs(message.MessageID)
	email.From = addressList(message.SelectedHeaders.From)
	email.To = addressList(message.SelectedHeaders.To)
	if !state.ReceivedAt.IsZero() {
		email.ReceivedAt = state.ReceivedAt.UTC().Format(time.RFC3339Nano)
	}

	return email
}

func boolSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out[value] = true
		}
	}

	return out
}

func objectDigestStrings(values []contracts.ObjectDigest) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}

	return out
}

func messageIDs(value string) []string {
	trimmed := strings.Trim(strings.TrimSpace(value), "<>")
	if trimmed == "" {
		return nil
	}

	return []string{trimmed}
}

func addressList(value string) []emailAddress {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	parsed, err := mail.ParseAddress(trimmed)
	if err != nil {
		return []emailAddress{{Email: trimmed}}
	}

	return []emailAddress{{Name: parsed.Name, Email: parsed.Address}}
}

func mutationFromJMAPPatch(
	id string,
	patches map[string]json.RawMessage,
) (appsvc.JMAPEmailMutation, error) {
	mutation := appsvc.JMAPEmailMutation{
		ObjectDigest: contracts.ObjectDigest(id),
		MailboxIDs:   map[string]bool{},
		Keywords:     map[string]bool{},
	}
	for path, raw := range patches {
		switch {
		case path == "mailboxIds":
			values, err := boolMapFromRaw(raw)
			if err != nil {
				return appsvc.JMAPEmailMutation{}, fmt.Errorf("mailboxIds: %w", err)
			}
			mutation.ReplaceMailboxes = true
			for key, value := range values {
				mutation.MailboxIDs[key] = value
			}
		case strings.HasPrefix(path, "mailboxIds/"):
			value, err := boolPatchValue(raw)
			if err != nil {
				return appsvc.JMAPEmailMutation{}, fmt.Errorf("%s: %w", path, err)
			}
			mutation.MailboxIDs[strings.TrimPrefix(path, "mailboxIds/")] = value
		case path == "keywords":
			values, err := boolMapFromRaw(raw)
			if err != nil {
				return appsvc.JMAPEmailMutation{}, fmt.Errorf("keywords: %w", err)
			}
			mutation.ReplaceKeywords = true
			for key, value := range values {
				mutation.Keywords[key] = value
			}
		case strings.HasPrefix(path, "keywords/"):
			value, err := boolPatchValue(raw)
			if err != nil {
				return appsvc.JMAPEmailMutation{}, fmt.Errorf("%s: %w", path, err)
			}
			mutation.Keywords[strings.TrimPrefix(path, "keywords/")] = value
		default:
			return appsvc.JMAPEmailMutation{}, fmt.Errorf("unsupported patch %q", path)
		}
	}

	return mutation, nil
}

func boolMapFromRaw(raw json.RawMessage) (map[string]bool, error) {
	var values map[string]bool
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}

	return values, nil
}

func boolPatchValue(raw json.RawMessage) (bool, error) {
	if string(raw) == "null" {
		return false, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, err
	}

	return value, nil
}

func emptyMapAsNil(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}

	return values
}

func emptyJMAPErrorMapAsNil(values map[string]jmapError) map[string]jmapError {
	if len(values) == 0 {
		return nil
	}

	return values
}

func downloadFilename(value string) string {
	filename := strings.TrimSpace(value)
	filename = strings.ReplaceAll(filename, "/", "_")
	filename = strings.ReplaceAll(filename, "\\", "_")

	return filename
}

func searchSnippetFilterText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}

	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		return ""
	}

	return firstFilterText(node)
}

func firstFilterText(node any) string {
	switch value := node.(type) {
	case map[string]any:
		if text, ok := value["text"].(string); ok {
			return strings.TrimSpace(text)
		}
		if conditions, ok := value["conditions"].([]any); ok {
			for _, condition := range conditions {
				if text := firstFilterText(condition); text != "" {
					return text
				}
			}
		}
	case []any:
		for _, item := range value {
			if text := firstFilterText(item); text != "" {
				return text
			}
		}
	}

	return ""
}

func markedSnippet(value, filterText string, limit int) *string {
	collapsed := collapseWhitespace(value)
	if collapsed == "" {
		return nil
	}
	if filterText == "" {
		snippet := trimOctets(html.EscapeString(collapsed), limit)

		return &snippet
	}

	location := strings.Index(
		strings.ToLower(collapsed),
		strings.ToLower(filterText),
	)
	if location < 0 {
		return nil
	}
	start := max(0, location-80)
	end := min(len(collapsed), location+len(filterText)+120)
	prefix := ""
	suffix := ""
	if start > 0 {
		prefix = "..."
	}
	if end < len(collapsed) {
		suffix = "..."
	}
	window := collapsed[start:end]
	relative := location - start
	marked := prefix +
		html.EscapeString(window[:relative]) +
		"<mark>" +
		html.EscapeString(window[relative:relative+len(filterText)]) +
		"</mark>" +
		html.EscapeString(window[relative+len(filterText):]) +
		suffix
	snippet := trimOctets(marked, limit)

	return &snippet
}

func trimOctets(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}

	return value[:limit-3] + "..."
}

func collapseWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
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

func writeJSONStatus(writer http.ResponseWriter, status int, output any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
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
