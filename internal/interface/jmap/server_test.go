// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package jmap

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
)

func TestJMAPSessionRequiresBearerToken(t *testing.T) {
	server := httptest.NewServer(NewHandler(nil, Options{BearerToken: "secret"}))
	defer server.Close()

	response, err := http.Get(server.URL + "/jmap/session")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %s, want 401", response.Status)
	}
	if response.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate header")
	}
}

func TestJMAPSessionAdvertisesInitialCapabilities(t *testing.T) {
	server := httptest.NewServer(NewHandler(nil, Options{BearerToken: "secret"}))
	defer server.Close()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/jmap/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %s, want 200", response.Status)
	}
	var session sessionResource
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	for _, capability := range []string{
		capabilityCore,
		capabilityMail,
		capabilityBlob,
		capabilityQuota,
	} {
		if _, ok := session.Capabilities[capability]; !ok {
			t.Fatalf("capability %q missing from %#v", capability, session.Capabilities)
		}
		if session.PrimaryAccounts[capability] != "gmeow" && capability != capabilityCore {
			t.Fatalf(
				"primary account for %q = %q",
				capability,
				session.PrimaryAccounts[capability],
			)
		}
	}
	if session.APIURL != server.URL+"/jmap/api" {
		t.Fatalf("apiUrl = %q", session.APIURL)
	}
}

func TestJMAPCoreEchoUsesMethodCallTuples(t *testing.T) {
	server := httptest.NewServer(NewHandler(nil, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:core"],
		"methodCalls":[["Core/echo",{"hello":"world"},"c1"]]
	}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/jmap/api", body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %s, want 200", response.Status)
	}
	var decoded struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
		SessionState    string            `json:"sessionState"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SessionState != "0" || len(decoded.MethodResponses) != 1 {
		t.Fatalf("unexpected response envelope: %#v", decoded)
	}
	var tuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &tuple); err != nil {
		t.Fatal(err)
	}
	if len(tuple) != 3 {
		t.Fatalf("method response tuple = %#v", tuple)
	}
	var name, clientID string
	var arguments map[string]string
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &arguments); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[2], &clientID); err != nil {
		t.Fatal(err)
	}
	if name != "Core/echo" || clientID != "c1" || arguments["hello"] != "world" {
		t.Fatalf(
			"unexpected method response name=%q clientID=%q args=%#v",
			name,
			clientID,
			arguments,
		)
	}
}

func TestJMAPUnknownMethodReturnsJMAPError(t *testing.T) {
	server := httptest.NewServer(NewHandler(nil, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:mail"],
		"methodCalls":[["Thread/get",{},"c1"]]
	}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/jmap/api", body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	var decoded struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	var tuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &tuple); err != nil {
		t.Fatal(err)
	}
	var name string
	var problem jmapError
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &problem); err != nil {
		t.Fatal(err)
	}
	if name != "error" || problem.Type != "unknownMethod" {
		t.Fatalf("unexpected error response name=%q problem=%#v", name, problem)
	}
}

func TestJMAPMailboxGetAndEmailQueryUseAppServices(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			mailboxes: []contracts.JMAPMailbox{{
				MailboxID: "inbox",
				Name:      "Inbox",
				Role:      "inbox",
				SortOrder: 10,
				IsSystem:  true,
			}},
			search: contracts.SearchResponse{
				Results: []contracts.SearchResult{{
					ObjectDigest: contracts.ObjectDigest("digest-1"),
				}},
				Total: 1,
			},
		},
		Objects: objectReaderFixture{},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:mail"],
		"methodCalls":[
			["Mailbox/get",{"accountId":"gmeow"},"m1"],
			["Email/query",{"accountId":"gmeow","limit":5},"e1"]
		]
	}`)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/jmap/api", body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	var decoded struct {
		MethodResponses []json.RawMessage `json:"methodResponses"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.MethodResponses) != 2 {
		t.Fatalf("method response count = %d", len(decoded.MethodResponses))
	}
	var mailboxTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &mailboxTuple); err != nil {
		t.Fatal(err)
	}
	var mailboxName string
	var mailboxes mailboxGetResponse
	if err := json.Unmarshal(mailboxTuple[0], &mailboxName); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(mailboxTuple[1], &mailboxes); err != nil {
		t.Fatal(err)
	}
	if mailboxName != "Mailbox/get" || len(mailboxes.List) != 1 ||
		mailboxes.List[0].ID != "inbox" {
		t.Fatalf("unexpected mailbox response name=%q args=%#v", mailboxName, mailboxes)
	}

	var emailTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[1], &emailTuple); err != nil {
		t.Fatal(err)
	}
	var emailName string
	var emails emailQueryResponse
	if err := json.Unmarshal(emailTuple[0], &emailName); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(emailTuple[1], &emails); err != nil {
		t.Fatal(err)
	}
	if emailName != "Email/query" || len(emails.IDs) != 1 ||
		emails.IDs[0] != "digest-1" || emails.Total != 1 {
		t.Fatalf("unexpected email query response name=%q args=%#v", emailName, emails)
	}
}

type jmapQueryFixture struct {
	mailboxes []contracts.JMAPMailbox
	states    map[contracts.ObjectDigest]contracts.JMAPEmailState
	search    contracts.SearchResponse
}

func (query jmapQueryFixture) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	return query.search, nil
}

func (query jmapQueryFixture) JMAPMailboxes(
	context.Context,
) ([]contracts.JMAPMailbox, error) {
	return query.mailboxes, nil
}

func (query jmapQueryFixture) JMAPEmailStates(
	context.Context,
	[]contracts.ObjectDigest,
) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error) {
	return query.states, nil
}

func (jmapQueryFixture) Structure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (jmapQueryFixture) Relationships(
	context.Context,
	contracts.RelationshipRequest,
) (contracts.RelationshipResponse, error) {
	return contracts.RelationshipResponse{}, nil
}

func (jmapQueryFixture) Graph(
	context.Context,
	contracts.GraphRequest,
) (contracts.GraphResponse, error) {
	return contracts.GraphResponse{}, nil
}

func (jmapQueryFixture) AnalysisStatus(
	context.Context,
	contracts.AnalysisStatusRequest,
) (contracts.AnalysisStatusResponse, error) {
	return contracts.AnalysisStatusResponse{}, nil
}

func (jmapQueryFixture) SourceCursors(
	context.Context,
	contracts.SourceCursorRequest,
) (contracts.SourceCursorResponse, error) {
	return contracts.SourceCursorResponse{}, nil
}

type objectReaderFixture struct{}

func (objectReaderFixture) ReadManifest(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Manifest, error) {
	return contracts.Manifest{}, nil
}

func (objectReaderFixture) GetStructure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (objectReaderFixture) Open(
	context.Context,
	contracts.ObjectDigest,
) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
