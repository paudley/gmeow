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
	"os"
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
		"methodCalls":[["Email/changes",{},"c1"]]
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
			emailQuery: contracts.JMAPEmailQueryResponse{
				IDs: []contracts.ObjectDigest{
					contracts.ObjectDigest("digest-1"),
				},
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
			["Mailbox/query",{"accountId":"gmeow"},"q1"],
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
	if len(decoded.MethodResponses) != 3 {
		t.Fatalf("method response count = %d", len(decoded.MethodResponses))
	}
	var mailboxQueryTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &mailboxQueryTuple); err != nil {
		t.Fatal(err)
	}
	var mailboxQueryName string
	var mailboxQuery mailboxQueryResponse
	if err := json.Unmarshal(mailboxQueryTuple[0], &mailboxQueryName); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(mailboxQueryTuple[1], &mailboxQuery); err != nil {
		t.Fatal(err)
	}
	if mailboxQueryName != "Mailbox/query" || len(mailboxQuery.IDs) != 1 ||
		mailboxQuery.IDs[0] != "inbox" {
		t.Fatalf(
			"unexpected mailbox query response name=%q args=%#v",
			mailboxQueryName,
			mailboxQuery,
		)
	}

	var mailboxTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[1], &mailboxTuple); err != nil {
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
	if err := json.Unmarshal(decoded.MethodResponses[2], &emailTuple); err != nil {
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

func TestJMAPEmailSetUpdatesKeywordsAndMailboxes(t *testing.T) {
	digest := contracts.ObjectDigest("digest-1")
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			states: map[contracts.ObjectDigest]contracts.JMAPEmailState{
				digest: {
					ObjectDigest: digest,
					MailboxIDs:   []string{"all", "inbox"},
					Keywords:     []string{"$seen"},
				},
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
			["Email/set",{"accountId":"gmeow","update":{
				"digest-1":{"keywords/$seen":null,"keywords/$flagged":true,"mailboxIds/archive":true}
			}},"s1"]
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
	var tuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &tuple); err != nil {
		t.Fatal(err)
	}
	var name string
	var setResponse emailSetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &setResponse); err != nil {
		t.Fatal(err)
	}
	if name != "Email/set" || len(setResponse.Updated) != 1 ||
		len(setResponse.NotUpdated) != 0 {
		t.Fatalf("unexpected Email/set response name=%q args=%#v", name, setResponse)
	}
}

func TestJMAPEmailQueryPassesFilterToAppServices(t *testing.T) {
	var captured contracts.JMAPEmailQueryRequest
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			emailQuery: contracts.JMAPEmailQueryResponse{
				IDs: []contracts.ObjectDigest{contracts.ObjectDigest("digest-2")},
			},
			requested: &captured,
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
		"methodCalls":[["Email/query",{"accountId":"gmeow","position":2,"limit":7,"filter":{"text":"apollo","inMailbox":"inbox","hasKeyword":"$flagged","notKeyword":"$seen"}},"e1"]]
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
	var output emailQueryResponse
	if err := json.Unmarshal(tuple[1], &output); err != nil {
		t.Fatal(err)
	}
	if len(output.IDs) != 1 || output.IDs[0] != "digest-2" {
		t.Fatalf("unexpected email query output: %#v", output)
	}
	if captured.Text != "apollo" ||
		captured.InMailbox != "inbox" ||
		captured.HasKeyword != "$flagged" ||
		captured.NotKeyword != "$seen" ||
		captured.Offset != 2 ||
		captured.Limit != 7 {
		t.Fatalf("unexpected captured query: %#v", captured)
	}
}

func TestJMAPThreadGetReturnsEmailIDs(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			threads: map[string]contracts.JMAPThread{
				"thread-1": {
					ID: "thread-1",
					EmailIDs: []contracts.ObjectDigest{
						contracts.ObjectDigest("digest-1"),
						contracts.ObjectDigest("digest-2"),
					},
				},
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
		"methodCalls":[["Thread/get",{"accountId":"gmeow","ids":["thread-1","missing"]},"t1"]]
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
	var threads threadGetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &threads); err != nil {
		t.Fatal(err)
	}
	if name != "Thread/get" ||
		len(threads.List) != 1 ||
		threads.List[0].ID != "thread-1" ||
		len(threads.List[0].EmailIDs) != 2 ||
		threads.List[0].EmailIDs[0] != "digest-1" ||
		len(threads.NotFound) != 1 ||
		threads.NotFound[0] != "missing" {
		t.Fatalf("unexpected thread response name=%q args=%#v", name, threads)
	}
}

func TestJMAPBlobGetReturnsMetadata(t *testing.T) {
	digest := contracts.ObjectDigest("digest-1")
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{},
		Objects: objectReaderFixture{
			manifests: map[contracts.ObjectDigest]contracts.Manifest{
				digest: {
					ObjectDigest: digest,
					MediaType:    "message/rfc822",
					Size:         42,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:blob"],
		"methodCalls":[["Blob/get",{"accountId":"gmeow","ids":["digest-1","missing"]},"b1"]]
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
	var blobs blobGetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &blobs); err != nil {
		t.Fatal(err)
	}
	if name != "Blob/get" ||
		len(blobs.List) != 1 ||
		blobs.List[0].ID != "digest-1" ||
		blobs.List[0].Type != "message/rfc822" ||
		blobs.List[0].Size != 42 ||
		len(blobs.NotFound) != 1 ||
		blobs.NotFound[0] != "missing" {
		t.Fatalf("unexpected blob response name=%q args=%#v", name, blobs)
	}
}

func TestJMAPDownloadStreamsBlob(t *testing.T) {
	digest := contracts.ObjectDigest("digest-1")
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{},
		Objects: objectReaderFixture{
			manifests: map[contracts.ObjectDigest]contracts.Manifest{
				digest: {
					ObjectDigest: digest,
					MediaType:    "message/rfc822",
					Size:         int64(len("raw-message")),
				},
			},
			content: map[contracts.ObjectDigest]string{
				digest: "raw-message",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	request, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/jmap/download/gmeow/digest-1/message.eml",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %s, want 200", response.Status)
	}
	if response.Header.Get("Content-Type") != "message/rfc822" {
		t.Fatalf("content type = %q", response.Header.Get("Content-Type"))
	}
	if !strings.Contains(response.Header.Get("Content-Disposition"), "message.eml") {
		t.Fatalf(
			"content disposition = %q",
			response.Header.Get("Content-Disposition"),
		)
	}
	if string(body) != "raw-message" {
		t.Fatalf("body = %q", body)
	}
}

func TestJMAPSearchSnippetGetMarksMatchingText(t *testing.T) {
	messageDigest := contracts.ObjectDigest("digest-message")
	headerDigest := contracts.ObjectDigest("digest-headers")
	bodyDigest := contracts.ObjectDigest("digest-body")
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{},
		Objects: objectReaderFixture{
			manifests: map[contracts.ObjectDigest]contracts.Manifest{
				messageDigest: {
					ObjectDigest: messageDigest,
					Facets: []contracts.Facet{{
						Kind: appsvc.MailMessageFacet,
					}},
					Compound: contracts.Compound{
						IsCompound: true,
						Parts: []contracts.CompoundPart{{
							Digest: headerDigest,
							Role:   "rfc822_headers",
						}, {
							Digest: bodyDigest,
							Role:   "email_body",
						}},
					},
				},
				headerDigest: {ObjectDigest: headerDigest, MediaType: "application/json"},
				bodyDigest:   {ObjectDigest: bodyDigest, MediaType: "text/plain"},
			},
			content: map[contracts.ObjectDigest]string{
				headerDigest: `[{"name":"Subject","value":"Quarterly Apollo update"}]`,
				bodyDigest:   `The Apollo program has a launch window next month.`,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:mail"],
		"methodCalls":[["SearchSnippet/get",{"accountId":"gmeow","emailIds":["digest-message","missing"],"filter":{"text":"Apollo"}},"sn1"]]
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
	var snippets searchSnippetGetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &snippets); err != nil {
		t.Fatal(err)
	}
	if name != "SearchSnippet/get" ||
		len(snippets.List) != 1 ||
		snippets.List[0].EmailID != "digest-message" ||
		snippets.List[0].Subject == nil ||
		!strings.Contains(*snippets.List[0].Subject, "<mark>Apollo</mark>") ||
		snippets.List[0].Preview == nil ||
		!strings.Contains(*snippets.List[0].Preview, "<mark>Apollo</mark>") ||
		len(snippets.NotFound) != 1 ||
		snippets.NotFound[0] != "missing" {
		t.Fatalf("unexpected snippet response name=%q args=%#v", name, snippets)
	}
}

type jmapQueryFixture struct {
	mailboxes  []contracts.JMAPMailbox
	states     map[contracts.ObjectDigest]contracts.JMAPEmailState
	threads    map[string]contracts.JMAPThread
	search     contracts.SearchResponse
	emailQuery contracts.JMAPEmailQueryResponse
	requested  *contracts.JMAPEmailQueryRequest
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

func (query jmapQueryFixture) JMAPEmailQuery(
	_ context.Context,
	request contracts.JMAPEmailQueryRequest,
) (contracts.JMAPEmailQueryResponse, error) {
	if query.requested != nil {
		*query.requested = request
	}
	return query.emailQuery, nil
}

func (query jmapQueryFixture) JMAPThreads(
	context.Context,
	[]string,
) (map[string]contracts.JMAPThread, error) {
	return query.threads, nil
}

func (query jmapQueryFixture) UpdateJMAPEmailState(
	_ context.Context,
	update contracts.JMAPEmailStateUpdate,
) (contracts.JMAPEmailState, error) {
	return contracts.JMAPEmailState{
		ObjectDigest: update.ObjectDigest,
		MailboxIDs:   append([]string{}, update.MailboxIDs...),
		Keywords:     append([]string{}, update.Keywords...),
	}, nil
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

type objectReaderFixture struct {
	manifests map[contracts.ObjectDigest]contracts.Manifest
	content   map[contracts.ObjectDigest]string
}

func (objects objectReaderFixture) ReadManifest(
	_ context.Context,
	digest contracts.ObjectDigest,
) (contracts.Manifest, error) {
	if objects.manifests != nil {
		manifest, ok := objects.manifests[digest]
		if !ok {
			return contracts.Manifest{}, os.ErrNotExist
		}

		return manifest, nil
	}

	return contracts.Manifest{}, nil
}

func (objectReaderFixture) GetStructure(
	context.Context,
	contracts.ObjectDigest,
) (contracts.Structure, error) {
	return contracts.Structure{}, nil
}

func (objects objectReaderFixture) Open(
	_ context.Context,
	digest contracts.ObjectDigest,
) (io.ReadCloser, error) {
	if objects.content != nil {
		content, ok := objects.content[digest]
		if !ok {
			return nil, os.ErrNotExist
		}

		return io.NopCloser(strings.NewReader(content)), nil
	}

	return io.NopCloser(strings.NewReader("")), nil
}

func (objectReaderFixture) WriteOverlays(
	context.Context,
	contracts.ObjectDigest,
	map[string]any,
) error {
	return nil
}
