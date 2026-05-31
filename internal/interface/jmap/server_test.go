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
	"time"
	"unicode/utf8"

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

func TestJMAPMailboxSetCreatesCustomMailboxAndPersistsCatalog(t *testing.T) {
	var cursor contracts.SourceCursor
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			mailboxes: []contracts.JMAPMailbox{{
				MailboxID: "inbox",
				Name:      "Inbox",
				Role:      "inbox",
				SortOrder: 10,
				IsSystem:  true,
			}},
		},
		Objects: objectReaderFixture{sourceCursor: &cursor},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:mail"],
		"methodCalls":[
			["Mailbox/set",{"accountId":"gmeow","create":{
				"c1":{"name":"Research","parentId":"inbox","sortOrder":70}
			}},"m1"]
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
	var setResponse mailboxSetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &setResponse); err != nil {
		t.Fatal(err)
	}
	created := setResponse.Created["c1"]
	if name != "Mailbox/set" ||
		created.Name != "Research" ||
		created.ParentID == nil ||
		*created.ParentID != "inbox" ||
		len(setResponse.NotCreated) != 0 {
		t.Fatalf("unexpected Mailbox/set response name=%q args=%#v", name, setResponse)
	}
	if cursor.SourceKind != contracts.JMAPMailboxCatalogSourceKind ||
		cursor.SourceName != contracts.JMAPMailboxCatalogSourceName {
		t.Fatalf("JMAP mailbox catalog was not persisted: %#v", cursor)
	}
	catalog, ok := cursor.Cursor[contracts.JMAPMailboxCatalogCursorKey].([]contracts.JMAPMailbox)
	if !ok || len(catalog) != 1 ||
		catalog[0].Name != "Research" ||
		catalog[0].ParentID != "inbox" {
		t.Fatalf("unexpected persisted mailbox catalog: %#v", cursor.Cursor)
	}
}

func TestJMAPMailboxSetRejectsParentCycle(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			mailboxes: []contracts.JMAPMailbox{
				{
					MailboxID:   "parent",
					Name:        "Parent",
					SortOrder:   70,
					IsSystem:    false,
					IsDestroyed: false,
				},
				{
					MailboxID:   "child",
					Name:        "Child",
					ParentID:    "parent",
					SortOrder:   80,
					IsSystem:    false,
					IsDestroyed: false,
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
			["Mailbox/set",{"accountId":"gmeow","update":{
				"parent":{"parentId":"child"}
			}},"m1"]
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
	var setResponse mailboxSetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &setResponse); err != nil {
		t.Fatal(err)
	}
	if name != "Mailbox/set" ||
		len(setResponse.Updated) != 0 ||
		setResponse.NotUpdated["parent"].Type != "invalidPatch" {
		t.Fatalf("unexpected Mailbox/set response name=%q args=%#v", name, setResponse)
	}
}

func TestJMAPMailboxSetRejectsDestroyWhenMailboxContainsEmail(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			mailboxes: []contracts.JMAPMailbox{{
				MailboxID:   "active",
				Name:        "Active",
				SortOrder:   70,
				IsSystem:    false,
				IsDestroyed: false,
			}},
			mailboxEmailCounts: map[string]int{"active": 1},
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
			["Mailbox/set",{"accountId":"gmeow","destroy":["active"]},"m1"]
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
	var setResponse mailboxSetResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &setResponse); err != nil {
		t.Fatal(err)
	}
	if name != "Mailbox/set" ||
		len(setResponse.Destroyed) != 0 ||
		setResponse.NotDestroyed["active"].Type != "invalidArguments" {
		t.Fatalf("unexpected Mailbox/set response name=%q args=%#v", name, setResponse)
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
		"methodCalls":[["Email/query",{"accountId":"gmeow","position":2,"limit":7,"filter":{"text":"apollo","inMailbox":"inbox","hasKeyword":"$flagged","notKeyword":"$seen","after":"2026-05-01T00:00:00Z","before":"2026-06-01T00:00:00Z"}},"e1"]]
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
		!captured.After.Equal(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)) ||
		!captured.Before.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) ||
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

func TestJMAPBlobLookupReturnsMatchedIDs(t *testing.T) {
	bodyDigest := contracts.ObjectDigest("digest-body")
	messageDigest := contracts.ObjectDigest("digest-message")
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			blobLookup: contracts.JMAPBlobLookupResponse{
				Blobs: map[contracts.ObjectDigest]contracts.JMAPBlobReferences{
					bodyDigest: {
						EmailIDs:   []contracts.ObjectDigest{messageDigest},
						ThreadIDs:  []string{"thread-1"},
						MailboxIDs: []string{"inbox"},
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
		"using":["urn:ietf:params:jmap:blob","urn:ietf:params:jmap:mail"],
		"methodCalls":[["Blob/lookup",{"accountId":"gmeow","typeNames":["Email","Thread","Mailbox"],"ids":["digest-body","missing"]},"b1"]]
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
	var lookup blobLookupResponse
	if err := json.Unmarshal(tuple[0], &name); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(tuple[1], &lookup); err != nil {
		t.Fatal(err)
	}
	if name != "Blob/lookup" ||
		len(lookup.List) != 2 ||
		lookup.List[0].ID != "digest-body" ||
		lookup.List[0].MatchedIDs["Email"][0] != "digest-message" ||
		lookup.List[0].MatchedIDs["Thread"][0] != "thread-1" ||
		lookup.List[0].MatchedIDs["Mailbox"][0] != "inbox" ||
		len(lookup.List[1].MatchedIDs["Email"]) != 0 ||
		len(lookup.List[1].MatchedIDs["Thread"]) != 0 ||
		len(lookup.List[1].MatchedIDs["Mailbox"]) != 0 {
		t.Fatalf("unexpected blob lookup response name=%q args=%#v", name, lookup)
	}
}

func TestJMAPBlobLookupRejectsUnknownDataType(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query:   jmapQueryFixture{},
		Objects: objectReaderFixture{},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(services, Options{BearerToken: "secret"}))
	defer server.Close()
	body := bytes.NewBufferString(`{
		"using":["urn:ietf:params:jmap:blob"],
		"methodCalls":[["Blob/lookup",{"accountId":"gmeow","typeNames":["CalendarEvent"],"ids":["digest-body"]},"b1"]]
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
	if name != "error" || problem.Type != "unknownDataType" {
		t.Fatalf("unexpected blob lookup error name=%q problem=%#v", name, problem)
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

func TestJMAPSearchSnippetHandlesUTF8Matches(t *testing.T) {
	snippet := markedSnippet(
		"Résumé status includes München and 東京 updates",
		"münchen",
		200,
	)
	if snippet == nil {
		t.Fatal("expected snippet")
	}
	if !strings.Contains(*snippet, "<mark>München</mark>") {
		t.Fatalf("unexpected snippet: %s", *snippet)
	}
	if !utf8.ValidString(*snippet) {
		t.Fatalf("snippet is not valid UTF-8: %q", *snippet)
	}
}

func TestJMAPAddressListParsesMultipleRecipients(t *testing.T) {
	values := addressList("Alice <alice@example.com>, Bob <bob@example.com>")
	if len(values) != 2 ||
		values[0].Name != "Alice" ||
		values[0].Email != "alice@example.com" ||
		values[1].Name != "Bob" ||
		values[1].Email != "bob@example.com" {
		t.Fatalf("unexpected address list: %#v", values)
	}
}

func TestJMAPQuotaReadMethodsReturnUsageState(t *testing.T) {
	services, err := appsvc.New(appsvc.Options{
		Query: jmapQueryFixture{
			breakdown: contracts.ObjectBreakdown{
				TotalObjects:   4,
				TotalSizeBytes: 2048,
				ByFacet: []contracts.BreakdownCount{{
					Label: appsvc.MailMessageFacet,
					Count: 2,
				}},
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
		"using":["urn:ietf:params:jmap:quota"],
		"methodCalls":[
			["Quota/get",{"accountId":"gmeow","ids":null},"qg"],
			["Quota/query",{"accountId":"gmeow","position":3},"qq"],
			["Quota/changes",{"accountId":"gmeow","sinceState":"0"},"qc"],
			["Quota/queryChanges",{"accountId":"gmeow","sinceQueryState":"0"},"qqc"]
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
	if len(decoded.MethodResponses) != 4 {
		t.Fatalf("method response count = %d", len(decoded.MethodResponses))
	}

	var getTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[0], &getTuple); err != nil {
		t.Fatal(err)
	}
	var getName string
	var getResponse quotaGetResponse
	if err := json.Unmarshal(getTuple[0], &getName); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(getTuple[1], &getResponse); err != nil {
		t.Fatal(err)
	}
	if getName != "Quota/get" || len(getResponse.List) != 3 ||
		len(getResponse.NotFound) != 0 {
		t.Fatalf("unexpected quota get response name=%q args=%#v", getName, getResponse)
	}
	if !hasQuotaUsage(getResponse.List, "filestore-bytes", 2048) ||
		!hasQuotaUsage(getResponse.List, "messages", 2) ||
		!hasQuotaUsage(getResponse.List, "objects", 4) {
		t.Fatalf("quota usage missing from %#v", getResponse.List)
	}

	var queryTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[1], &queryTuple); err != nil {
		t.Fatal(err)
	}
	var queryResponse quotaQueryResponse
	if err := json.Unmarshal(queryTuple[1], &queryResponse); err != nil {
		t.Fatal(err)
	}
	if queryResponse.Position != 3 || queryResponse.Total != 3 ||
		len(queryResponse.IDs) != 3 {
		t.Fatalf("unexpected quota query response: %#v", queryResponse)
	}

	var changesTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[2], &changesTuple); err != nil {
		t.Fatal(err)
	}
	var changes quotaChangesResponse
	if err := json.Unmarshal(changesTuple[1], &changes); err != nil {
		t.Fatal(err)
	}
	if changes.NewState != "0" || changes.HasMoreChanges ||
		len(changes.Created) != 0 ||
		len(changes.Updated) != 0 ||
		len(changes.Destroyed) != 0 {
		t.Fatalf("unexpected quota changes response: %#v", changes)
	}

	var queryChangesTuple []json.RawMessage
	if err := json.Unmarshal(decoded.MethodResponses[3], &queryChangesTuple); err != nil {
		t.Fatal(err)
	}
	var queryChanges quotaQueryChangesResponse
	if err := json.Unmarshal(queryChangesTuple[1], &queryChanges); err != nil {
		t.Fatal(err)
	}
	if queryChanges.NewQueryState != "0" || queryChanges.Total != 0 ||
		len(queryChanges.Removed) != 0 ||
		len(queryChanges.Added) != 0 {
		t.Fatalf("unexpected quota query changes response: %#v", queryChanges)
	}
}

type jmapQueryFixture struct {
	mailboxes          []contracts.JMAPMailbox
	states             map[contracts.ObjectDigest]contracts.JMAPEmailState
	threads            map[string]contracts.JMAPThread
	search             contracts.SearchResponse
	emailQuery         contracts.JMAPEmailQueryResponse
	blobLookup         contracts.JMAPBlobLookupResponse
	breakdown          contracts.ObjectBreakdown
	mailboxEmailCounts map[string]int
	requested          *contracts.JMAPEmailQueryRequest
}

func (query jmapQueryFixture) Search(
	context.Context,
	contracts.SearchRequest,
) (contracts.SearchResponse, error) {
	return query.search, nil
}

func (query jmapQueryFixture) ObjectBreakdown(
	context.Context,
) (contracts.ObjectBreakdown, error) {
	return query.breakdown, nil
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

func (query jmapQueryFixture) JMAPBlobLookup(
	context.Context,
	contracts.JMAPBlobLookupRequest,
) (contracts.JMAPBlobLookupResponse, error) {
	return query.blobLookup, nil
}

func (query jmapQueryFixture) UpdateJMAPMailboxCatalog(
	context.Context,
	contracts.JMAPMailboxCatalogUpdate,
) ([]contracts.JMAPMailbox, error) {
	return query.mailboxes, nil
}

func (query jmapQueryFixture) JMAPMailboxEmailCounts(
	context.Context,
	contracts.JMAPMailboxEmailCountRequest,
) (contracts.JMAPMailboxEmailCountResponse, error) {
	return contracts.JMAPMailboxEmailCountResponse{
		Counts: query.mailboxEmailCounts,
	}, nil
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

func (jmapQueryFixture) RelatedObjects(
	context.Context,
	contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	return contracts.RelatedObjectsResponse{}, nil
}

type objectReaderFixture struct {
	manifests    map[contracts.ObjectDigest]contracts.Manifest
	content      map[contracts.ObjectDigest]string
	sourceCursor *contracts.SourceCursor
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

func (objects objectReaderFixture) WriteSourceCursor(
	_ context.Context,
	cursor contracts.SourceCursor,
) error {
	if objects.sourceCursor != nil {
		*objects.sourceCursor = cursor
	}

	return nil
}

func hasQuotaUsage(quotas []jmapQuota, id string, used uint64) bool {
	for _, quota := range quotas {
		if quota.ID == id && quota.Used == used {
			return true
		}
	}

	return false
}
