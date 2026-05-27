// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package jmap

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
		"methodCalls":[["Email/query",{},"c1"]]
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
