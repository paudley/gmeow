// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestRESTMailSearchUsesAppServices(t *testing.T) {
	services := testServices(t, "hello rest mail")
	server := httptest.NewServer(NewHandler(services))
	defer server.Close()

	body := bytes.NewBufferString(`{"query":"rest","limit":5}`)
	response, err := http.Post(server.URL+"/v1/mail_search", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %s", response.Status)
	}

	var decoded appsvc.ObjectSearchResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Total != 1 || decoded.Results[0].Facets[0] != appsvc.MailMessageFacet {
		t.Fatalf("unexpected REST search response: %#v", decoded)
	}
}

func TestRESTMetricsExposesPrometheusText(t *testing.T) {
	services := testServices(t, "metrics mail")
	server := httptest.NewServer(NewHandler(services))
	defer server.Close()

	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %s", response.Status)
	}
	if contentType := response.Header.Get(
		"Content-Type",
	); !strings.Contains(
		contentType,
		"text/plain",
	) {
		t.Fatalf("unexpected content type: %s", contentType)
	}
}

func testServices(t *testing.T, text string) *appsvc.Services {
	t.Helper()
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	t.Cleanup(filestoreService.Close)
	queryService := testsupport.StartQueryGRPC(t, ctx, filestoreService.Store)
	t.Cleanup(queryService.Close)
	digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader(text),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: appsvc.MailMessageFacet}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testsupport.CleanupQueryObjects(t, digest) })
	manifest, err := filestoreService.Client.ReadManifest(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Titles = []contracts.Title{{Value: text}}
	if err := queryService.Client.Project(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}

	services, err := appsvc.New(appsvc.Options{
		Query:   queryService.Client,
		Objects: filestoreService.Client,
	})
	if err != nil {
		t.Fatal(err)
	}

	return services
}
