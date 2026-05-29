// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package jmap

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeTokenValidator struct {
	wantToken string
	clientID  string
	err       error
}

func (validator fakeTokenValidator) ValidateBearerToken(
	_ context.Context,
	token string,
) (string, bool, error) {
	if validator.err != nil {
		return "", false, validator.err
	}
	if token != validator.wantToken {
		return "", false, nil
	}

	return validator.clientID, true, nil
}

func TestJMAPValidatorAuthorizesBearerToken(t *testing.T) {
	validator := fakeTokenValidator{wantToken: "good-token", clientID: "client-1"}
	server := httptest.NewServer(NewHandler(nil, Options{Validator: validator}))
	defer server.Close()

	cases := map[string]struct {
		token string
		want  int
	}{
		"valid":   {token: "good-token", want: http.StatusOK},
		"invalid": {token: "bad-token", want: http.StatusUnauthorized},
		"missing": {token: "", want: http.StatusUnauthorized},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, server.URL+"/jmap/session", nil)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.token != "" {
				request.Header.Set("Authorization", "Bearer "+testCase.token)
			}

			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()

			if response.StatusCode != testCase.want {
				t.Fatalf("status = %s, want %d", response.Status, testCase.want)
			}
		})
	}
}

func TestJMAPValidatorFailsClosedOnError(t *testing.T) {
	validator := fakeTokenValidator{err: errors.New("query unavailable")}
	server := httptest.NewServer(NewHandler(nil, Options{Validator: validator}))
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/jmap/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer anything")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %s, want 401", response.Status)
	}
}
