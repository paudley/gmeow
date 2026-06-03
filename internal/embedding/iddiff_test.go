// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// unit returns a one-hot unit vector at index i; identical i → cosine 1, distinct
// i → cosine 0. It lets idDiff unit tests construct value-match / mismatch
// directly without the embedder.
func unit(i int) Vector {
	v := make(Vector, FullDim)
	v[i] = 1

	return v
}

func interval(first, last string) Interval {
	parse := func(s string) time.Time {
		if s == "" {
			return time.Time{}
		}
		t, _ := time.Parse("2006-01-02", s)

		return t
	}

	return Interval{First: parse(first), Last: parse(last)}
}

// corpusWeight mimics ω at corpus scale (rare values, many entities) so the
// functional veto can fire; idf is then large.
func corpusWeight(c scoredClaim) float64 { return omega(c.Kind, 1, 5000) }

func TestIdDiffCanonicalCases(t *testing.T) {
	params := idDiffParams{
		TauSet: 0.9, TauFunc: 0.9, TauCtx: 0.6,
		Lambda: 1.5, SetPenalty: 0.5, VetoMass: 4.0,
		ObservationMode: false, W: corpusWeight,
	}

	name := func(value string, vec int) scoredClaim {
		return scoredClaim{Attr: "name", Value: value, Kind: KindFunctional, Vec: unit(vec)}
	}
	email := func(value string, vec int) scoredClaim {
		return scoredClaim{Attr: "email", Value: value, Kind: KindSet, Vec: unit(vec)}
	}

	tests := []struct {
		name       string
		a, b       []scoredClaim
		wantMerge  bool // score > 0 and no veto
		wantVeto   bool
		wantNotPos bool // score <= 0
	}{
		{
			name:      "identical name+email -> strong positive",
			a:         []scoredClaim{name("ada lovelace", 1), email("ada@x.example", 2)},
			b:         []scoredClaim{name("ada lovelace", 1), email("ada@x.example", 2)},
			wantMerge: true,
		},
		{
			name:       "two populated disjoint email sets -> negative (set identity)",
			a:          []scoredClaim{email("a1@x", 10), email("a2@x", 11)},
			b:          []scoredClaim{email("b1@y", 12), email("b2@y", 13)},
			wantNotPos: true,
		},
		{
			name: "shared value but disjoint validity -> no correlation",
			a: []scoredClaim{
				{
					Attr:  "name",
					Value: "n",
					Kind:  KindFunctional,
					Vec:   unit(20),
					Valid: interval("2000-01-01", "2005-01-01"),
				},
			},
			b: []scoredClaim{
				{
					Attr:  "name",
					Value: "n",
					Kind:  KindFunctional,
					Vec:   unit(20),
					Valid: interval("2010-01-01", "2015-01-01"),
				},
			},
			wantNotPos: true,
		},
		{
			name:       "name change, no supersedes -> contradiction veto (separate)",
			a:          []scoredClaim{name("paul x", 30), email("shared@x.example", 31)},
			b:          []scoredClaim{name("lindsey y", 32), email("shared@x.example", 31)},
			wantVeto:   true,
			wantNotPos: true,
		},
		{
			name: "name change WITH supersedes -> evolution, merges on shared context",
			a: []scoredClaim{
				{
					Attr:       "name",
					Value:      "lindsey y",
					Kind:       KindFunctional,
					Vec:        unit(32),
					ULID:       "L",
					Supersedes: []string{"P"},
				},
				email("shared@x.example", 31),
			},
			b: []scoredClaim{
				{Attr: "name", Value: "paul x", Kind: KindFunctional, Vec: unit(30), ULID: "P"},
				email("shared@x.example", 31),
			},
			wantMerge: true,
		},
		{
			name:       "strangers, disjoint sparse cards -> neutral (not merged)",
			a:          []scoredClaim{email("alice@a", 40)},
			b:          []scoredClaim{email("bob@b", 41)},
			wantNotPos: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := idDiff(tc.a, tc.b, params)
			if got.Veto != tc.wantVeto {
				t.Fatalf(
					"veto=%v want %v (IC=%.3f IAC=%.3f score=%.3f)",
					got.Veto,
					tc.wantVeto,
					got.IC,
					got.IAC,
					got.Score,
				)
			}
			if tc.wantMerge && (got.Veto || got.Score <= 0) {
				t.Fatalf("want merge but veto=%v score=%.3f", got.Veto, got.Score)
			}
			if tc.wantNotPos && !got.Veto && got.Score > 0 {
				t.Fatalf(
					"want non-positive score, got %.3f (IC=%.3f IAC=%.3f)",
					got.Score,
					got.IC,
					got.IAC,
				)
			}
		})
	}
}

func TestIntervalOverlaps(t *testing.T) {
	cases := []struct {
		a, b Interval
		want bool
	}{
		{Interval{}, Interval{}, true}, // both unbounded
		{interval("2000-01-01", "2005-01-01"), interval("2004-01-01", "2010-01-01"), true},
		{interval("2000-01-01", "2005-01-01"), interval("2006-01-01", "2010-01-01"), false},
		{interval("2000-01-01", ""), interval("", "1999-01-01"), false},
		{interval("", "2005-01-01"), interval("2001-01-01", ""), true},
	}
	for i, c := range cases {
		if got := c.a.Overlaps(c.b); got != c.want {
			t.Fatalf("case %d: overlaps=%v want %v", i, got, c.want)
		}
	}
}

func TestOmegaColdStartNonZero(t *testing.T) {
	// At cold start (df=0, entities=0) idf=0, so ω falls back to the kind base —
	// never zero, so a fresh value still contributes to IC.
	if got := omega(KindFunctional, 0, 0); got != kindBaseFunctional {
		t.Fatalf("cold functional ω=%.3f want %.3f", got, kindBaseFunctional)
	}
	if got := omega(KindSet, 0, 0); got != kindBaseSet {
		t.Fatalf("cold set ω=%.3f want %.3f", got, kindBaseSet)
	}
	// A rare value at scale weighs strictly more than a ubiquitous one.
	rare := omega(KindSet, 1, 5000)
	common := omega(KindSet, 5000, 5000)
	if rare <= common {
		t.Fatalf("rare ω=%.3f should exceed common ω=%.3f", rare, common)
	}
}

// TestResolveStrangersStaySeparate: two unrelated records mint two entities even
// when they block together (a shared low-ω context term cannot overcome a name
// contradiction).
func TestResolveStrangersStaySeparate(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	alice, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name: alice smith", true),
		claimInput("org: acme corp", false),
		claimInput("email: alice@a.example", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}

	bob, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name: bob jones", true),
		claimInput("org: acme corp", false),
		claimInput("email: bob@b.example", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve bob: %v", err)
	}

	if !alice.IsNew || !bob.IsNew || alice.Entity == bob.Entity {
		t.Fatalf("strangers merged: alice=%+v bob=%+v", alice, bob)
	}
}

// TestResolveMemoIdempotentReingest: an identical observation re-ingested after
// the index has grown must resolve to the SAME entity as a NOOP and mint nothing
// — independent of centroid drift or blocking recall (the observation memo).
func TestResolveMemoIdempotentReingest(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	claims := []ClaimInput{
		claimInput("name: ada lovelace", true),
		claimInput("email: ada@x.example", false),
	}
	first, err := service.Resolve(ctx, claims, 0.72, 0.88)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}

	// Grow the index with unrelated entities so a blocking miss is plausible.
	for i := range 50 {
		_, err := service.Resolve(ctx, []ClaimInput{
			claimInput(fmt.Sprintf("name: noise person %d", i), true),
			claimInput(fmt.Sprintf("email: noise%d@z.example", i), false),
		}, 0.72, 0.88)
		if err != nil {
			t.Fatalf("noise resolve %d: %v", i, err)
		}
	}

	before := service.index.Len()
	again, err := service.Resolve(ctx, claims, 0.72, 0.88)
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}

	if !again.IsNoop || again.Entity != first.Entity {
		t.Fatalf("re-ingest not idempotent: want NOOP on %s, got %+v", first.Entity, again)
	}
	if service.index.Len() != before {
		t.Fatalf("re-ingest minted entities: %d -> %d", before, service.index.Len())
	}
}
