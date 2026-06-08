// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"fmt"
	"math"
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

// TestContextualMassBoundedByIncomingNotCandidateSize locks the 1:1 fix: one
// incoming name-part claim earns at most one match's mass even when the candidate
// holds many claims that all match it. Without the bound, contextual IC grew with
// candidate size and a bag of common name parts accreted unrelated people into a
// blob (corpus: one entity, 1 name, 1294 emails before strict grounding; a residual
// name-fragment blob after, until this bound).
func TestContextualMassBoundedByIncomingNotCandidateSize(t *testing.T) {
	p := idDiffParams{
		TauSet: 0.9, TauFunc: 0.9, TauCtx: 0.6, Lambda: 1.5,
		SetPenalty: 0.5, VetoMass: 4.0, ObservationMode: true, W: corpusWeight,
	}

	// A large candidate whose 50 name-part claims all fuzzy-match the incoming one.
	big := make([]scoredClaim, 0, 50)
	for i := range 50 {
		big = append(big, scoredClaim{
			Attr:  "given-name",
			Value: fmt.Sprintf("doug%d", i),
			Kind:  KindContextual,
			Vec:   unit(1),
		})
	}
	incoming := []scoredClaim{
		{Attr: "given-name", Value: "doug", Kind: KindContextual, Vec: unit(1)},
	}

	got := idDiff(incoming, big, p)
	oneMatch := omega(KindContextual, 1, 5000)
	if got.IC > oneMatch*1.0001 {
		t.Fatalf(
			"contextual IC %.4f exceeds single best match %.4f — candidate-size multiplication not bounded",
			got.IC,
			oneMatch,
		)
	}
}

// TestNameScoreSubsumption locks the role-free idf-subsumption semantics for the
// canonical forms of one person's name (CONTACT_IDENTITY_RESOLUTION.md §4.2):
// completion is compatible (corroborates, no penalty), divergence withholds
// corroboration and penalises, and a shared RARE token cannot fuse two divergent
// names (the anti-over-merge / blob guard).
func TestNameScoreSubsumption(t *testing.T) {
	// per-token rarity mass: common given names low, distinctive tokens high. (There
	// is no "surname" role — only rarity.)
	mass := map[string]float64{
		"patrick": 0.30, "colm": 0.30, "susan": 0.80,
		"audley": 1.20, "smith": 1.20,
	}
	idx := map[string]int{}
	tok := func(v string) scoredClaim {
		i, ok := idx[v]
		if !ok {
			i = len(idx) + 1
			idx[v] = i
		}

		return scoredClaim{Attr: "name-token", Value: v, Kind: KindName, Vec: unit(i)}
	}
	name := func(vs ...string) []scoredClaim {
		out := make([]scoredClaim, len(vs))
		for i, v := range vs {
			out[i] = tok(v)
		}

		return out
	}
	p := idDiffParams{
		TauName: 0.9,
		W:       func(c scoredClaim) float64 { return mass[c.Value] },
	}

	cases := []struct {
		desc        string
		a, b        []scoredClaim
		wantIC      float64
		wantDiverge bool // IAC > 0 (and IC withheld)
	}{
		{
			"Patrick subset of Patrick Audley (completion)",
			name("patrick"),
			name("patrick", "audley"),
			0.30,
			false,
		},
		{
			"Patrick Audley ~ Patrick Colm Audley (extra middle)",
			name("patrick", "audley"),
			name("patrick", "colm", "audley"),
			1.50,
			false,
		},
		{
			"Mr Patrick Audley == Patrick Audley (honorific pre-stripped)",
			name("patrick", "audley"),
			name("patrick", "audley"),
			1.50,
			false,
		},
		{
			"Patrick Audley vs Patrick Smith (divergent surnames)",
			name("patrick", "audley"),
			name("patrick", "smith"),
			0,
			true,
		},
		{
			"Patrick Audley vs Susan Audley (shared rare surname, blob guard)",
			name("patrick", "audley"),
			name("susan", "audley"),
			0,
			true,
		},
		{
			"P Audley matches Patrick Audley (initial)",
			name("p", "audley"),
			name("patrick", "audley"),
			1.20,
			false,
		},
	}
	for _, c := range cases {
		ic, iac := nameScore(c.a, c.b, p)
		if (iac > 0) != c.wantDiverge {
			t.Errorf("%s: diverge=%v (iac=%.2f), want %v", c.desc, iac > 0, iac, c.wantDiverge)
		}
		if !c.wantDiverge && math.Abs(ic-c.wantIC) > 0.01 {
			t.Errorf("%s: ic=%.2f, want %.2f", c.desc, ic, c.wantIC)
		}
		if c.wantDiverge && ic != 0 {
			t.Errorf("%s: divergent pair must withhold IC, got ic=%.2f", c.desc, ic)
		}
	}
}

// TestResolveValidTimeGate locks Tier 2's co-validity: a shared identifier value
// correlates two records only while it is contemporaneously held. Disjoint valid
// time (a transferred/inherited value) does NOT merge; overlapping valid time does.
func TestResolveValidTimeGate(t *testing.T) {
	ctx := context.Background()
	const threshold = 0.5

	// Both records carry the same name + email (enough IC to merge when co-valid);
	// validity rides every claim, so disjoint windows gate ALL the IC out.
	rec := func(from, until string) []ClaimInput {
		claim := func(text string) ClaimInput {
			return ClaimInput{
				Text:       text,
				Hash:       StatementHash(text),
				ValidFrom:  from,
				ValidUntil: until,
			}
		}

		// Post-extraction, a name arrives as role-free TOKENS (claim_extract), so the
		// name evidence here is two name-token claims, not one whole-name claim.
		return []ClaimInput{
			claim("name-token: pat"),
			claim("name-token: holder"),
			claim("email: admin@axion.example"),
		}
	}
	email := rec

	t.Run("disjoint validity does not merge (transfer)", func(t *testing.T) {
		s := newTestService()
		a, err := s.Resolve(
			ctx,
			email("1996-01-01T00:00:00Z", "1998-01-01T00:00:00Z"),
			threshold,
			threshold,
		)
		if err != nil {
			t.Fatalf("resolve a: %v", err)
		}
		b, err := s.Resolve(
			ctx,
			email("2010-01-01T00:00:00Z", "2012-01-01T00:00:00Z"),
			threshold,
			threshold,
		)
		if err != nil {
			t.Fatalf("resolve b: %v", err)
		}
		if !b.IsNew || b.Entity == a.Entity {
			t.Fatalf(
				"disjoint-validity shared identifier merged transferred holders: a=%+v b=%+v",
				a,
				b,
			)
		}
	})

	t.Run("overlapping validity merges", func(t *testing.T) {
		s := newTestService()
		a, err := s.Resolve(
			ctx,
			email("1996-01-01T00:00:00Z", "1998-01-01T00:00:00Z"),
			threshold,
			threshold,
		)
		if err != nil {
			t.Fatalf("resolve a: %v", err)
		}
		b, err := s.Resolve(
			ctx,
			email("1997-01-01T00:00:00Z", "1999-01-01T00:00:00Z"),
			threshold,
			threshold,
		)
		if err != nil {
			t.Fatalf("resolve b: %v", err)
		}
		if b.Entity != a.Entity {
			t.Fatalf("overlapping-validity shared identifier did not merge: a=%+v b=%+v", a, b)
		}
	})
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

func TestResolveSharedWorkplaceAndAddressDoNotIdentifyPerson(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	alice, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name-token: alice", true),
		claimInput("name-token: nguyen", true),
		claimInput("email: alice.nguyen@personal.example", false),
		claimInput("phone: +15550101", false),
		claimInput("works-for: example research lab", false),
		claimInput("job-title: engineer", false),
		claimInput("street-address: 100 shared campus way", false),
		claimInput("locality: victoria", false),
		claimInput("region: bc", false),
		claimInput("country: ca", false),
		claimInput("postal-code: v8v 1a1", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}

	bob, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name-token: bob", true),
		claimInput("name-token: patel", true),
		claimInput("email: bob.patel@personal.example", false),
		claimInput("phone: +15550102", false),
		claimInput("works-for: example research lab", false),
		claimInput("job-title: engineer", false),
		claimInput("street-address: 100 shared campus way", false),
		claimInput("locality: victoria", false),
		claimInput("region: bc", false),
		claimInput("country: ca", false),
		claimInput("postal-code: v8v 1a1", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve bob: %v", err)
	}

	if alice.Entity == bob.Entity {
		t.Fatalf(
			"shared workplace/address context merged distinct people: alice=%+v bob=%+v",
			alice,
			bob,
		)
	}
}

func TestResolveTinyOverlapInRichIdentifierSetDoesNotIdentifyPerson(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	alice, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name-token: alice", true),
		claimInput("name-token: nguyen", true),
		claimInput("email: shared-one@example.test", false),
		claimInput("email: shared-two@example.test", false),
		claimInput("phone: +15550101", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}

	rich := []ClaimInput{
		claimInput("name-token: robert", true),
		claimInput("name-token: patel", true),
		claimInput("email: shared-one@example.test", false),
		claimInput("email: shared-two@example.test", false),
		claimInput("phone: +15550200", false),
	}
	for i := range 30 {
		rich = append(rich,
			claimInput(fmt.Sprintf("email: role-%02d@example.test", i), false),
			claimInput(fmt.Sprintf("account: https://social.example/%02d", i), false),
		)
	}

	bob, err := service.Resolve(ctx, rich, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve rich observation: %v", err)
	}

	if alice.Entity == bob.Entity {
		t.Fatalf(
			"tiny overlap in rich identifier set merged distinct people: alice=%+v rich=%+v",
			alice,
			bob,
		)
	}
}

func TestResolveTinyOverlapPlusSharedPhoneInRichIdentifierSetDoesNotIdentifyPerson(
	t *testing.T,
) {
	ctx := context.Background()
	service := newTestService()

	alice, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name-token: alice", true),
		claimInput("name-token: nguyen", true),
		claimInput("email: shared-one@example.test", false),
		claimInput("email: shared-two@example.test", false),
		claimInput("email: shared-three@example.test", false),
		claimInput("phone: +15550101", false),
		claimInput("birth-date: 1970-01-01", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}

	rich := []ClaimInput{
		claimInput("name-token: robert", true),
		claimInput("name-token: patel", true),
		claimInput("email: shared-one@example.test", false),
		claimInput("email: shared-two@example.test", false),
		claimInput("email: shared-three@example.test", false),
		claimInput("phone: +15550101", false),
		claimInput("birth-date: 1980-01-01", false),
	}
	for i := range 30 {
		rich = append(rich,
			claimInput(fmt.Sprintf("email: role-%02d@example.test", i), false),
			claimInput(fmt.Sprintf("account: https://social.example/%02d", i), false),
			claimInput(fmt.Sprintf("url: https://profile.example/%02d", i), false),
		)
	}

	bob, err := service.Resolve(ctx, rich, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve rich observation: %v", err)
	}

	if alice.Entity == bob.Entity {
		t.Fatalf(
			"tiny overlap plus shared phone in rich identifier set merged distinct people: alice=%+v rich=%+v",
			alice,
			bob,
		)
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

// TestResolveIdentifierBlockingMergesSharedEmail: a record sharing only an email
// with an existing entity merges via the inverted identifier index even when its
// centroid (different name/context) would miss the HNSW blocking at scale — the
// cross-format recall channel.
func TestResolveIdentifierBlockingMergesSharedEmail(t *testing.T) {
	ctx := context.Background()
	service := newTestService()

	alice, err := service.Resolve(ctx, []ClaimInput{
		claimInput("name: alice anderson", true),
		claimInput("email: shared@x.example", false),
		claimInput("note: alpha context one", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}

	// Grow the index so centroid blocking alone would not surface alice.
	for i := range 100 {
		_, err := service.Resolve(ctx, []ClaimInput{
			claimInput(fmt.Sprintf("name: noise person %d", i), true),
			claimInput(fmt.Sprintf("email: noise%d@z.example", i), false),
		}, 0.72, 0.88)
		if err != nil {
			t.Fatalf("noise resolve %d: %v", i, err)
		}
	}

	before := service.index.Len()
	// Same email, otherwise entirely different (no name, different note + a phone)
	// — a cross-source observation of the same person.
	bob, err := service.Resolve(ctx, []ClaimInput{
		claimInput("email: shared@x.example", false),
		claimInput("note: zeta unrelated context", false),
		claimInput("phone: 5551112222", false),
	}, 0.72, 0.88)
	if err != nil {
		t.Fatalf("resolve shared-email record: %v", err)
	}

	if bob.IsNew || bob.Entity != alice.Entity {
		t.Fatalf(
			"shared-email record did not merge via identifier blocking: alice=%s got=%+v",
			alice.Entity,
			bob,
		)
	}
	if service.index.Len() != before {
		t.Fatalf(
			"minted instead of merging on shared identifier: %d -> %d",
			before,
			service.index.Len(),
		)
	}
}
