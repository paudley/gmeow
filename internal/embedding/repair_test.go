// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"reflect"
	"testing"
)

func edge(
	a, b string,
	w float64,
) SignedEdge {
	return SignedEdge{A: a, B: b, Weight: w}
}

// TestClusterByConstrainedLinkage covers the net-new clustering logic without an
// embedder: transitive single-linkage merges along positive edges, a cannot-link
// pair blocks the union that would co-locate it, and the canonical is the min ULID.
func TestClusterByConstrainedLinkage(t *testing.T) {
	entities := []string{"a", "b", "c", "d", "x"}

	t.Run("transitive merge", func(t *testing.T) {
		got := clusterByConstrainedLinkage(
			entities,
			[]SignedEdge{edge("a", "b", 2.0), edge("b", "c", 1.5)},
			nil,
		)
		// {a,b,c} merged (canonical a), d and x singletons.
		if !hasCluster(got, []string{"a", "b", "c"}) ||
			!hasCluster(got, []string{"d"}) || !hasCluster(got, []string{"x"}) {
			t.Fatalf("transitive merge wrong: %v", got)
		}
	})

	t.Run("cannot-link blocks chaining", func(t *testing.T) {
		// a-b and b-c are strong positives, but a-c is a hard contradiction: a-b
		// merges first (strongest), then b-c is rejected because it would put the
		// cannot-link pair a-c in one cluster.
		got := clusterByConstrainedLinkage(
			entities,
			[]SignedEdge{edge("a", "b", 3.0), edge("b", "c", 2.0)},
			map[edgeKey]bool{orderedKey("a", "c"): true},
		)
		if !hasCluster(got, []string{"a", "b"}) || !hasCluster(got, []string{"c"}) {
			t.Fatalf("cannot-link did not block chaining: %v", got)
		}
	})

	t.Run("deterministic + min-ULID canonical", func(t *testing.T) {
		g1 := clusterByConstrainedLinkage(entities, []SignedEdge{edge("c", "a", 1.0)}, nil)
		g2 := clusterByConstrainedLinkage(entities, []SignedEdge{edge("a", "c", 1.0)}, nil)
		if !reflect.DeepEqual(g1, g2) {
			t.Fatalf("non-deterministic: %v vs %v", g1, g2)
		}
		for _, c := range g1 {
			if len(c) == 2 && c[0] != "a" { // {a,c} canonical must be "a" (min ULID)
				t.Fatalf("canonical is not min ULID: %v", c)
			}
		}
	})
}

func hasCluster(clusters [][]string, want []string) bool {
	for _, c := range clusters {
		if reflect.DeepEqual(c, want) {
			return true
		}
	}

	return false
}

// TestRepairDoesNotMergeDistinctEntities is the safety check on the stub embedder:
// REPAIR over a set of genuinely-distinct entities (different names + identifiers)
// must NOT spuriously merge any, and re-running it is a NOOP. (The positive-merge
// behaviour depends on the real embedder's variant matching and is validated on the
// live corpus; here we lock that REPAIR is conservative and idempotent.)
func TestRepairDoesNotMergeDistinctEntities(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	for _, claims := range [][]ClaimInput{
		{claimInput("name: dana lin", true), claimInput("email: dana@x.example", false)},
		{claimInput("name: omar reed", true), claimInput("email: omar@y.example", false)},
		{claimInput("name: priya nair", true), claimInput("email: priya@z.example", false)},
	} {
		if _, err := service.Resolve(ctx, claims, threshold, threshold); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	before := service.index.Len()
	res, err := service.Repair(
		ctx,
		RepairParams{Threshold: threshold, NameThreshold: threshold},
	)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if res.Merges != 0 || service.index.Len() != before {
		t.Fatalf(
			"repair merged distinct entities: %+v (len %d→%d)",
			res,
			before,
			service.index.Len(),
		)
	}

	// Idempotent: a second pass changes nothing.
	res2, err := service.Repair(
		ctx,
		RepairParams{Threshold: threshold, NameThreshold: threshold},
	)
	if err != nil {
		t.Fatalf("repair 2: %v", err)
	}
	if res2.Merges != 0 {
		t.Fatalf("second repair was not a NOOP: %+v", res2)
	}
}
