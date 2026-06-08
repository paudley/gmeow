// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"fmt"
	"math"
	"testing"
)

// TestResidualSeparatesSharedStructure is the synthetic analogue of the corpus
// finding: vectors dominated by ONE shared direction (the "facebook.com/" / "@domain"
// boilerplate) plus a small distinct component (the handle) have high RAW cosine
// (the shared direction dominates), but removing the top principal direction leaves
// the distinct components, which are near-orthogonal — so the residual separates them.
func TestResidualSeparatesSharedStructure(t *testing.T) {
	const d = 8
	unitAt := func(i int, mag float32) Vector {
		v := make(Vector, d)
		v[i] = mag

		return v
	}
	// v_i = a_i·e0 (shared boilerplate, magnitude VARIES so it lands in a principal
	// direction rather than the mean) + 1·e_{distinct}, L2-normalized. Six distinct
	// axes (each low-variance, so none is the top PC).
	cache := map[string]Vector{}
	for i := range 60 {
		v := make(Vector, d)
		v[0] = float32(1 + i%5) // 1..5
		v[1+(i%6)] = 1
		normalizeInPlace(v)
		cache[fmt.Sprintf("h%d", i)] = v
	}

	a := cache["h0"] // distinct dir e1
	b := cache["h1"] // distinct dir e2 (different person, same shared structure)

	raw := dotF(a, b)
	if raw < 0.6 {
		t.Fatalf(
			"setup: raw cosine of shared-dominated vectors should be high, got %.2f",
			raw,
		)
	}

	basis := computeResidualBasis(cache, 2)
	if basis == nil || len(basis.dirs) != 2 {
		t.Fatalf("expected a 2-direction basis, got %+v", basis)
	}

	res := dotF(basis.residual(a), basis.residual(b))
	if res >= raw-0.2 {
		t.Fatalf(
			"residual should substantially REDUCE the shared-structure cosine: raw %.2f, residual %.2f",
			raw,
			res,
		)
	}
	// A value identical to another stays matched in residual space.
	if same := dotF(basis.residual(a), basis.residual(a)); same < 0.99 {
		t.Fatalf("residual of a value with itself must stay 1.0, got %.2f", same)
	}
	// Too little data → no basis (no-op).
	if computeResidualBasis(map[string]Vector{"x": unitAt(0, 1)}, 20) != nil {
		t.Fatalf("a tiny cache must yield no basis (residualize is a no-op)")
	}
}

func normalizeInPlace(v Vector) {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	inv := float32(1.0 / math.Sqrt(s))
	for i := range v {
		v[i] *= inv
	}
}

func dotF(a, b Vector) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}

	return s
}
