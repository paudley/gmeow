// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"math"
	"sort"
)

// residualBasis projects boilerplate (shared-structure) variance OUT of value
// embeddings before cosine comparison.
//
// Method: Audley (2025), "Emergent Knowledge Graphs from Nonlinear Semantic
// Residuals" (audley_2025_emergent_knowledge_graphs;
// https://github.com/paudley/nonlinear-semantic-graphs). The residual is
//
//	R = X − X·Wₖ·Wₖᵀ           (ABTT-adjacent — Mu & Viswanath, "All-but-the-Top")
//
// where Wₖ are the top-k principal directions of the value-embedding set. Those
// dominant directions encode the set's shared structure — email "@domains",
// account/url hosts like "facebook.com/", vCard boilerplate — so removing them
// separates structured identifiers that share a host/domain (facebook.com/X vs
// facebook.com/Y, a@gmail vs b@gmail) which raw cosine wrongly calls close, while
// preserving genuine matches. This is the right lever because the false-match is a
// REPRESENTATION problem (the shared substring dominates the vector), not a metric
// one — no Lₚ distance on the whole-value embedding separates them, but the residual
// does. IDF cannot suppress it either (both values are unique → high ω); decomposing
// the value by hand backfired (the host becomes an exact-match claim with high
// cold-start ω). Offline-validated on this corpus at k≈20: distinct same-host
// identifiers fell from 0.63 / 0.77 mean / max cosine to 0.03 / 0.24, while a true
// name variant ("Patrick Audley" / "Patrick C Audley") stayed at 0.90 and a
// Bob/Robert nickname at 0.65.
type residualBasis struct {
	mean Vector   // sample mean of the value-embedding set
	dirs []Vector // top-k unit, orthonormal principal directions
}

// residual returns the unit-normalized residual of v after removing the mean and
// its projection onto the top-k principal directions. A value that is almost pure
// boilerplate has a near-zero residual and is returned as the zero vector — it then
// correlates with nothing (cosine 0), which is exactly right: a bare "@gmail.com" or
// "facebook.com/" carries no identity. A nil/empty basis is a no-op (raw vector).
func (b *residualBasis) residual(v Vector) Vector {
	if b == nil || len(b.dirs) == 0 || len(v) != len(b.mean) {
		return v
	}

	r := make(Vector, len(v))
	for i := range v {
		r[i] = v[i] - b.mean[i]
	}
	for _, d := range b.dirs {
		var dot float32
		for i := range r {
			dot += r[i] * d[i]
		}
		for i := range r {
			r[i] -= dot * d[i]
		}
	}

	var sum float64
	for i := range r {
		sum += float64(r[i]) * float64(r[i])
	}
	norm := math.Sqrt(sum)
	if norm < 1e-6 {
		return r // ~zero residual: pure boilerplate, correlates with nothing
	}

	inv := float32(1.0 / norm)
	for i := range r {
		r[i] *= inv
	}

	return r
}

// residualBasisSampleCap bounds how many cached vectors feed a basis estimate; the
// top few principal directions are stable from a few thousand samples, and this
// keeps the periodic rebuild cheap.
const residualBasisSampleCap = 4096

// computeResidualBasis estimates the mean and top-k principal directions of a value-
// embedding set by power iteration with deflation — no external linear-algebra
// dependency (k is small and the embedding spectrum decays, so a handful of
// iterations per component converges). Deterministic (fixed init seed) so resolution
// stays reproducible. Returns nil when there is too little data to be meaningful.
func computeResidualBasis(cache map[string]Vector, k int) *residualBasis {
	if k <= 0 || len(cache) < k+1 {
		return nil
	}

	sample := sampleVectors(cache, residualBasisSampleCap)
	if len(sample) < k+1 {
		return nil
	}

	d := len(sample[0])
	mean := make(Vector, d)
	for _, v := range sample {
		if len(v) != d {
			continue
		}
		for i := range v {
			mean[i] += v[i]
		}
	}
	inv := float32(1.0 / float64(len(sample)))
	for i := range mean {
		mean[i] *= inv
	}

	// Centered copies (the data matrix X whose covariance we eigendecompose).
	centered := make([]Vector, 0, len(sample))
	for _, v := range sample {
		if len(v) != d {
			continue
		}
		c := make(Vector, d)
		for i := range v {
			c[i] = v[i] - mean[i]
		}
		centered = append(centered, c)
	}

	dirs := make([]Vector, 0, k)
	for comp := range k {
		v := seededUnit(d, comp)
		var prev float64
		for iter := range 128 {
			// w = C·v = (1/n)·Xᵀ(X·v)
			w := make(Vector, d)
			for _, x := range centered {
				var xv float32
				for i := range x {
					xv += x[i] * v[i]
				}
				for i := range x {
					w[i] += x[i] * xv
				}
			}
			// Deflate against already-found directions (Gram–Schmidt) so we get the
			// NEXT principal direction, not the first again.
			for _, p := range dirs {
				var dot float32
				for i := range w {
					dot += w[i] * p[i]
				}
				for i := range w {
					w[i] -= dot * p[i]
				}
			}
			norm := vectorNorm(w)
			if norm < 1e-9 {
				break // degenerate (rank exhausted)
			}
			scale := float32(1.0 / norm)
			for i := range w {
				w[i] *= scale
			}
			v = w
			if iter > 0 && norm-prev < 1e-6*norm {
				break // converged (Rayleigh quotient ≈ eigenvalue stabilized)
			}
			prev = norm
		}
		dirs = append(dirs, v)
	}

	return &residualBasis{mean: mean, dirs: dirs}
}

// sampleVectors takes up to cap vectors from the cache by a deterministic stride
// over sorted keys (reproducible; no RNG), so repeated builds on the same cache give
// the same basis.
func sampleVectors(cache map[string]Vector, cap int) []Vector {
	keys := make([]string, 0, len(cache))
	for h := range cache {
		keys = append(keys, h)
	}
	sort.Strings(keys)

	stride := 1
	if len(keys) > cap {
		stride = len(keys) / cap
	}

	out := make([]Vector, 0, min(len(keys), cap))
	for i := 0; i < len(keys) && len(out) < cap; i += stride {
		if v := cache[keys[i]]; len(v) > 0 {
			out = append(out, v)
		}
	}

	return out
}

func vectorNorm(v Vector) float64 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}

	return math.Sqrt(sum)
}

// seededUnit builds a deterministic pseudo-random unit vector (splitmix64-seeded by
// the component index) — a fixed, non-degenerate power-iteration start that keeps
// the basis reproducible.
func seededUnit(d, seed int) Vector {
	state := uint64(seed)*0x9E3779B97F4A7C15 + 0x9E3779B97F4A7C15
	v := make(Vector, d)
	for i := range v {
		state ^= state >> 30
		state *= 0xBF58476D1CE4E5B9
		state ^= state >> 27
		v[i] = float32(int64(state>>11))/float32(1<<53)*2 - 1
	}
	norm := vectorNorm(v)
	if norm == 0 {
		v[0] = 1

		return v
	}
	inv := float32(1.0 / norm)
	for i := range v {
		v[i] *= inv
	}

	return v
}
