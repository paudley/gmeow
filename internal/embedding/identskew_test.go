// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"os"
	"sort"
	"strings"
	"testing"
)

// TestDumpIdentifierSkew is a read-only diagnostic over a persisted resolution
// state (SnapshotState artifact). It is gated on GMEOW_STATE_DUMP=<path> so it
// never runs in `make check`; point it at ./data/resolution.state to inspect the
// live corpus state.
//
// Premise (per the resolution principle): for an identifier concept
// (email/phone/account/url), a value's posting list SHOULD point at ~1 entity. A
// fat posting list is never benign popularity — it is either (A) an under-merge
// bug (records that should have collapsed didn't) or (B) a normalization/validation
// collision (distinct or null values bucketed together). This dump classifies which.
func TestDumpIdentifierSkew(t *testing.T) {
	path := os.Getenv("GMEOW_STATE_DUMP")
	if path == "" {
		t.Skip("set GMEOW_STATE_DUMP=<resolution.state path> to run the diagnostic")
	}

	data, err := os.ReadFile(path) //nolint:gosec // diagnostic, operator-provided path
	if err != nil {
		t.Fatalf("read state %q: %v", path, err)
	}

	service := newTestService()
	if err := service.LoadState(data); err != nil {
		t.Fatalf("load state: %v", err)
	}

	l := service.ledger

	// --- identifier-index skew: posting-list size per identifier value ---
	type keyStat struct {
		concept, value string
		entities       int
	}
	stats := make([]keyStat, 0, len(l.ident))
	var emptyValueKeys, totalPostings int
	bucket := map[string]int{} // posting-size bucket -> #keys
	bump := func(n int) {
		switch {
		case n <= 1:
			bucket["01"]++
		case n <= 5:
			bucket["02..05"]++
		case n <= 20:
			bucket["06..20"]++
		case n <= 100:
			bucket["21..100"]++
		default:
			bucket[">100"]++
		}
	}
	for key, ents := range l.ident {
		uniq := map[string]bool{}
		for _, e := range ents {
			uniq[e] = true
		}
		n := len(uniq)
		concept, value, _ := strings.Cut(key, "\x00")
		if strings.TrimSpace(value) == "" {
			emptyValueKeys++
		}
		totalPostings += len(ents)
		bump(n)
		stats = append(stats, keyStat{concept: concept, value: value, entities: n})
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].entities != stats[j].entities {
			return stats[i].entities > stats[j].entities
		}

		return stats[i].value < stats[j].value
	})

	t.Logf(
		"=== identifier index: %d distinct keys, %d total postings, %d empty-value keys ===",
		len(l.ident),
		totalPostings,
		emptyValueKeys,
	)
	for _, b := range []string{"01", "02..05", "06..20", "21..100", ">100"} {
		t.Logf("  posting-size %-8s : %d keys", b, bucket[b])
	}
	// Under-merge signal: a strong identifier (email/phone) on >1 entity usually
	// means the same person was split (e.g. a name-form variant vetoed the merge).
	// Role/shared values exist but should be the minority; a large count here points
	// at under-merge (the name-veto calibration risk).
	splitByConcept := map[string]int{}
	for key, ents := range l.ident {
		concept, _, _ := strings.Cut(key, "\x00")
		uniq := map[string]bool{}
		for _, e := range ents {
			uniq[e] = true
		}
		if len(uniq) >= 2 {
			splitByConcept[concept]++
		}
	}
	t.Logf("--- identifier values split across >=2 entities (under-merge signal) ---")
	for _, c := range []string{"email", "phone", "account", "url"} {
		t.Logf("  %-8s on >=2 entities: %d keys", c, splitByConcept[c])
	}

	t.Logf(
		"--- top 50 fattest identifier keys (each row = a value shared across N entities) ---",
	)
	for i, s := range stats {
		if i >= 50 || s.entities <= 1 {
			break
		}
		v := s.value
		if len(v) > 80 {
			v = v[:80] + "…"
		}
		t.Logf("  [%3d entities] %-8s %q", s.entities, s.concept, v)
	}

	// --- entity-size histogram: claims per entity (over-merge counter-check) ---
	sizes := make([]int, 0, len(l.claims))
	for _, set := range l.claims {
		sizes = append(sizes, len(set))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))
	var total int
	for _, n := range sizes {
		total += n
	}
	t.Logf("=== entities: %d, total claims: %d, mean claims/entity: %.1f ===",
		len(sizes), total, float64(total)/float64(max(1, len(sizes))))
	if len(sizes) > 0 {
		t.Logf("  largest entities (claim count): %v", sizes[:min(15, len(sizes))])
	}
	// A healthy distribution has a long tail of small entities; a few giant ones
	// signal over-merge (one node absorbing many people via a shared value).
	for _, q := range []int{50, 100, 200, 500} {
		c := 0
		for _, n := range sizes {
			if n >= q {
				c++
			}
		}
		t.Logf("  entities with >=%d claims: %d", q, c)
	}

	// --- blob autopsy: composition of the single largest entity ---
	var blob string
	blobN := -1
	for e, set := range l.claims {
		if len(set) > blobN {
			blobN, blob = len(set), e
		}
	}
	if blob == "" {
		return
	}
	byConcept := map[string]int{}
	names := map[string]int{}
	emails := map[string]int{}
	givens := map[string]int{}
	families := map[string]int{}
	worksFor := map[string]int{}
	phones := map[string]int{}
	for _, entry := range l.claims[blob] {
		concept, value := splitClaimText(entry.Text)
		byConcept[concept]++
		switch concept {
		case "name":
			names[value]++
		case "email":
			emails[value]++
		case "given-name":
			givens[value]++
		case "family-name":
			families[value]++
		case "works-for":
			worksFor[value]++
		case "phone":
			phones[value]++
		}
	}
	t.Logf("=== BLOB autopsy: entity %s with %d claims ===", blob, blobN)
	concepts := make([]string, 0, len(byConcept))
	for c := range byConcept {
		concepts = append(concepts, c)
	}
	sort.Slice(
		concepts,
		func(i, j int) bool { return byConcept[concepts[i]] > byConcept[concepts[j]] },
	)
	for _, c := range concepts {
		t.Logf("  concept %-14s : %d claims", c, byConcept[c])
	}
	t.Logf(
		"  DISTINCT names in blob: %d, DISTINCT emails in blob: %d",
		len(names),
		len(emails),
	)
	top := func(m map[string]int, label string, lim int) {
		type kv struct {
			k string
			n int
		}
		rows := make([]kv, 0, len(m))
		for k, n := range m {
			rows = append(rows, kv{k, n})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].n > rows[j].n })
		for i, r := range rows {
			if i >= lim {
				break
			}
			v := r.k
			if len(v) > 60 {
				v = v[:60] + "…"
			}
			t.Logf("    %s x%-3d %q", label, r.n, v)
		}
	}
	t.Logf(
		"  DISTINCT given=%d family=%d works-for=%d phone=%d",
		len(givens),
		len(families),
		len(worksFor),
		len(phones),
	)
	top(names, "name ", 10)
	top(givens, "given", 12)
	top(families, "famly", 12)
	top(worksFor, "works", 12)
	top(phones, "phone", 8)
}
