// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/embedding"
)

// TestIncrementalOrderedVCardCorpus is an opt-in diagnostic for order-sensitive
// contact-resolution failures. It preserves source card order and grows the
// prefix size stepwise, so the first bad merge can be isolated and converted to a
// small synthetic regression test. It logs only aggregate counts; do not add raw
// contact values here.
func TestIncrementalOrderedVCardCorpus(t *testing.T) {
	path := os.Getenv("GMEOW_INCREMENTAL_VCF")
	if path == "" {
		t.Skip("set GMEOW_INCREMENTAL_VCF to run the ordered vCard corpus diagnostic")
	}

	content, err := os.ReadFile(path) //nolint:gosec // opt-in local diagnostic
	if err != nil {
		t.Fatalf("read corpus vCard: %v", err)
	}
	cards := orderedVCardBlocks(string(content))
	if len(cards) == 0 {
		t.Fatalf("no vCards found in %s", path)
	}

	prefixes := corpusPrefixes(len(cards), os.Getenv("GMEOW_INCREMENTAL_PREFIXES"))
	limit := optionalPositiveInt(os.Getenv("GMEOW_MAX_RECORDS_PER_ENTITY"))
	reportLargest := os.Getenv("GMEOW_REPORT_LARGEST_ENTITY") != ""
	reportTop := optionalPositiveInt(os.Getenv("GMEOW_REPORT_TOP_ENTITIES"))
	reportDiverse := optionalPositiveInt(os.Getenv("GMEOW_REPORT_DIVERSE_ENTITIES"))

	var firstFail int
	for _, prefix := range prefixes {
		stats := runOrderedVCardPrefix(t, cards[:prefix])
		t.Logf(
			"prefix=%d entities=%d max_records_per_entity=%d noop_records=%d",
			prefix,
			stats.entities,
			stats.maxRecordsPerEntity,
			stats.noops,
		)
		if reportLargest {
			t.Log(stats.largest.String())
		}
		if reportTop > 0 {
			for i, entityStats := range stats.topEntities(reportTop) {
				t.Logf("top_entity_rank=%d\n%s", i+1, entityStats.String())
			}
		}
		if reportDiverse > 0 {
			for i, entityStats := range stats.diverseEntities(reportDiverse) {
				t.Logf(
					"diverse_entity_rank=%d diversity_score=%d\n%s",
					i+1,
					entityStats.identityDiversityScore(),
					entityStats.String(),
				)
			}
		}
		if limit > 0 && stats.maxRecordsPerEntity > limit {
			firstFail = prefix
			break
		}
	}

	if limit <= 0 || firstFail == 0 {
		return
	}

	lo, hi := 1, firstFail
	for lo < hi {
		mid := lo + (hi-lo)/2
		stats := runOrderedVCardPrefix(t, cards[:mid])
		if stats.maxRecordsPerEntity > limit {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	stats := runOrderedVCardPrefix(t, cards[:lo])
	t.Fatalf(
		"first prefix with entity receiving more than %d records is %d (entities=%d max_records_per_entity=%d)\n%s",
		limit,
		lo,
		stats.entities,
		stats.maxRecordsPerEntity,
		stats.largest.String(),
	)
}

type orderedCorpusStats struct {
	entities            int
	maxRecordsPerEntity int
	noops               int
	largest             orderedEntityStats
	all                 []orderedEntityStats
}

type orderedEntityStats struct {
	entity       string
	records      int
	noops        int
	recordsSeen  []int
	claimCounts  map[string]int
	distinctHash map[string]map[string]bool
	last         orderedObservationStats
}

type orderedObservationStats struct {
	recordIndex int
	isNoop      bool
	claims      map[string]int
	overlaps    map[string]int
}

func runOrderedVCardPrefix(t *testing.T, cards []string) orderedCorpusStats {
	t.Helper()

	ctx := context.Background()
	resolver := newIncrementalCorpusResolver()
	entities := map[string]*orderedEntityStats{}
	noops := 0

	for i, card := range cards {
		claimSets := orderedCardClaimSets(t, card)
		records, _, err := ResolveImport(
			ctx,
			resolver,
			0.72,
			0.88,
			FormatVCard,
			"ordered-corpus",
			[]byte(card),
			ImportOptions{ImportLevel: 5},
			SourceProvenance{
				Location:      fmt.Sprintf("ordered-vcard-prefix:%d", i+1),
				ContentDigest: ContentDigest([]byte(card)),
				IngestedAt:    time.Unix(0, 0).UTC(),
			},
		)
		if err != nil {
			t.Fatalf("resolve vCard %d: %v", i+1, err)
		}
		for j, record := range records {
			if record.Entity == "" {
				continue
			}
			stats := entities[record.Entity]
			if stats == nil {
				stats = &orderedEntityStats{
					entity:       record.Entity,
					claimCounts:  map[string]int{},
					distinctHash: map[string]map[string]bool{},
				}
				entities[record.Entity] = stats
			}
			stats.records++
			stats.recordsSeen = append(stats.recordsSeen, i+1)
			if record.IsNoop {
				noops++
				stats.noops++
			}
			if j < len(claimSets) {
				stats.last = stats.observationStats(i+1, record.IsNoop, claimSets[j])
				stats.addClaims(claimSets[j])
			}
		}
	}

	all := make([]orderedEntityStats, 0, len(entities))
	var largest orderedEntityStats
	for _, stats := range entities {
		all = append(all, *stats)
		if stats.records > largest.records ||
			(stats.records == largest.records && stats.entity < largest.entity) {
			largest = *stats
		}
	}

	return orderedCorpusStats{
		entities:            len(entities),
		maxRecordsPerEntity: largest.records,
		noops:               noops,
		largest:             largest,
		all:                 all,
	}
}

func (s orderedCorpusStats) topEntities(limit int) []orderedEntityStats {
	out := append([]orderedEntityStats{}, s.all...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].records != out[j].records {
			return out[i].records > out[j].records
		}
		return out[i].entity < out[j].entity
	})
	if len(out) > limit {
		out = out[:limit]
	}

	return out
}

func (s orderedCorpusStats) diverseEntities(limit int) []orderedEntityStats {
	out := append([]orderedEntityStats{}, s.all...)
	sort.Slice(out, func(i, j int) bool {
		left := out[i].identityDiversityScore()
		right := out[j].identityDiversityScore()
		if left != right {
			return left > right
		}
		if out[i].records != out[j].records {
			return out[i].records > out[j].records
		}
		return out[i].entity < out[j].entity
	})
	if len(out) > limit {
		out = out[:limit]
	}

	return out
}

func orderedCardClaimSets(t *testing.T, card string) [][]claimStatement {
	t.Helper()

	deltas, _, err := BuildContactDeltas(
		FormatVCard,
		"ordered-corpus-diagnostic",
		[]byte(card),
		ImportOptions{ImportLevel: 5},
	)
	if err != nil {
		t.Fatalf("parse diagnostic vCard claims: %v", err)
	}

	out := make([][]claimStatement, len(deltas))
	for i, delta := range deltas {
		out[i] = claimStatementsFromBody(delta.Content)
	}

	return out
}

func (s *orderedEntityStats) addClaims(claims []claimStatement) {
	for _, claim := range claims {
		concept, _, found := strings.Cut(claim.Text, ": ")
		if !found {
			concept = "unclassified"
		}
		s.claimCounts[concept]++
		if s.distinctHash[concept] == nil {
			s.distinctHash[concept] = map[string]bool{}
		}
		s.distinctHash[concept][claim.Hash] = true
	}
}

func (s orderedEntityStats) String() string {
	if s.records == 0 {
		return "largest entity: none"
	}

	var b strings.Builder
	fmt.Fprintf(
		&b,
		"largest entity: records=%d noops=%d first_record=%d last_record=%d span_sample=%v\n",
		s.records,
		s.noops,
		s.recordsSeen[0],
		s.recordsSeen[len(s.recordsSeen)-1],
		s.sampleRecordIndexes(12),
	)
	for _, concept := range sortedEntityClaimConcepts(s.claimCounts) {
		fmt.Fprintf(
			&b,
			"  concept=%-14s claims=%-4d distinct_values=%d\n",
			concept,
			s.claimCounts[concept],
			len(s.distinctHash[concept]),
		)
	}
	if s.last.recordIndex > 0 {
		fmt.Fprintf(&b, "  last_observation=%d noop=%t\n", s.last.recordIndex, s.last.isNoop)
		for _, concept := range sortedEntityClaimConcepts(s.last.claims) {
			fmt.Fprintf(
				&b,
				"    last concept=%-14s claims=%-3d preexisting_overlap=%d\n",
				concept,
				s.last.claims[concept],
				s.last.overlaps[concept],
			)
		}
	}

	return strings.TrimRight(b.String(), "\n")
}

func (s orderedEntityStats) identityDiversityScore() int {
	distinct := func(concept string) int {
		return len(s.distinctHash[concept])
	}

	return 3*distinct("name-token") +
		2*distinct("email") +
		2*distinct("phone") +
		distinct("account") +
		distinct("url") +
		2*distinct("birth-date")
}

func (s *orderedEntityStats) observationStats(
	recordIndex int,
	isNoop bool,
	claims []claimStatement,
) orderedObservationStats {
	out := orderedObservationStats{
		recordIndex: recordIndex,
		isNoop:      isNoop,
		claims:      map[string]int{},
		overlaps:    map[string]int{},
	}
	for _, claim := range claims {
		concept, _, found := strings.Cut(claim.Text, ": ")
		if !found {
			concept = "unclassified"
		}
		out.claims[concept]++
		if s.distinctHash[concept][claim.Hash] {
			out.overlaps[concept]++
		}
	}

	return out
}

func (s orderedEntityStats) sampleRecordIndexes(limit int) []int {
	if len(s.recordsSeen) <= limit {
		return append([]int{}, s.recordsSeen...)
	}
	out := append([]int{}, s.recordsSeen[:limit/2]...)
	out = append(out, s.recordsSeen[len(s.recordsSeen)-(limit-limit/2):]...)

	return out
}

func sortedEntityClaimConcepts(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}

		return keys[i] < keys[j]
	})

	return keys
}

func newIncrementalCorpusResolver() Resolver {
	service := embedding.NewService(
		embedding.NewResolver(
			embedding.NewMemoryCache(),
			stubEmbedder{dim: embedding.FullDim},
		),
		embedding.NewEntityIndex(embedding.FullDim, embedding.CoarseDim),
		"stub",
	)
	var counter int
	service.SetIDSource(func() string {
		counter++
		return fmt.Sprintf("01CORPUS%06d", counter)
	})

	return service
}

func orderedVCardBlocks(content string) []string {
	lines := strings.SplitAfter(content, "\n")
	var blocks []string
	var current strings.Builder
	inCard := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.EqualFold(trimmed, "BEGIN:VCARD") {
			inCard = true
			current.Reset()
		}
		if !inCard {
			continue
		}

		current.WriteString(line)
		if strings.EqualFold(trimmed, "END:VCARD") {
			blocks = append(blocks, current.String())
			inCard = false
		}
	}

	return blocks
}

func corpusPrefixes(total int, raw string) []int {
	if strings.TrimSpace(raw) == "" {
		var prefixes []int
		for n := 50; n < total; n *= 2 {
			prefixes = append(prefixes, n)
		}
		return append(prefixes, total)
	}

	seen := map[int]bool{}
	var prefixes []int
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			continue
		}
		if n > total {
			n = total
		}
		if !seen[n] {
			seen[n] = true
			prefixes = append(prefixes, n)
		}
	}
	sort.Ints(prefixes)
	if len(prefixes) == 0 {
		return []int{total}
	}

	return prefixes
}

func optionalPositiveInt(raw string) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return 0
	}

	return n
}
