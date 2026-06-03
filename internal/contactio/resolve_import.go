// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/embedding"
)

// EntityPrefix is the IRI namespace for resolved contact entities; the local
// part is the entity ULID.
const EntityPrefix = "urn:gmeow:entity:"

// ObservationPrefix is the IRI namespace for the entity-independent observation
// identity used as the FILESTORE source-dedup ExternalID; the local part is the
// observation fingerprint (a hash of the parsed claim set).
const ObservationPrefix = "urn:gmeow:obs:"

// Resolver is the ingest-time entity-resolution surface ResolveImport needs. It
// is satisfied by the in-process *embedding.Service and by the gRPC
// *rpc.EmbeddingClient (same shape), so import swaps transports without change.
type Resolver interface {
	Resolve(
		ctx context.Context,
		claims []embedding.ClaimInput,
		threshold, nameThreshold float64,
	) (embedding.Resolution, error)
}

// ResolvedRecord is the ingest outcome for one parsed logical contact: either a
// NOOP (no new information for its entity) or an immutable delta record
// (entity-subjected Turtle to persist in FILESTORE).
type ResolvedRecord struct {
	Entity string
	// ObsFingerprint is the entity-INDEPENDENT identity of the observation: a
	// stable hash of the parsed claim set, computed before resolution. It keys
	// FILESTORE source-dedup so that re-ingesting the same observation is a NOOP
	// regardless of which entity it resolves to (resolution may drift); identity
	// is a separate, latent question (see docs/architecture/CONTACT_IDENTITY_RESOLUTION.md).
	ObsFingerprint string
	Content        string
	Facets         []contracts.Facet
	IsNoop         bool
	IsNew          bool
	Delta          int
}

// ResolveImport parses one input into logical-contact records, resolves each
// against the entity space via the Resolver, and returns the immutable delta
// records to persist (NOOPs carry no Content). ImportResult.Contacts is rewritten
// to the distinct resolved ENTITY ids. observedAt stamps each new claim's
// transaction time (the projection folds these into (first_seen,last_seen)).
func ResolveImport(
	ctx context.Context,
	resolver Resolver,
	threshold, nameThreshold float64,
	format string,
	sourceName string,
	content []byte,
	options ImportOptions,
	observedAt time.Time,
) ([]ResolvedRecord, ImportResult, error) {
	deltas, result, err := BuildContactDeltas(format, sourceName, content, options)
	if err != nil {
		return nil, ImportResult{}, err
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	records := make([]ResolvedRecord, 0, len(deltas))
	entitySeen := map[string]bool{}
	entities := []string{}
	for _, delta := range deltas {
		statements := claimStatementsFromBody(delta.Content)
		obsFingerprint := observationFingerprint(statements)
		resolution, resolveErr := resolver.Resolve(
			ctx,
			claimInputs(statements),
			threshold,
			nameThreshold,
		)
		if resolveErr != nil {
			return nil, ImportResult{}, resolveErr
		}

		if resolution.Entity != "" && !entitySeen[resolution.Entity] {
			entitySeen[resolution.Entity] = true
			entities = append(entities, resolution.Entity)
		}

		if resolution.IsNoop {
			records = append(records, ResolvedRecord{
				Entity:         resolution.Entity,
				ObsFingerprint: obsFingerprint,
				IsNoop:         true,
			})

			continue
		}

		delta := deltaClaims(statements, resolution.NewClaimHashes)
		records = append(records, ResolvedRecord{
			Entity:         resolution.Entity,
			ObsFingerprint: obsFingerprint,
			Content: buildEntityDeltaRecord(
				resolution.Entity,
				delta,
				options.ImportLevel,
				observedAt,
			),
			Facets: importFacets(
				format,
				[]string{EntityPrefix + resolution.Entity},
				options.ImportLevel,
			),
			IsNew: resolution.IsNew,
			Delta: len(delta),
		})
	}

	result.Contacts = entities

	return records, result, nil
}

// claimInputs projects record claim statements onto the resolution input type
// (text + hash + name flag; the Turtle Line is only needed when persisting).
func claimInputs(statements []claimStatement) []embedding.ClaimInput {
	inputs := make([]embedding.ClaimInput, len(statements))
	for i, s := range statements {
		inputs[i] = embedding.ClaimInput{Text: s.Text, Hash: s.Hash, IsName: s.IsName}
	}

	return inputs
}

// observationFingerprint is the entity-independent identity of a parsed
// observation: a stable hash of its sorted claim hashes. Two ingests of the same
// observation (same claim set) yield the same fingerprint regardless of import
// time or which entity they resolve to, so FILESTORE source-dedup collapses
// redundant re-ingests to a NOOP. The empty-claim observation hashes to a fixed
// sentinel rather than colliding with other empties at the source layer.
func observationFingerprint(statements []claimStatement) string {
	hashes := make([]string, len(statements))
	for i, s := range statements {
		hashes[i] = s.Hash
	}
	sort.Strings(hashes)

	return embedding.StatementHash(strings.Join(hashes, "\n"))
}

// deltaClaims selects the statements whose hashes the resolver reported as new,
// preserving record order, so the delta record persists exactly the new claims.
func deltaClaims(statements []claimStatement, newHashes []string) []claimStatement {
	newSet := make(map[string]bool, len(newHashes))
	for _, hash := range newHashes {
		newSet[hash] = true
	}
	delta := make([]claimStatement, 0, len(newHashes))
	for _, s := range statements {
		if newSet[s.Hash] {
			delta = append(delta, s)
		}
	}

	return delta
}

// buildEntityDeltaRecord renders an immutable delta record: each new claim
// re-subjected to the resolved entity IRI, plus an RDF-star gmeow:observedAt
// annotation carrying this observation's transaction time, plus the entity's
// importance claim. Records are append-only; the entity's full graph is the
// ordered stack of all its records.
func buildEntityDeltaRecord(
	entity string,
	delta []claimStatement,
	level int,
	observedAt time.Time,
) string {
	subject := iri(EntityPrefix + entity)
	stamp := typedDateTime(observedAt)

	var builder strings.Builder
	writePrefixes(&builder)
	for _, claim := range delta {
		triple := subject + " " + claim.Line
		builder.WriteString(triple + " .\n")
		builder.WriteString(
			"<< " + triple + " >> " + iri(gmeowPrefix+"observedAt") + " " + stamp + " .\n",
		)
	}
	builder.WriteString(
		BuildRDFStarDelta([]Claim{importanceClaim(EntityPrefix+entity, level)}),
	)

	return builder.String()
}
