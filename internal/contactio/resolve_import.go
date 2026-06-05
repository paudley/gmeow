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
	"blackcat.ca/gmeow/internal/ontology"
	"blackcat.ca/gmeow/internal/rdfbundle"
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
// to the distinct resolved ENTITY ids. The SourceProvenance places the carrier and
// transaction clocks (four-clock model); per-claim VALID time, when the source
// grounds it, is the only clock that reaches resolution.
// SourceProvenance is the import-time carrier/transaction provenance of one source
// artifact, placed on gmeow:Source / gmeow:ImportActivity nodes per the four-clock
// model (~/Active/gmeow-ontology/docs/import-provenance.md). None of these clocks
// is a claim's validity — only source-asserted VALID time gates resolution.
type SourceProvenance struct {
	Location      string    // file path / URL — gmeow:sourceLocation (audit)
	ModifiedAt    time.Time // file mtime — gmeow:sourceModifiedAt (CARRIER time)
	ContentDigest string    // content hash — gmeow:contentDigest (source identity)
	IngestedAt    time.Time // this import run — gmeow:ingestedAt (TRANSACTION time)
}

func ResolveImport(
	ctx context.Context,
	resolver Resolver,
	threshold, nameThreshold float64,
	format string,
	sourceName string,
	content []byte,
	options ImportOptions,
	prov SourceProvenance,
) ([]ResolvedRecord, ImportResult, error) {
	deltas, result, err := BuildContactDeltas(format, sourceName, content, options)
	if err != nil {
		return nil, ImportResult{}, err
	}
	if prov.IngestedAt.IsZero() {
		prov.IngestedAt = time.Now().UTC()
	}

	records := make([]ResolvedRecord, 0, len(deltas))
	entitySeen := map[string]bool{}
	entities := []string{}
	for _, delta := range deltas {
		parsed, annotations, parseErr := rdfbundle.Parse(delta.Content)
		if parseErr != nil {
			return nil, ImportResult{}, parseErr
		}
		comparison := claimStatementsFromStatements(parsed, annotations)
		obsFingerprint := observationFingerprint(comparison)
		resolution, resolveErr := resolver.Resolve(
			ctx,
			claimInputs(comparison),
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

		newHashes := make(map[string]bool, len(resolution.NewClaimHashes))
		for _, hash := range resolution.NewClaimHashes {
			newHashes[hash] = true
		}

		records = append(records, ResolvedRecord{
			Entity:         resolution.Entity,
			ObsFingerprint: obsFingerprint,
			Content: buildEntityDeltaGraph(
				resolution.Entity,
				delta.Identity,
				parsed,
				annotations,
				newHashes,
				options.ImportLevel,
				prov,
			),
			Facets: importFacets(
				format,
				[]string{EntityPrefix + resolution.Entity},
				options.ImportLevel,
			),
			IsNew: resolution.IsNew,
			Delta: len(resolution.NewClaimHashes),
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
		inputs[i] = embedding.ClaimInput{
			Text:       s.Text,
			Hash:       s.Hash,
			IsName:     s.IsName,
			ValidFrom:  s.ValidFrom,
			ValidUntil: s.ValidUntil,
		}
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

// deltaLinkPredicates are the agent→attached-node link predicates re-subjected
// to the entity in a delta (the node sub-graph is otherwise kept verbatim).
var deltaLinkPredicates = map[string]bool{
	schemaContactPointPred:         true,
	foafAccountPred:                true,
	schemaAddressPred:              true,
	ontology.HasContactPointPlaces: true, // entity → gmeow:PostalAddress
	ontology.LocatedAt:             true, // entity → gmeow:Place (coordinates)
	ontology.HasName:               true, // entity → gmeow:PersonName (appellation)
}

// buildEntityDeltaGraph renders an immutable delta record that PRESERVES the
// node sub-graph (standards-first storage). Only the root agent's own triples
// are re-subjected to the entity ULID; attached schema:ContactPoint /
// foaf:OnlineAccount / schema:PostalAddress nodes (keyed by canonical mailto: /
// tel: / urn: IRIs) are kept verbatim, with their schema:about back-link
// re-pointed to the entity. The delta is scoped to NEW information: nodes whose
// comparison value the resolver reported new, the entity→new-node links, new
// root-direct claims, and the entity type. The four clocks are placed per the
// import-provenance model: carrier + identity on a gmeow:Source, transaction time
// on a gmeow:ImportActivity, a derived recordedNoLaterThan and any source-grounded
// VALID time per claim; the importance claim closes the record.
func buildEntityDeltaGraph(
	entity, root string,
	statements []rdfbundle.Statement,
	annotations []rdfbundle.AnnotationRecord,
	newHashes map[string]bool,
	level int,
	prov SourceProvenance,
) string {
	entityIRI := EntityPrefix + entity

	// Classify the new info: which attached nodes are new, which root-direct
	// comparison claims are new. Per-claim VALID time is inherited from the bearing
	// node (the only clock that gates resolution).
	newNodes := map[string]bool{}
	rootDirect := map[string]bool{}
	validity := bearingNodeValidity(statements, annotations)

	for _, statement := range statements {
		claims, ok := extractClaims(statement)
		if !ok {
			continue
		}
		// A statement is NEW info when any of its comparison claims is new (a name
		// statement yields several tokens; one new token keeps the whole appellation).
		anyNew := false
		for _, claim := range claims {
			if newHashes[claim.Hash] {
				anyNew = true

				break
			}
		}
		if !anyNew {
			continue
		}
		if node := nonRootNodeIRI(statement, root); node != "" {
			newNodes[node] = true
		} else {
			for _, claim := range claims {
				if newHashes[claim.Hash] {
					rootDirect[claim.Hash] = true
				}
			}
		}
	}

	subjectIRI := iri(entityIRI)

	var builder strings.Builder

	writePrefixes(&builder)

	// The four clocks (import-provenance.md): the carrier on a gmeow:Source, the
	// transaction time on a gmeow:ImportActivity, the entity linked to both. mtime
	// is NEVER a claim's validity — it only yields a derived recordedNoLaterThan.
	carrierStamp := typedDateTime(prov.ModifiedAt)
	emitProvenanceNodes(&builder, entityIRI, prov)

	emitted := map[string]bool{}
	// emit writes one kept claim triple plus its per-claim clocks: a derived
	// terminus-ante-quem from the carrier (low confidence) and, when the source
	// grounded it, the VALID-time bounds. No transaction time on a claim.
	emit := func(subjectTerm, predicate, objectTerm, bearing string) {
		triple := subjectTerm + " " + iri(predicate) + " " + objectTerm
		if emitted[triple] {
			return
		}
		emitted[triple] = true
		builder.WriteString(triple + " .\n")

		quoted := "<< " + triple + " >> "
		if !prov.ModifiedAt.IsZero() {
			builder.WriteString(
				quoted + iri(ontology.RecordedNoLaterThan) + " " + carrierStamp + " .\n",
			)
			builder.WriteString(
				quoted + iri(ontology.Confidence) + " " + typedDecimal("0.3") + " .\n",
			)
		}
		if v, ok := validity[bearing]; ok {
			if v.From != "" {
				builder.WriteString(
					quoted + iri(ontology.ValidFrom) + " " + typedDateTimeStr(v.From) + " .\n",
				)
			}
			if v.Until != "" {
				builder.WriteString(
					quoted + iri(ontology.ValidUntil) + " " + typedDateTimeStr(v.Until) + " .\n",
				)
			}
		}
	}

	for _, statement := range statements {
		subject := statement.Subject.Value
		predicate := statement.Predicate.Value

		keep := false
		switch {
		case subject == root && predicate == rdfTypePred:
			keep = true
		case subject == root && deltaLinkPredicates[predicate] && newNodes[statement.Object.Value]:
			keep = true
		case subject == root:
			if claims, ok := extractClaims(statement); ok {
				for _, claim := range claims {
					if rootDirect[claim.Hash] {
						keep = true

						break
					}
				}
			}
		case newNodes[subject]:
			keep = true
		}
		if !keep {
			continue
		}

		subjectText := subjectIRI
		if subject != root {
			subjectText = iri(subject)
		}

		emit(subjectText, predicate, deltaObject(statement.Object, root, entityIRI), subject)
	}

	builder.WriteString(
		BuildRDFStarDelta([]Claim{importanceClaim(entityIRI, level)}),
	)

	return builder.String()
}

// emitProvenanceNodes writes the gmeow:Source (carrier time + identity) and
// gmeow:ImportActivity (transaction time) nodes and links the entity to both. The
// Source is keyed by content digest (its reliable identity); the ImportActivity by
// ingestion time. These are provenance structure, not per-claim clocks.
func emitProvenanceNodes(
	builder *strings.Builder,
	entityIRI string,
	prov SourceProvenance,
) {
	sourceID := prov.ContentDigest
	if sourceID == "" {
		sourceID = shortHash([]byte(prov.Location))
	}
	source := iri("urn:gmeow:source:" + sourceID)
	activity := iri(
		"urn:gmeow:import:" + shortHash([]byte(prov.IngestedAt.UTC().Format(time.RFC3339))),
	)
	entity := iri(entityIRI)

	write := func(s, p, o string) { builder.WriteString(s + " " + iri(p) + " " + o + " .\n") }

	write(source, rdfTypePred, iri(ontology.SourceClass))
	if !prov.ModifiedAt.IsZero() {
		write(source, ontology.SourceModifiedAt, typedDateTime(prov.ModifiedAt))
	}
	if prov.ContentDigest != "" {
		write(source, ontology.ContentDigest, literal(prov.ContentDigest))
	}
	if prov.Location != "" {
		// Carrier storage is a structured gmeow:StorageLocation, not a bare string:
		// medium (local filesystem) + path + service, linked via gmeow:storedIn. This
		// is the surface seam for a later physicalPlace gazetteer step.
		storage := iri("urn:gmeow:storage:" + shortHash([]byte(prov.Location)))
		write(source, ontology.StoredIn, storage)
		write(storage, rdfTypePred, iri(ontology.StorageLocationClass))
		write(storage, ontology.StorageMedium, iri(ontology.StorageMediumLocalFilesyst))
		write(storage, ontology.StoragePath, literal(prov.Location))
		write(storage, ontology.StorageService, literal("local"))
	}

	write(activity, rdfTypePred, iri(ontology.ImportActivityClass))
	write(activity, ontology.IngestedAt, typedDateTime(prov.IngestedAt))

	write(entity, ontology.HasSource, source)
	write(entity, ontology.WasDerivedFrom, source)
	write(entity, ontology.WasGeneratedBy, activity)
}

// nonRootNodeIRI returns the attached-node IRI (subject or object) a statement
// involves other than the root agent, or "" for a root-direct literal claim.
func nonRootNodeIRI(statement rdfbundle.Statement, root string) string {
	if statement.Subject.Kind == "iri" && statement.Subject.Value != root &&
		isAttachedNodeIRI(statement.Subject.Value) {
		return statement.Subject.Value
	}
	if statement.Object.Kind == "iri" && statement.Object.Value != root &&
		isAttachedNodeIRI(statement.Object.Value) {
		return statement.Object.Value
	}

	return ""
}

// isAttachedNodeIRI reports whether an IRI is a contact-point / account /
// address node (a global, canonical sub-node of an agent graph).
func isAttachedNodeIRI(value string) bool {
	return strings.HasPrefix(value, "mailto:") ||
		strings.HasPrefix(value, "tel:") ||
		strings.HasPrefix(value, "urn:gmeow:account:") ||
		strings.HasPrefix(value, "urn:gmeow:addr:") ||
		strings.HasPrefix(value, "urn:gmeow:place:") ||
		strings.HasPrefix(value, "urn:gmeow:name:")
}

// deltaObject renders a statement object for the delta, rewriting a reference to
// the root agent into the entity IRI (e.g. a node's schema:about back-link).
func deltaObject(term rdfbundle.Term, root, entity string) string {
	if term.Kind == "iri" && term.Value == root {
		return iri(entity)
	}

	return renderObjectTerm(term)
}
