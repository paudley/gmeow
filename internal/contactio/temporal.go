// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/ontology"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

// claimValidity holds a node's VALID-time (tenure) bounds as RFC3339 strings
// (empty = unbounded). This is the ONLY clock that reaches resolution's
// co-validity gate (four-clock model: ~/Active/gmeow-ontology/docs/import-
// provenance.md) — assertion / carrier / transaction time are never parsed here.
type claimValidity struct{ From, Until string }

// bearingNodeValidity maps each subject to the VALID-time interval its node
// carries, from the temporal structures a grounded source uses:
//   - schema:startDate / schema:endDate (e.g. index.ttl org:Membership);
//   - reified OWL-Time time:hasBeginning/hasEnd → time:Instant → time:inXSDDateTime;
//   - gmeow:duringInterval → gmeow:TimeInterval (gmeow:startedAtTime/endedAtTime);
//   - gmeow:validFrom / gmeow:validUntil as direct triples OR RDF-star annotations
//     (the canonical light form, incl. the provider-lifecycle dead-service bound).
//
// A claim then inherits its bearing node's interval (attached by subject). Envelope
// formats (vCard) carry none of these, so their claims stay unbounded — correct.
func bearingNodeValidity(
	statements []rdfbundle.Statement,
	annotations []rdfbundle.AnnotationRecord,
) map[string]claimValidity {
	instantTime := map[string]string{}       // time:Instant node → dateTime
	intervalBounds := map[string][2]string{} // gmeow:TimeInterval node → [start,end]

	for _, s := range statements {
		switch s.Predicate.Value {
		case ontology.TimeInXSDDateTime:
			instantTime[s.Subject.Value] = normDateTime(s.Object.Value)
		case ontology.StartedAtTime:
			b := intervalBounds[s.Subject.Value]
			b[0] = normDateTime(s.Object.Value)
			intervalBounds[s.Subject.Value] = b
		case ontology.EndedAtTime:
			b := intervalBounds[s.Subject.Value]
			b[1] = normDateTime(s.Object.Value)
			intervalBounds[s.Subject.Value] = b
		}
	}

	out := map[string]claimValidity{}
	set := func(subject, from, until string) {
		v := out[subject]
		if from != "" {
			v.From = from
		}
		if until != "" {
			v.Until = until
		}
		out[subject] = v
	}

	for _, s := range statements {
		switch s.Predicate.Value {
		case ontology.SchemaStartDate, ontology.ValidFrom:
			set(s.Subject.Value, normDateTime(s.Object.Value), "")
		case ontology.SchemaEndDate, ontology.ValidUntil:
			set(s.Subject.Value, "", normDateTime(s.Object.Value))
		case ontology.TimeHasBeginning:
			// Reified (→ time:Instant → inXSDDateTime) or a direct date literal
			// (the provider-lifecycle form emit.go writes).
			if dt := instantTime[s.Object.Value]; dt != "" {
				set(s.Subject.Value, dt, "")
			} else {
				set(s.Subject.Value, normDateTime(s.Object.Value), "")
			}
		case ontology.TimeHasEnd:
			if dt := instantTime[s.Object.Value]; dt != "" {
				set(s.Subject.Value, "", dt)
			} else {
				set(s.Subject.Value, "", normDateTime(s.Object.Value))
			}
		case ontology.DuringInterval:
			if b, ok := intervalBounds[s.Object.Value]; ok {
				set(s.Subject.Value, b[0], b[1])
			}
		}
	}

	// RDF-star validity annotations attach to their base statement's subject (the
	// node the claim is derived from). time:hasEnd is the legacy provider-lifecycle
	// annotation form, accepted for round-trip.
	for _, a := range annotations {
		switch a.Predicate.Value {
		case ontology.ValidFrom:
			set(a.Statement.Subject.Value, normDateTime(a.Object.Value), "")
		case ontology.ValidUntil, ontology.TimeHasEnd:
			set(a.Statement.Subject.Value, "", normDateTime(a.Object.Value))
		}
	}

	return out
}

// normDateTime canonicalizes a source date/dateTime literal to RFC3339 UTC,
// accepting xsd:dateTime, xsd:date, and partial "2018-05" / "2018" forms
// (interpreted as the start of the period). Unparseable → "" (unbounded).
func normDateTime(value string) string {
	value = strings.TrimSpace(value)
	for _, layout := range []string{
		time.RFC3339, "2006-01-02T15:04:05Z07:00", "2006-01-02", "2006-01", "2006",
	} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}

	return ""
}
