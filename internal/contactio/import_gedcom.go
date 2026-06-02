// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"errors"
	"fmt"
	"strings"
)

func gedcomToRDF(content string) ([]renderedContact, []RecordRejection, error) {
	lines, err := parseGEDCOMLines(content)
	if err != nil {
		return nil, nil, err
	}
	individuals, families, err := parseGEDCOMRecords(lines)
	if err != nil {
		return nil, nil, err
	}
	if len(individuals) == 0 {
		return nil, nil, errors.New("GEDCOM import has no individuals")
	}

	bodies := map[string]*strings.Builder{}
	order := []string{}
	bodyFor := func(subject string) *strings.Builder {
		builder := bodies[subject]
		if builder == nil {
			builder = &strings.Builder{}
			bodies[subject] = builder
			order = append(order, subject)
		}

		return builder
	}

	for _, individual := range individuals {
		subject := gedcomSubject(individual.xref)
		builder := bodyFor(subject)
		writeTriple(builder, subject, rdfType, iri(foafPrefix+"Person"))
		writeGEDCOMFacts(builder, subject, individual.facts)
	}
	// Family relationship edges attach to the per-individual record of their
	// subject, so each contact's relationships live in its own delta.
	for _, family := range families {
		writeGEDCOMFamilyEdges(family, bodyFor)
	}

	records := make([]renderedContact, 0, len(order))
	for _, subject := range order {
		records = append(
			records,
			renderedContact{identity: subject, body: bodies[subject].String()},
		)
	}

	return records, nil, nil
}

func parseGEDCOMLines(content string) ([]gedcomLine, error) {
	lines := []gedcomLine{}
	for lineNumber, raw := range strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n") {
		raw = strings.TrimRight(raw, "\r")
		raw = strings.TrimPrefix(raw, "\ufeff")
		if strings.TrimSpace(raw) == "" {
			continue
		}
		parts := strings.SplitN(raw, " ", 3)
		if len(parts) < 2 {
			return nil, fmt.Errorf("GEDCOM line %d is malformed", lineNumber+1)
		}
		var level int
		if _, err := fmt.Sscanf(parts[0], "%d", &level); err != nil {
			return nil, fmt.Errorf("GEDCOM line %d has invalid level", lineNumber+1)
		}
		line := gedcomLine{level: level}
		if strings.HasPrefix(parts[1], "@") && strings.HasSuffix(parts[1], "@") {
			line.xref = parts[1]
			if len(parts) < 3 {
				return nil, fmt.Errorf("GEDCOM line %d is missing tag", lineNumber+1)
			}
			tagParts := strings.SplitN(parts[2], " ", 2)
			line.tag = strings.ToUpper(strings.TrimSpace(tagParts[0]))
			if len(tagParts) == 2 {
				line.value = strings.TrimSpace(tagParts[1])
			}
		} else {
			line.tag = strings.ToUpper(strings.TrimSpace(parts[1]))
			if len(parts) == 3 {
				line.value = strings.TrimSpace(parts[2])
			}
		}
		// GEDCOM uses _-prefixed tags for vendor extensions (like vCard X-);
		// accept them by rule — gedcomPredicate maps them to a deterministic
		// gmeow vendor-extension predicate rather than failing the file.
		if !supportedGEDCOMTags[line.tag] && !strings.HasPrefix(line.tag, "_") {
			return nil, fmt.Errorf(
				"unsupported GEDCOM tag %q on line %d",
				line.tag,
				lineNumber+1,
			)
		}
		lines = append(lines, line)
	}

	return lines, nil
}

func parseGEDCOMRecords(
	lines []gedcomLine,
) ([]gedcomIndividual, []gedcomFamily, error) {
	individuals := []gedcomIndividual{}
	families := []gedcomFamily{}
	for index := 0; index < len(lines); {
		line := lines[index]
		if line.level != 0 {
			index++
			continue
		}
		next := index + 1
		for next < len(lines) && lines[next].level != 0 {
			next++
		}
		facts := gedcomFactsFromLines(lines[index+1 : next])
		switch line.tag {
		case "INDI":
			individuals = append(individuals, gedcomIndividual{xref: line.xref, facts: facts})
		case "FAM":
			families = append(families, gedcomFamily{xref: line.xref, facts: facts})
		}
		index = next
	}

	return individuals, families, nil
}

func gedcomFactsFromLines(lines []gedcomLine) map[string][]gedcomFact {
	root := map[string][]gedcomFact{}
	stack := []*gedcomFact{}
	for _, line := range lines {
		fact := gedcomFact{tag: line.tag, value: line.value}
		for len(stack) > 0 && stack[len(stack)-1].tag != "" && line.level <= len(stack) {
			stack = stack[:len(stack)-1]
		}
		if len(stack) == 0 || line.level == 1 {
			root[fact.tag] = append(root[fact.tag], fact)
			stack = append(stack[:0], &root[fact.tag][len(root[fact.tag])-1])
			continue
		}
		parent := stack[len(stack)-1]
		parent.children = append(parent.children, fact)
		stack = append(stack, &parent.children[len(parent.children)-1])
	}

	return root
}

var supportedGEDCOMTags = map[string]bool{
	"ABBR": true, "ADDR": true, "AUTH": true, "BAPM": true, "BIRT": true, "BURI": true,
	"CHAN": true, "CHAR": true, "CHIL": true, "CHR": true, "CITY": true, "CONC": true,
	"CONT": true, "CTRY": true, "DATE": true, "DEAT": true, "DIV": true,
	"EMAIL": true, "EVEN": true, "FAM": true, "FAMC": true, "FAMS": true,
	"FILE": true, "FORM": true, "GEDC": true, "GIVN": true, "GRAD": true,
	"HEAD": true, "HUSB": true, "INDI": true, "LANG": true, "LATI": true, "LONG": true,
	"MAP": true, "MARR": true, "NAME": true, "NICK": true, "NOTE": true, "NPFX": true,
	"NSFX": true, "OBJE": true, "OCCU": true, "PAGE": true, "PHON": true,
	"PLAC": true, "POST": true, "PUBL": true, "REFN": true, "RESI": true,
	"RFN": true, "SEX": true, "SOUR": true, "STAE": true, "SUBM": true,
	"SURN": true, "TIME": true, "TITL": true, "TRLR": true, "TYPE": true,
	"VERS": true, "WIFE": true, "_FSFTID": true, "_MARNM": true, "_PRIM": true,
}

func writeGEDCOMFacts(
	builder *strings.Builder,
	subject string,
	facts map[string][]gedcomFact,
) {
	for tag, items := range facts {
		for _, fact := range items {
			writeGEDCOMFact(builder, subject, tag, fact)
		}
	}
}

func writeGEDCOMFact(builder *strings.Builder, subject, tag string, fact gedcomFact) {
	if predicate := gedcomPredicate(tag); predicate != "" && fact.value != "" {
		object := literal(fact.value)
		if tag == "EMAIL" {
			if email := normalizedEmailIRI(fact.value); email != "" {
				object = email
			}
		}
		writeTriple(builder, subject, predicate, object)
	}
	for _, child := range fact.children {
		if child.value == "" {
			continue
		}
		predicate := gmeowPrefix + "gedcom" + compactPredicateName(tag+" "+child.tag)
		writeTriple(builder, subject, predicate, literal(child.value))
	}
}

func writeGEDCOMFamilyEdges(
	family gedcomFamily,
	bodyFor func(string) *strings.Builder,
) {
	husbands := gedcomFactValues(family.facts["HUSB"])
	wives := gedcomFactValues(family.facts["WIFE"])
	children := gedcomFactValues(family.facts["CHIL"])
	for _, husband := range husbands {
		for _, wife := range wives {
			h, w := gedcomSubject(husband), gedcomSubject(wife)
			writeTriple(bodyFor(h), h, relPrefix+"spouseOf", iri(w))
			writeTriple(bodyFor(w), w, relPrefix+"spouseOf", iri(h))
		}
	}
	for _, parent := range append(append([]string{}, husbands...), wives...) {
		for _, child := range children {
			p, c := gedcomSubject(parent), gedcomSubject(child)
			writeTriple(bodyFor(p), p, relPrefix+"parentOf", iri(c))
			writeTriple(bodyFor(c), c, relPrefix+"childOf", iri(p))
		}
	}
}

func gedcomFactValues(facts []gedcomFact) []string {
	values := []string{}
	for _, fact := range facts {
		if fact.value != "" {
			values = append(values, fact.value)
		}
	}

	return values
}

func gedcomPredicate(tag string) string {
	switch tag {
	case "NAME":
		return foafPrefix + "name"
	case "EMAIL":
		return schemaPrefix + "email"
	case "PHON":
		return schemaPrefix + "telephone"
	case "ADDR", "RESI":
		return schemaPrefix + "address"
	case "OCCU":
		return schemaPrefix + "jobTitle"
	case "NOTE":
		return schemaPrefix + "description"
	case "BIRT":
		return schemaPrefix + "birthDate"
	case "DEAT":
		return gmeowPrefix + "gedcomDeath"
	default:
		if strings.HasPrefix(tag, "_") {
			return gmeowPrefix + "gedcomVendorExtension/" + strings.TrimPrefix(tag, "_")
		}

		return gmeowPrefix + "gedcom" + compactPredicateName(tag)
	}
}

func gedcomSubject(xref string) string {
	return "urn:gmeow:gedcom:" + shortHash([]byte(strings.TrimSpace(xref)))
}
