<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: AGPL-3.0-only
-->

# Contact Ontology

This directory is the **source of truth** for how contact-domain source data maps to RDF terms.
The Go mapping maps in `internal/contactio/` are checked against these tables by a completeness
test (see "Completeness contract" below); the docs are authoritative, the code is verified against
them.

See `docs/architecture/CONTACT_INGESTION_REDESIGN.md` for the surrounding design.

## Standards-first

Use established vocabularies before inventing local terms:

| Prefix | Namespace | Used for |
|---|---|---|
| `schema:` | `https://schema.org/` | Person, Organization, ContactPoint, PostalAddress, Role, Event, worksFor, affiliation, URLs |
| `foaf:` | `http://xmlns.com/foaf/0.1/` | people, agents, names, online accounts, social identity |
| `vcard:` | `http://www.w3.org/2006/vcard/ns#` | vCard-native names, tel, email, addresses, URLs, categories |
| `rel:` | `http://purl.org/vocab/relationship/` | human relationships |
| `org:` | `http://www.w3.org/ns/org#` | employment, membership, organizational roles |
| `prov:` | `http://www.w3.org/ns/prov#` | claim/evidence provenance |
| `dcterms:` | `http://purl.org/dc/terms/` | labels, descriptions, source metadata, dates |
| `time:` | `http://www.w3.org/2006/time#` | intervals and temporal scopes |
| `gedcom:` | `http://www.w3.org/2000/10/swap/pim/gedcom#` | family / genealogical structure |
| `doap:` | `http://usefulinc.com/ns/doap#` | software / projects (from analyzers) |
| `skos:`/`owl:` | … | controlled concepts, equivalence, distinction, candidate identity |

Local `gmeow:` terms (namespace `https://blackcatinformatics.ca/gmeow/`) are
allowed **only** where standards do not express the concept cleanly. The namespace domain already
publishes the ontology with content negotiation and Cloudflare-worker SPARQL+ endpoints, so
`gmeow:` IRIs are dereferenceable and queryable — that **published ontology is the canonical term
authority**. A new `gmeow:` term must be registered there before code relies on it;
[`gmeow-terms.md`](gmeow-terms.md) is only the importer-side index of which terms are used and why.

## Contact-domain type closure (collection sources only)

Applied per record/subject for **collection** sources to decide agent vs attachment. NOT applied
to rooted single-contact graphs (those preserve their whole graph as one contact).

- **Agent-denoting** (create agents): `foaf:Person`, `foaf:Organization`, `schema:Person`,
  `schema:Organization`, `gedcom:Individual`, `gedcom:Family`, `vcard:Individual`,
  `vcard:Organization`, `org:Organization`.
- **Attachment** (imported iff reachable from an agent; never standalone agents):
  `schema:ContactPoint`, `schema:PostalAddress`, `foaf:OnlineAccount`, `org:Role`,
  `org:Membership`, `vcard:Email`/`vcard:Telephone`/`vcard:Address`, `time:Instant`/
  `time:Interval` (when annotating a contact statement).

## Per-format mapping tables

One table per source format / API field set. Columns:

| source_property | source_example | predicate (IRI) | object_kind | entity_role | temporal | gmeow_reason |
|---|---|---|---|---|---|---|

- `entity_role` ∈ {agent-denoting, locator, account, relationship, event, org/role, note,
  identifier, evidence}.
- `temporal` ∈ {—, observation, validity} (which temporal slot the value feeds, if any).
- `gmeow_reason` MUST be non-empty whenever the predicate is in the `gmeow:` namespace.

Tables (skeletons; backfill is tracked by the completeness test):

- [vCard](vcard.md)
- Apple AddressBook — TODO
- BBDB — TODO
- GEDCOM — TODO
- CSV (Google / Outlook / Yahoo / LinkedIn / generic) — TODO
- RDF/Turtle vocabularies (the rooted-graph case spans ~35) — TODO
- [Gmail People API](gmail.md) — skeleton; proves API sync is additive (new `ParseRecords` + table)
- Facebook API — TODO

## Completeness contract

A test (generalizing `TestProviderLifecycleTableCarriesSourceMetadata`) asserts that every key in
every Go mapping map appears in its table here, and vice-versa. Adding a format or API field set is
therefore: add rows here → test fails until the code matches → implement.
