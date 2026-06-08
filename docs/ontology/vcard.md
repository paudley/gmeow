<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: AGPL-3.0-only
-->

# vCard → RDF Mapping (skeleton)

Source: vCard 3.0 / 4.0 (RFC 6350). Maps to standard terms first; `gmeow:vcard*` source-preserving
predicates are used where no standard term is a clean fit (see [gmeow-terms](gmeow-terms.md)).

Acceptance is **predicate-completeness**: a record is accepted iff every property has a mapping; a
mapped property with an invalid value (e.g. `TEL:aaron_holmes`) is accepted and the value preserved
as a typed-fallback literal. `X-<vendor>-<name>` properties map by rule to
`gmeow:vendorExtension/<vendor>/<name>`.

Columns: source_property | source_example | predicate | object_kind | entity_role | temporal |
gmeow_reason. (`gmeow_reason` required only for `gmeow:` predicates.)

| source_property | source_example | predicate | object_kind | entity_role | temporal | gmeow_reason |
|---|---|---|---|---|---|---|
| `FN` | `FN:Aaron Holmes` | `schema:name` | literal | agent-denoting | — | — |
| `N` | `N:Holmes;Aaron;;;` | `vcard:hasName` / components | node | agent-denoting | — | — |
| `EMAIL` | `EMAIL:a@b.com` | `schema:email` | email | locator | — | — |
| `TEL` | `TEL:+1 604 …` | `schema:telephone` | tel | locator | — | — |
| `ADR` | `ADR:;;1 Ave;Richmond;…` | `schema:address` → `schema:PostalAddress` | node | locator | — | — |
| `ORG` | `ORG:Blackcat` | `schema:worksFor`/`org:memberOf` | node | org/role | — | — |
| `TITLE` | `TITLE:Director` | `schema:jobTitle` | literal | org/role | — | — |
| `URL` | `URL:https://…` | `schema:url` | iri | locator/account | — | — |
| `IMPP` | `IMPP:aim:…` | `foaf:OnlineAccount` | node | account | validity (provider lifecycle) | — |
| `UID` | `UID:…` | (identity key) | — | identifier | — | — |
| `REV` | `REV:2012-…` | `prov:generatedAtTime` (RDF* annotation) | dateTime | evidence | observation | — |
| `KIND` | `KIND:org` | `rdf:type` selector | — | agent-denoting | — | — |
| `MEMBER` | `MEMBER:urn:uuid:…` | `org:hasMember` | iri | relationship | — | — |
| `RELATED` | `RELATED;TYPE=friend:…` | `rel:*` | iri | relationship | — | — |
| `CATEGORIES` | `CATEGORIES:a,b` | `gmeow:vcardCategories` | literal | note | — | no standard category predicate that fits the source semantics |
| `NOTE` | `NOTE:@work…` | `gmeow:vcardNote` | literal | note | — | preserves free-text note verbatim |
| `X-<vendor>-<name>` | `X-AIM:handle` | `gmeow:vendorExtension/<vendor>/<name>` | literal | (per vendor) | — | deterministic vendor-extension preservation, not an opaque bucket |

> TODO (Phase 1/4): backfill the full vCard property + parameter set from
> `internal/contactio/contactio.go` (`vcardSourcePropertyPredicates`, `vcardParamPredicates`,
> `supportedVCardProperties`) so the completeness test passes. The rows above are representative,
> not exhaustive.
