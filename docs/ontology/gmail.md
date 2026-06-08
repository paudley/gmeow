<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: AGPL-3.0-only
-->

# Gmail People API → RDF Mapping (skeleton)

Source: Google People API `people.connections.list` / `people.get` resources
(`https://people.googleapis.com/v1/people`). This table is the proof that adding API sync is
**additive**: a Gmail sync is a new `ParseRecords` implementation (Google People JSON →
`[]IntermediateRecord`) plus the rows below; it reuses the same `MapRecord` → `[]Claim` →
`BuildRDFStarDelta` pipeline as the file importers. No storage, dedupe, or projection changes are
required — the resulting claims flow through the identical FILESTORE-only path.

Acceptance is predicate-completeness (every populated People field maps or the record is rejected),
identical to the file formats. Observation time comes from `metadata.sources[].updateTime`
(`prov:generatedAtTime`), never from sync time as real-world validity.

| People field (path) | example | predicate | object_kind | entity_role | temporal | gmeow_reason |
|---|---|---|---|---|---|---|
| `resourceName` (`people/c123`) | account id | (identity key) | — | identifier | — | — |
| `names[]` (whole) | "Ada Lovelace" | `gmeow:hasName` → `gmeow:PersonName` | node | agent-denoting | — | reified co-equal appellation (names model) |
| `names[].displayName` | "Ada Lovelace" | `gmeow:fullName` (on the PersonName) | literal | agent-denoting | — | surface form |
| `names[].familyName` | "Lovelace" | `gmeow:hasNamePart` → `gmeow:NamePart` (`namePartType gmeow:namePartSurname` + `gmeow:partText`) | node | agent-denoting | — | typed part — no flat shortcut |
| `names[].givenName` | "Ada" | `gmeow:hasNamePart` → `gmeow:NamePart` (`namePartType gmeow:namePartGiven` + `gmeow:partText`) | node | agent-denoting | — | typed part — no flat shortcut |
| `emailAddresses[].value` | "ada@example.test" | `schema:email` | email | locator | — | — |
| `phoneNumbers[].value` | "+1 555 0100" | `schema:telephone` | tel | locator | — | — |
| `addresses[].formattedValue` | "1 Example St…" | `schema:address` → `schema:PostalAddress` | node | locator | — | — |
| `organizations[].name` | "Analytical Engines" | `schema:worksFor` | node | org/role | — | — |
| `organizations[].title` | "Mathematician" | `schema:jobTitle` | literal | org/role | — | — |
| `urls[].value` | "https://…" | `schema:url` | iri | locator/account | — | — |
| `biographies[].value` | free text | `schema:description` | literal | note | — | — |
| `metadata.sources[].updateTime` | "2024-…Z" | `prov:generatedAtTime` (RDF* annotation) | dateTime | evidence | observation | — |
| `memberships[].contactGroupMembership` | group ref | `foaf:member` (group→person) | iri | relationship | — | — |
| `userDefined[].key`/`.value` | custom field | `gmeow:vendorExtension/google/<key>` | literal | (per field) | — | deterministic vendor-extension preservation, not an opaque bucket |

> TODO (when Gmail sync lands): implement `ParseRecords` for the People API payload (OAuth + paging),
> wire it through the existing mapper, and extend the mapping-table↔code completeness test to cover
> this table. Entity classification (`vcardIsAgentDenoting` analog): a People resource with names is
> agent-denoting; an email-only/phone-only fragment is a locator observation.
