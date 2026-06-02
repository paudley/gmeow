# Contact Ingestion Redesign: FILESTORE-Only, Semantic, Complete

## Status / Framing

This is **hardening and consolidation of an existing importer, not a greenfield build**. About
70% already exists in `internal/contactio/contactio.go` (~3,650 lines): vCard / BBDB / CSV /
GEDCOM / Apple-plist / RDF / native parsers that emit RDF/Turtle, unmapped-property rejection, a
cited provider-lifecycle table, some RDF-star, and FILESTORE-only ingest via `source.Ingest`
(`internal/cli/query.go` → `contactio.BuildImportObjectWithOptions` → `service.Ingest`). The
"make importers FILESTORE-only" goal is already met at write time. The remaining work is making
the model below consistent: a shared intermediate claim model, correct entity classification,
logical-contact object identity, version-set/coverage dedupe, and full ontology mapping coverage.

## Summary

Rework contact import so every contact-domain record is either fully normalized into explicit RDF*/RDF-star claims or rejected at the smallest safe record boundary. Importers talk only to FILESTORE. QUERY/projections are never part of import correctness.

## HARD INVARIANT — FILESTORE-only

During import the importer MUST NOT open, read, or write QUERY / PostgreSQL / Apache AGE /
pgvector / any projection, and MUST NOT depend on projection state for any decision (dedupe,
identity, existence). Import correctness — completeness, identity/version-set resolution, dedupe,
rejection — must be **provable from FILESTORE objects + FILESTORE metadata indexes alone**, with
QUERY offline.

This is not stylistic. An early **hybrid FILESTORE/QUERY email importer took >10 hours to import
4k messages; the FILESTORE-only variant took 65 seconds (~550×)**. Per-record QUERY round-trips
plus projection/index contention on the write path are catastrophic at corpus scale (14k+ vCards,
repeated snapshot imports). Every identity/dedupe lookup the importer needs is served by a
FILESTORE metadata index (reused `si/`/`sa/` + version-sets), never by QUERY. Enforced by code
structure (no QUERY client reachable from the import path) and a guard test that runs a full import
with QUERY down.

## Core Model

### Import cardinality is a property of source SHAPE, not the container file

- **Rooted single-contact graph** — RDF/Turtle (and future rich API profiles) that declare a
  primary subject (`schema:mainEntity` / `foaf:primaryTopic`, or the page's `schema:about`). The
  **entire graph is ONE contact** — the importance=1 maximal case. Embedded entities (ancestors
  via `gedcom:`, coauthors, `org:Membership` affiliations, `bibo:` works, `skos:` skills) are that
  contact's relationships/claims, **not** separate contacts. Parsing such a file as more than one
  contact is an **error**. The reference files `~/Active/sites/{paudley,bii}/dist/index.ttl` are
  each exactly one contact (a person; an organization).
- **Collection source** — a vCard file with many `BEGIN:VCARD` records, CSV rows, BBDB records, an
  Apple AddressBook bundle with many person records, or an **un-rooted** FOAF/RDF graph of co-equal
  agents. Each record is a separate contact **or** a locator-only observation.
- The importer chooses the shape from structure (primary-subject declaration vs N co-equal
  records), never from the file path. One file may yield 1 contact or N contacts.

### The stored object is the logical contact, never the container file

Conflating the FILESTORE object with the file it was found in re-creates the "source-record
pseudo-contact" anti-pattern in the storage layer. The model mirrors mail exactly: the *message*
is the object (keyed by Message-ID/content); the *mbox* is an observation record. For contacts:

- **Object = logical contact entity**, keyed by its OWN stable identity:
  - **Agent object** — vCard `UID` / Apple `UID` / GEDCOM xref, or a normalized-content fingerprint
    for id-less agent-denoting records. Content = that agent's RDF* claim graph.
  - **Contact-point object** — normalized email / phone / URL / postal / IM handle. Shared; links
    one-to-many to agents.
  - **Account object** — normalized account handle / profile URL.
  - **Relationship / event / role** — RDF* claims referencing agents, with provenance + temporal.
- **Container file = observation provenance only:** an import-run manifest (accepted/rejected
  counts + reasons + coarse source label) and per-observation coverage records ("this
  contact-version was observed in file X at mtime Y"). Not user-facing contacts.

The model separates:

- **Agents**: people, organizations, families, groups, projects, roles-as-actors where appropriate.
- **Locators/contact points**: email, phone, postal address, website, IM handle, social profile, account id, arbitrary reachable endpoint.
- **Accounts/identifiers**: GitHub account, LinkedIn profile, GEDCOM xref, vCard UID, FOAF account, employee id, source-local ids.
- **Claims**: names, titles, relationships, employment, events, notes, memberships, projects, addresses, contact methods, validity intervals, observation evidence.
- **Import evidence**: quiet metadata like `vcard-historical-import`, import run id, observation time, maybe file/path for debugging.

No field name is special. Email-only and phone-only are both locator-only. Name-only is agent-denoting when the source format asserts a contact/card/person.

## Problems Being Solved

The contact importer needs to handle large, messy, historically accumulated contact data without corrupting the contact graph, losing fields, creating junk contacts, or depending on disposable projections. The existing contact import path and earlier design discussion exposed several concrete problems this redesign must solve.

- **Import authority is wrong unless it is FILESTORE-only.**
  - Contact ingestion must interact directly only with FILESTORE.
  - QUERY/PostgreSQL/projections are disposable views and cannot participate in import correctness, dedupe authority, or validation.
  - Import success must be provable from FILESTORE objects and FILESTORE metadata alone.

- **Partial field ingestion is data loss.**
  - Contact imports must import 100% of contact-domain data for an accepted logical record.
  - Dropping unknown fields, silently ignoring properties, or treating unrecognized input as nonessential violates the import contract.
  - Meeting the requirement by storing an opaque source blob, generic `fields` map, `unknown` bucket, or raw catch-all is also forbidden because it does not create usable normalized data.

- **The all-or-none unit must be the logical record, not necessarily the whole container file.**
  - A vCard collection, Apple export, LinkedIn CSV, GEDCOM file, or BBDB file may contain many independently separable contact records.
  - One unmapped record in a 10,000-record file must not force the other 9,999 valid records to be discarded.
  - The importer still must never partially import the failing record.
  - If record boundaries cannot be proven because the container is structurally corrupt, the safe boundary becomes the file.

- **Source provenance must not dominate the model.**
  - Imported records may all be labeled with coarse evidence such as `vcard-historical-import`.
  - Detailed source bookkeeping is useful for debugging, audit, and observation evidence, but it is not the primary user-facing contact model.
  - The graph should not fill with thousands of source-record pseudo-contacts merely because old files existed.

- **Locator-only records must not create random people.**
  - Email-only, phone-only, address-only, URL-only, IM-only, or social-handle-only records are contact-point observations, not people or organizations.
  - Creating a canonical person/contact for each locator-only record pollutes the graph.
  - A locator can be important and searchable without pretending it denotes an agent.

- **No individual locator type is special.**
  - Email is not a privileged identity signal.
  - Telephone numbers, postal addresses, websites, handles, account URLs, and other locators follow the same semantic rule.
  - A field named `email`, `phone`, `foo`, `bar`, or `yak` has no import meaning until the parser maps it to a documented ontology concept.

- **Names and agent-denoting records need different treatment than locators.**
  - A name-only vCard may still assert a contact/person/card, so it can create a low-information agent.
  - The fact that the agent is weakly identified does not make it a locator.
  - The ontology must distinguish weak agent description from contact-point observation.

- **Dedupe cannot be field equality.**
  - The same email account or phone number can be shared by multiple people or organizations.
  - A historical work email with a bounded validity interval must not cause a new email-only observation to merge into the historical person.
  - Names are weak descriptive claims, not merge keys.
  - Source-local identifiers must not be treated as global identifiers.

- **Temporal semantics are currently easy to conflate.**
  - Observation time, real-world validity, and interaction evidence are different facts.
  - Import time is not real-world validity.
  - File mtime can be useful observation evidence when better source timestamps are absent, but it does not prove when a person used a contact point.
  - Known provider shutdown dates can bound account/contact-point validity, but they do not identify the person.

- **Full-corpus rewrite and cleanup-afterward models do not scale.**
  - Importing 10,000 files into 100,000 contacts cannot create a full canonical snapshot per import.
  - The normal path must touch only input records and relevant identity/contact-point keys.
  - Later query cleanup cannot be the mechanism that makes ingestion correct.

- **RDF*/RDF-star remains required.**
  - Claims need provenance, temporality, confidence/strength, mapping evidence, and import context attached to specific statements.
  - Flattening everything into unqualified triples loses essential semantics.
  - Abandoning RDF* would make contact imports unable to represent the distinctions this design depends on.

- **Standards must come before local invention.**
  - Schema.org, FOAF, vCard RDF, REL, ORG, PROV, DC Terms, TIME, SKOS/OWL, DOAP, GEDCOM-compatible terms, and other established vocabularies should be used first.
  - `gmeow:` terms are allowed only where standards do not cover the needed concept cleanly.
  - Local terms must be documented and reviewed.

- **Format breadth is part of the requirement.**
  - The importer must support the real contact sources in use: vCard, Apple contacts, BBDB, GEDCOM, RDF/Turtle, LinkedIn CSV, and generic CSV with explicit mapping.
  - Gmail import/export/sync should be considered in the ontology and field model even if Gmail sync is implemented later.
  - Analyzer/updater flows for GitHub, social profiles, messaging systems, employment/project discovery, and relationship enrichment must add claims later without rewriting source imports.

## Noted Issues From Design Review

The following issues were explicitly called out and must be treated as design constraints, not optional polish:

- Importers talk only to FILESTORE.
- FILESTORE is authoritative; QUERY is disposable.
- Projections will not be available for ingestion decisions.
- The FILESTORE content model is immutable by digest; mutable FILESTORE metadata indexes already exist and can support lookup/repair workflows.
- Raw source format is not the stored contact model.
- 100% of accepted contact-domain ingestion data must become normalized FILESTORE data.
- Opaque blobs, raw catch-alls, unknown buckets, extra fields, and arbitrary property maps are forbidden as a way to claim coverage.
- Taking everything not already understood and calling it `fields` is forbidden.
- Standards-first ontology is mandatory; local ontology is secondary.
- RDF*/RDF-star is still required.
- Dedupe must not equate email with person.
- Dedupe must not equate phone, address, URL, handle, or arbitrary locator with person.
- Email-only or phone-only records must not create random canonical people.
- Locator-only records still need to be imported completely as locator/contact-point observations.
- Name-only records can create weak agents when the source asserts a contact/person/card.
- Historical contact-point validity must not be overwritten or reopened by later locator observations.
- Observation time, real-world validity, and interaction evidence must remain separate.
- File mtime is relevant as fallback observation evidence, not as proof of real-world validity.
- Known shutdown dates for services such as AIM, ICQ, Myspace-era services, and other discontinued providers should be researched and applied to account/contact-point validity where defensible.
- LinkedIn exports are clear enough to import and should be supported.
- GEDCOM files are clear enough to import and should be supported.
- Mixed dumps may contain non-contact domains; the import rule applies to contact-domain records and contact-attached events/relationships.
- If a contact includes events, relationships, roles, accounts, or temporal details, those are in scope and must be imported.
- Analyzer modules are later updaters that add FILESTORE-backed claims; they are not ingestion-time shortcuts.
- Importance is a 0-10 import/source-level value; imported contact/entity importance becomes the max of existing and import level where an agent exists.
- Side properties such as `hasMet`, `hasWorkedWith`, `hasUsed`, and `hasAgreement` are temporally scoped claims.
- A maximal contact example exists in `~/Active/sites/paudley/dist/index.ttl`, but repository docs must not embed private personal data.
- No PII should be committed in tests or docs.
- The design must avoid creating a filestore garbage dump that relies on query-time cleanup.
- The design must avoid a billion-entry snapshot pattern from repeated imports.
- Import evidence can be coarse, such as tagging a corpus as `vcard-historical-import`.

## Reasoning

The design separates agents, locators, accounts, claims, and evidence because those concepts have different identity, temporal, and dedupe behavior. Treating them as one generic "contact" type causes bad merges, random pseudo-contacts, and loss of meaning.

FILESTORE-only ingestion is necessary because FILESTORE is the durable authority. If import depends on QUERY, then correctness depends on a projection that may be stale, absent, rebuilt, or intentionally discarded. The importer can use FILESTORE-owned metadata indexes because the repository already uses FILESTORE metadata for manifests, source indexes, annotations, cursors, recovery, and locks. Those indexes are operational lookup structures over immutable content, not the canonical contact model.

The logical-record all-or-none boundary preserves data without punishing independent records. Rejecting a whole 10,000-record file because one record has an unmapped property is unnecessary data loss when record boundaries are reliable. Importing part of that bad record would also be data loss. Per-record atomicity is the smallest boundary that satisfies both correctness and practicality.

Opaque raw storage is rejected because it moves the real work out of ingestion and makes later consumers parse historical source formats again. That would satisfy byte retention but fail semantic import. The system needs normalized contact claims that can be searched, exported, reasoned about, and analyzed.

Locator-only records do not create agents because locators are reusable and ambiguous. An email address, phone number, or handle can be shared, reassigned, historical, organizational, or automated. Creating a person from a locator creates false entities. The correct import is to preserve the locator observation and let agent links exist only when the input actually asserts an agent or later evidence creates an explicit identity claim.

Names are different because some source formats use a named card/person record to assert an agent, even if weakly. A name-only vCard is low-confidence agent evidence; an email-only vCard is locator evidence. The distinction comes from source semantics, not from treating one field as magic.

Temporal separation prevents false history. `observedAt` says the source mentioned a claim at a time. `validFrom`/`validUntil` say when the claim was true in the world. Interaction evidence says the system observed actual use, such as messages exchanged. Those facts can support each other, but they are not interchangeable.

RDF*/RDF-star is the right representation because contact facts need statement-level metadata. A single contact point link may have source evidence, observation time, validity bounds, confidence, import importance, and mapping provenance. Reifying that consistently is central to avoiding silent merges and time corruption.

The identity index exists to make ingestion scale without becoming QUERY. Normal imports must lookup only the keys present in the input, such as normalized contact points, source-local ids, and strong account/profile identifiers. Rebuilding the index from RDF* deltas is a repair operation, not the routine import path.

Versioning and low-noise membership records follow the existing mail archive pattern. Repeated imports should preserve coverage/evidence without duplicating complete contact graphs or rewriting the corpus. Meaningful changes become version records or new RDF* claims; exact duplicates become compact coverage.

## Core Rules

- Import boundary is the **logical contact record**, not the whole file, when records can be isolated safely.
- A 10,000-record file may import 9,999 valid records and reject 1 invalid record.
- A rejected record writes nothing for that record.
- If the file structure is corrupt enough that record boundaries cannot be proven, reject the file.
- **Acceptance is predicate-completeness, not value-validity.** The precise rule for a logical
  record R: `accept(R) ⇔ ∀ source-property p ∈ R : mapping(p) ≠ ∅`, where `mapping(p)` is a
  documented predicate (standard-first, or a documented `gmeow:` term). Value validity is
  orthogonal: a *mapped* predicate carrying a junk value (e.g. vCard `TEL:aaron_holmes`) is
  accepted and the value is preserved verbatim as a typed-fallback literal — that is evidence, not
  a rejection trigger. Rejection happens only when a *property/predicate has no mapping at all*.
- No opaque blobs.
- No generic `unknown`, `extra`, `fields`, `raw`, or arbitrary property buckets.
- Unknown source properties require explicit ontology mapping or record rejection — with one
  blessed exception: a **deterministic transform rule** for vendor extension properties.
  `X-<vendor>-<name>` maps by rule to `gmeow:vendorExtension/<vendor>/<name>` (a documented mapping
  with infinite domain — a distinct queryable predicate per input, NOT an opaque `fields` bucket).
  This keeps the long tail of LinkedIn/Outlook/Yahoo/Google/Evolution/Plaxo X- dialects importable
  without a perpetual hand-patched allowlist. A single catch-all literal remains forbidden.
- Source provenance is minimal operational evidence, not a user-facing source-record universe.
- Contact importers must not call QUERY, PostgreSQL, projections, or read projection state.
- FILESTORE remains the authority.

## Ontology Strategy

Use standards first:

- Schema.org for `Person`, `Organization`, `ContactPoint`, `PostalAddress`, `Role`, `Event`, `worksFor`, `affiliation`, URLs, sameAs-style links where appropriate.
- FOAF for people, agents, names, accounts, online accounts, social identity.
- vCard RDF for vCard-native names, telephone, email, addresses, URLs, categories.
- REL for human relationships.
- ORG for employment, membership, organizational roles.
- PROV for claim/evidence provenance.
- DC Terms for labels, descriptions, source metadata, dates.
- TIME for intervals and temporal scopes.
- GEDCOM-compatible terms for family/genealogical structure.
- DOAP for software/projects discovered from GitHub or similar analyzers.
- SKOS/OWL for controlled concepts, equivalence, distinction, candidate identity.

Add `gmeow:` ontology terms only when standards do not express the needed concept cleanly. The
`gmeow:` namespace domain (`blackcatinformatics.ca`) **already publishes the ontology** with full
content negotiation and Cloudflare-worker SPARQL+ endpoints, so `gmeow:` IRIs are dereferenceable
and queryable. That published ontology is the **canonical term authority**: a new `gmeow:` term
must be registered there before code relies on it; the repo's `docs/ontology/` tables are the
per-format mapping layer and an importer-side index, not the term definitions.

For now, only the in-repo mapping-table↔code completeness test runs; validating that every emitted
`gmeow:` term actually resolves in the **published** ontology is **deferred** (decided 2026-06-01).
When added, prefer a committed ontology snapshot for hermetic CI over live SPARQL at test time.

### Contact-domain type closure (collection sources only)

For **collection** sources the importer must decide, per record/subject, whether it denotes an
agent or an attachment. Enumerate two tiers (extends `contactentity.IsContactEntityType`, which
today recognizes only 8 agent types):

- **Agent-denoting types** (create agents): `foaf:Person`, `foaf:Organization`, `schema:Person`,
  `schema:Organization`, `gedcom:Individual`, `gedcom:Family`, `vcard:Individual`,
  `vcard:Organization`, `org:Organization`.
- **Attachment types** (imported iff reachable from an agent; do NOT create standalone agents):
  `schema:ContactPoint`, `schema:PostalAddress`, `foaf:OnlineAccount`, `org:Role`,
  `org:Membership`, `vcard:Email`/`vcard:Telephone`/`vcard:Address`, `time:Instant`/
  `time:Interval` when annotating a contact statement.

This closure is **not** applied to rooted single-contact graphs — those preserve their whole graph
as one contact (semantic superset).

### Per-format ontology mapping tables (`docs/ontology/`)

Every format (and every API field set) gets a documented mapping table — the source of truth that
the Go mapping maps are checked against. Columns:

| source_property | source_example | predicate (IRI) | object_kind | entity_role | temporal | gmeow_reason |
|---|---|---|---|---|---|---|

`entity_role` ∈ {agent-denoting, locator, account, relationship, event, org/role, note,
identifier, evidence} (operationalizes `ClassifyEntityCreation`). `gmeow_reason` is mandatory and
non-empty whenever the predicate is in the `gmeow:` namespace. A completeness test asserts
every key in every Go mapping map appears in its table and vice-versa (generalize
`TestProviderLifecycleTableCarriesSourceMetadata`), so adding a format or API field set becomes:
add rows → test fails until the code matches → implement.

## Entity Creation Rules

Entity creation is decided by an explicit `ClassifyEntityCreation` step over each record's mapped
claims, **for collection sources** (rooted single-contact graphs are one agent by definition and
keep their whole graph).

> **Current bug to fix:** every existing parser hardcodes `a foaf:Person` for every subject —
> including email-only and phone-only records (`vcardToRDF`, `csvToRDF`, `gedcomToRDF`, `bbdbToRDF`).
> A `.vcf` with only `EMAIL:` currently mints a person, directly violating the
> locators-don't-create-agents rule below. `ClassifyEntityCreation` is the corrective: emit
> `foaf:Person`/`Organization` only for agent-denoting records.

- **Agent-denoting records create agents.**
  - vCard with `FN`, `N`, organization card, Apple contact with name/org, GEDCOM individual/family, FOAF person/org, schema Person/Organization, LinkedIn contact with name/profile.
  - Name-only records create low-information agents if the format asserts a contact/person/card.
  - Agent identity strength is weak unless a stronger identifier exists.

- **Locator-only records do not create agents.**
  - Email-only, phone-only, address-only, URL-only, IM-only, social-handle-only records create locator/contact-point/account observations.
  - They remain searchable and exportable as contact points.
  - They do not appear as random person/contact entities.

- **Arbitrary fields do not create entities.**
  - `foo`, `bar`, `yak`, or any unknown CSV/header/property is rejected unless there is an explicit documented mapping.
  - A mapped custom field may create a claim, concept, locator, event, note, role, or relationship depending on semantics.

- **Accounts are not automatically people.**
  - A GitHub account, LinkedIn URL, Matrix ID, AIM handle, etc. creates an account/contact-point identity.
  - It links to an agent only when the source record or existing explicit identity evidence supports that link.

## Dedupe Model

Dedupe splits cleanly across the FILESTORE/QUERY boundary:

- **FILESTORE-side (at import, required):** only two questions, both answered by a FILESTORE
  metadata index (logical-identity key → version-set digest, reusing `si/`/`sa/`):
  "does an agent/contact-point/account with this identity key already exist?" and "is this exact
  record content already observed?" That is all the importer needs and all it may do.
- **QUERY-side (downstream projection, NOT at import):** the richer semantic-merge graph — alias
  resolution, `sameAs`/`possibleMatch`/`distinctFrom`, weak-name non-merging, importance
  max-merge — runs after import as a QUERY projection (`query_contact_aliases`,
  migrations 00012–00014, `refreshContactRollupsTx`) that reads FILESTORE and is fully rebuildable
  from the RDF* objects. The importer never performs or depends on this.

Dedupe is semantic and scoped, not string-equality person merging.

- Contact-point keys:
  - normalized email, phone, URL, postal address, IM handle, social profile, account handle.
  - These dedupe contact points/accounts, not people.
  - One contact point may link to many agents.

- Agent keys:
  - strong global identifiers when the ontology/source semantics say they denote the same agent.
  - source-local identifiers only dedupe inside that source/import namespace.
  - names are weak descriptive claims, not merge keys by themselves.

- Relationship keys:
  - relationships are claims with provenance and temporal scope.
  - GEDCOM family links, employment, association, friendship, membership, project involvement are explicit RDF* claims.

- Conflicts:
  - Conflicting identity evidence creates explicit conflict/distinct/candidate claims.
  - It does not rewrite old data or silently choose a winner.

- Later analysis/updaters:
  - Analyzer output adds new FILESTORE-backed claims.
  - It does not mutate imported source claims.
  - It may assert `sameAs`, `possibleMatch`, `distinctFrom`, employment/project/account evidence, or validity intervals with provenance.

## Temporal Model

Keep three concepts separate:

- **Observation time**:
  - when this source/import provides evidence for the claim.
  - Prefer source-native timestamps like vCard `REV`, export metadata, RDF dates, GEDCOM dates, LinkedIn export metadata.
  - Use file mtime only as fallback observation evidence when source-native time is absent.
  - Store uncertainty when only a broad interval is defensible.

- **Real-world validity**:
  - when the claim was true in the world.
  - Only set `validFrom` / `validUntil` when the source or known provider lifecycle supports it.
  - Do not infer validity from import time alone.

- **Interaction evidence**:
  - email sent/replied, messages exchanged, account scans, GitHub activity, social analysis.
  - Future analyzer/updater work, not contact import work.

Provider lifecycle table:

- Maintain a documented table for discontinued services/accounts where hard bounds are known.
- AIM, ICQ, Myspace-era account semantics, Google profile products, etc. get explicit lifecycle rules only when researched and cited/documented.
- These rules bound contact-point/account validity, not person identity.

### RDF* reification convention

Today RDF-star is emitted in only ~4 places and provenance is per-object, not per-claim. The
convention: the base triple is always emitted plain; an annotation block hangs off the quoted
triple `<< S P O >>` **only when there is provenance / temporal / confidence to attach** (uniform
RDF-star on every triple would 2–3× output size and break the hand-rolled non-streaming Turtle
parser). Annotation predicates, fixed set:

- `prov:wasDerivedFrom <filestore-digest | source-iri>` — provenance (currently missing).
- `prov:generatedAtTime "<import-time>"^^xsd:dateTime` — observation time (distinct from validity).
- `time:hasBeginning` / `time:hasEnd` — real-world validity (already used; keep).
- `gmeow:confidence "<0..1>"^^xsd:decimal`, `gmeow:importLevel "<0..10>"^^xsd:integer` — move
  importance from a flat triple to a per-claim annotation (the "max of existing and import level"
  semantics require per-claim granularity).
- `gmeow:mappedFrom "<source-property>"` — mapping evidence / audit.

`StatementHash` = stable hash of `(S, P, O)` so the same claim reifies to the same node across
imports (also the dedupe/version key). It must stay compatible with the consumer, which keys
annotations by `(SourceDigest, StatementHash)` (`contactentity.applyTemporalAnnotation`).

## FILESTORE Storage Design

Reuse existing FILESTORE primitives. No new storage subsystem, no new transaction model, no new
atomic-write path — the mail importer already does this exact fan-out.

- Content objects (immutable by digest), keyed by **logical-contact identity, never by file**:
  - one **agent object** per logical agent, one **contact-point object** per normalized
    locator, one **account object** per normalized account; relationships/events/roles are RDF*
    claims attached to agents.
  - content = normalized RDF*/RDF-star claims for that entity.
  - 12 snapshots of the same 2,000 people → ~2,000 agent version-sets + a handful of coverage
    records each, **not** 24,000 redundant objects. Cardinality is bounded by distinct contacts —
    the same scale FILESTORE already handles for mail messages.

- Import-run manifest (per file/run, observation provenance only — not a contact):
  - accepted/rejected counts; each rejected record with its exact mapping/parser reason.
  - coarse source label such as `vcard-historical-import`; optional debug file path / run id.

- Coverage / membership records (per observation): "this contact-version was observed in file X at
  mtime Y." Clone of the mail `archive_membership.go` pattern. These, not contact rewrites, absorb
  repeated snapshot imports.

- Contact identity index (the one genuinely-new piece, minimal):
  - a FILESTORE metadata index mapping a **logical-identity key → version-set digest**, reusing the
    `si/`/`sa/` Pebble namespaces with a new `SourceKind` (e.g. `ContactIdentitySourceKind`).
    No bespoke "one-to-many binding" structure — one-to-many agent↔contact-point links are RDF*
    claims, not index entries.
  - touched keys = only the entities present in the input file.
  - updated **atomically with the object Put** via the existing `commitBatch` (manifest + recipe +
    recovery + `si/` + `sa/` in one `pebble.Sync`) — already atomic today, nothing new required.
  - rebuildable via `verify --repair` (which already rebuilds `si/` from provenance).

- Versioning:
  - each logical contact is a **version-set** using the existing `VERSIONING.md` model
    (`VersionSetMetadata` / `VersionRecordMetadata`).
  - identical re-observation → compact coverage record; drift of the same logical entity →
    `minor`/`major` delta against the canonical version.
  - no full-corpus snapshots; no projection cleanup pass required.

## Parser / Mapper Pipeline

Two extension points + one shared serializer (collapsed from the six interfaces an earlier draft
proposed — completeness, classification, and temporal derivation are *properties of* `MapRecord`'s
output, not separate stages):

- `ParseRecords(source) → []IntermediateRecord` — per format AND per API (Gmail/Facebook sync is
  just a new `ParseRecords` impl over the API payload). Also detects source shape (rooted
  single-contact graph vs collection).
- `MapRecord(IntermediateRecord) → []Claim` — `Claim = {subject, predicate, object, objectKind,
  role, provenance, temporal}` (extend `contracts.RDFStatement`, which is almost this). This is
  where completeness (`accept(R)` predicate), entity classification, and temporal derivation are
  decided — they are fields on the emitted claims. Adding a format/API = a new `ParseRecords` +
  rows in its ontology mapping table.
- `BuildRDFStarDelta([]Claim) → object(s)` — single shared, non-overridable serializer; replaces
  today's per-format `strings.Builder` Turtle emission.

Per input file:

1. Detect format AND **source shape** (primary-subject declaration → one rooted contact; else a
   collection of N records).
2. `ParseRecords`: rooted graph → one record carrying the whole graph; collection → N records.
3. `MapRecord` each record into `[]Claim` (typed intermediate model — not a Turtle string).
4. Validate predicate-completeness; a record with any unmapped property is rejected whole.
5. Normalize identity/contact-point keys; look them up in the FILESTORE identity index (touched
   keys only).
6. `BuildRDFStarDelta`: build the logical-contact object(s) with per-claim provenance + temporal
   annotations.
7. Write accepted objects to FILESTORE; update the identity index atomically in the same
   `commitBatch`; write coverage records for re-observations.
8. Record rejected records in the import-run manifest, writing nothing for them.

## Supported Formats

- vCard:
  - Map all vCard properties, parameters, groups, types, PREF, ALTID, PID, GEO, TZ, REV, UID, KIND, MEMBER, RELATED, IMPP, LANG, PHOTO, LOGO, SOUND, KEY, CATEGORIES, NOTE, CLIENTPIDMAP.
  - Unsupported/custom `X-*` properties require explicit mappings before import.

- Apple AddressBook (a **bundle format**, not a single file — `.abbu` directories with multiple
  record types across two generations; the real corpus has 11,238 `.abcdp`, 766 `.abcdg`,
  28 `.abcddb`):
  - `.abcdp` ABPerson binary plist — exists today (`appleAddressBookPersonToRDF`); keep.
  - `.abcdg` ABGroup binary plist — **new parser required.** Carries `GroupName`, `ABMembers`
    (UUID refs to persons), and distribution lists → emit `org:Membership` / `schema:memberOf`
    edges. These are the membership edges the agents-vs-locators model depends on.
  - `.abcddb` SQLite (newer bundles) — decide and document: parse the sqlite (authoritative) vs
    import the derived `.abcdp` mirror (simpler, no sqlite dep, possible field omissions). Record
    the decision and any accepted omissions.
  - `.abcds` / `.abcdi` / `.abcdmr` (saved searches, "me"-record index, modification records) —
    document explicitly as out-of-contact-domain skips.
  - A bundle walker resolves member UID references across files and links `Images/<UUID>` photos.

- BBDB:
  - Build a real parser for records, names, affixes, companies, phones, addresses, net addresses, notes/alists, creation/modification metadata where present.
  - Alist keys must be mapped explicitly.

- GEDCOM:
  - Map individuals, families, names, events, dates, places, relationships, notes, sources as contact-domain family/person graph.
  - Non-contact genealogy constructs can be skipped only when they do not describe contacts/relationships/events attached to contacts.

- LinkedIn CSV:
  - Map names, company, position, profile URL, email, connected-on/export fields, notes if present.
  - LinkedIn profile/account is an account/profile identity; person merge requires explicit agent-denoting evidence.

- RDF/Turtle:
  - Today's importer passes the file through near-verbatim. **That is wrong** — it never parses
    into our normalized local forms, violating "raw source format is not the stored contact model."
  - **Rooted single-contact graph** (declares `schema:mainEntity` / `foaf:primaryTopic` / page
    `schema:about`): the whole graph is ONE contact. Parse every tuple into our normalized local
    RDF* forms; every predicate/type across all its vocabularies (the reference `index.ttl` spans
    ~35: `gedcom:`, `org:`, `bibo:`, `skos:`, `prov:`, `time:`, `dcterms:`, `geo:`, `mads:`,
    `doap:`, `dpv:`, `odrl:`, `gmeow:`, …) must map to a recognized term, else the contact is
    rejected. Success = **semantic superset**: 100% of input tuples represented in the normalized
    contact + added provenance → strict superset. Do not store raw TTL, decompose, or drop tuples.
  - **Un-rooted RDF collection** (co-equal agents, no primary subject): treat as a collection and
    apply the contact-domain type closure to classify agents vs locators per record.
  - Rename any misleading `foaf`-specific functions to RDF/contact-neutral names.

- Generic CSV:
  - Header-driven only when headers are known/mapped.
  - Unknown headers reject the affected record unless a mapping profile is provided.
  - No automatic "field" ingestion.

## Public API / Type Changes

Reuse existing primitives; add only what is genuinely new.

- Contracts:
  - `contact_import_run` (import-run manifest), `contact_import_membership` (coverage record),
    `contact_point`, `contact_account`.
  - contact version-set domains for agent, contact point, account, and relationship (using the
    existing `VersionSetMetadata` / `VersionRecordMetadata`).
  - extend `contracts.RDFStatement` into the importer-side `Claim` (add role / provenance /
    temporal fields).

- FILESTORE — **mostly reuse, do not reinvent:**
  - add a `ContactIdentitySourceKind` and route the identity index through the **existing**
    `LookupSourceObject` / `recordSourceObjectIndexesTo` over `si/`/`sa/`. The proposed
    `LookupContactIdentityKeys` / `WriteContactIdentityBindings` / new atomic-write-path APIs are
    redundant with these + `commitBatch` (already atomic) and should NOT be added.
  - index rebuild reuses `verify --repair` (already rebuilds `si/` from provenance) — no new
    rebuild command.

- Contact mapper seams — two extension points + one shared serializer (NOT six co-equal
  interfaces):
  - `ParseRecords` (format/API specific), `MapRecord(record) → []Claim`, and the shared
    `BuildRDFStarDelta([]Claim)`. Completeness, `ClassifyEntityCreation`, and temporal derivation
    are properties of `MapRecord`'s output, not separate public interfaces.

## Documentation Changes

- Update `AGENTS.md`:
  - importers talk only to FILESTORE.
  - 100% or none applies at smallest safe logical record boundary.
  - no opaque blobs or arbitrary unknown-field buckets.
  - source provenance is minimal evidence metadata.
  - locators do not create agents.
  - standards-first ontology requirement.

- Update architecture docs:
  - contact ingestion architecture.
  - contact identity index as FILESTORE metadata.
  - temporal model.
  - dedupe semantics.
  - analyzer/updater separation.
  - format-specific supported field mappings.

- Add ontology docs:
  - supported external vocabularies.
  - every `gmeow:` term.
  - mapping table per format.

## Test Plan

- FILESTORE-only tests:
  - contact import succeeds with QUERY down.
  - code-level test prevents contact import from depending on QUERY/PostgreSQL.

- Completeness tests:
  - accepted record maps every field.
  - unsupported property rejects only that record.
  - structurally corrupt file rejects the file.
  - no output contains opaque raw/unknown/extra/fields buckets.

- Entity classification tests (collection sources):
  - email-only creates contact-point observation only — **does NOT emit `foaf:Person`**
    (regression test for the current bug).
  - phone-only creates contact-point observation only.
  - URL-only creates contact-point/account observation only.
  - name-only vCard creates low-information agent.
  - arbitrary unmapped `foo` rejects the record; a mapped predicate with a junk value
    (`TEL:aaron_holmes`) is accepted and preserved as a typed-fallback literal.
  - `X-<vendor>-<name>` maps to a distinct `gmeow:vendorExtension/...` predicate, not a bucket.

- Dedupe tests:
  - shared email links multiple agents to one contact point.
  - same phone links multiple agents when evidence says shared.
  - vCard UID dedupes only within correct UID semantics.
  - LinkedIn profile creates account/profile identity.
  - weak name match does not merge.
  - conflicting identity evidence creates conflict claims.

- Temporal tests:
  - vCard `REV` becomes observation evidence.
  - file mtime fallback is used only when source-native time is absent.
  - provider shutdown table sets contact-point/account validity bounds.
  - import time is not used as real-world validity.

- Format tests:
  - representative vCard, Apple (incl. `.abcdg` group → membership edges), BBDB, GEDCOM,
    RDF/Turtle, LinkedIn CSV, generic CSV.
  - largest random vCards from the local corpus; a `BEGIN:VCARD`-collection file yields N contacts.
  - **rooted RDF**: a synthetic fixture structurally mirroring `index.ttl` (multi-vocabulary,
    declares `schema:mainEntity`) imports as **exactly one** contact whose normalized form is a
    **semantic superset** — every input tuple represented after parsing into local RDF* forms,
    every predicate mapped, zero rejects. (Use a synthetic fixture; do not commit the real PII
    file.)
  - **un-rooted RDF collection** of co-equal agents yields N contacts.
  - mapping-table ↔ code completeness: every Go mapping key appears in its `docs/ontology/` table
    and vice-versa.

- Scale tests:
  - 10,000-record file with one bad record imports 9,999 and rejects 1.
  - ≥3 dated `Address_Book-*` snapshots of the same people → distinct-contact object count ≈
    distinct people (not Σ records); re-import writes coverage records, not new version-sets.
  - import work is proportional to input size plus touched identity keys.

## Implementation Checklist

Sequenced into independently shippable phases (reuse existing parsers; do not rewrite them):

- **Phase 0 — Docs:** this revision + `docs/ontology/` mapping-table skeleton + contact-domain
  closure enumeration.
- **Phase 1 — Intermediate `Claim` model + shared serializer:** introduce `Claim` and
  `BuildRDFStarDelta`; refactor existing `vcardToRDF`/`csvToRDF`/etc. to emit `[]Claim` →
  serializer. Behavior-preserving; golden-RDF tests guard output.
- **Phase 2 — `ClassifyEntityCreation`:** locator-only records stop emitting `foaf:Person`
  (correctness fix) + classification tests.
- **Phase 3 — Logical-contact object identity + version-set + coverage records:** split storage
  from container; `ContactIdentitySourceKind` index (reuse `si/`/`sa/`); clone
  `archive_membership.go`/`archive_import.go` fan-out; snapshot-collapse scale test.
- **Phase 4 — Predicate-completeness acceptance + X- transform + per-record reject manifest.**
- **Phase 5 — Rooted-RDF single-contact (semantic superset, ~35-vocabulary mapping coverage) +
  un-rooted collection closure.**
- **Phase 6 — Apple AddressBook bundle:** `.abcdg` groups, bundle walker, `.abcddb` decision.
- **Phase 7 — Move CLI command out of the `query` namespace** (cosmetic; never QUERY-dependent).
- **Phase 8 — API-sync readiness:** confirm `ParseRecords`/`MapRecord` seams; stub a Gmail-People
  mapping table proving API sync is additive.
- Throughout: add tests listed above; run live dev-stack imports into FILESTORE and report
  accepted/rejected counts with FILESTORE object evidence.

## Assumptions

- FILESTORE metadata indexes are allowed because the current FILESTORE design already uses mutable LSM metadata for source indexes, manifests, annotations, cursors, recovery, and locks.
- Contact import source labels can be coarse, such as `vcard-historical-import`.
- Source provenance is retained for evidence/debugging, but it is not a primary user-facing contact model.
- QUERY projection is useful only after import to inspect/search; it is not part of import success.
