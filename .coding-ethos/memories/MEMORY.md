# Memory


<!-- coding-ethos-memory:c77664a9ea9fb786 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/MEMORY.md

# Memory Index

- [FILESTORE-only import invariant](filestore-only-import-invariant.md) — importers never touch QUERY during import; hybrid was 550× slower (10h vs 65s)
- [Contact ingestion redesign](contact-ingestion-redesign.md) — logical-contact objects, rooted-RDF=one-contact semantic superset, mapping coverage is the real work
- [Ingest-time entity resolution](ingest-time-entity-resolution.md) — LOCKED arch: resolve-then-append immutable delta records into ULID entity graphs; vector cache = new-info detector; supersedes the observation model
- [Published gmeow ontology](published-gmeow-ontology.md) — gmeow: namespace (no bcid) is a live published ontology (content-neg + SPARQL+); it is the canonical term authority, not repo docs
- [Contact corpus location](contact-corpus-location.md) — real test corpus at /home/paudley/Gmail/MailStore2/Archive_Root/PIM_Data/Contacts (~40k files); don't search home dir
- [Contact identity theory](contact-identity-theory.md) — CANONICAL: identity is NOT attribute equality; read CONTACT_IDENTITY_RESOLUTION.md before any resolution proposal
- [Normalize before compare](normalize-before-compare.md) — resolution principle: canonicalize attribute predicates AND values before idDiff, else cross-format claims never match (18k over-split)
- [IM pseudo-email hack](im-pseudo-email-hack.md) — `<jid>@<service>.i.blackcat.ca` are XMPP/IM accounts (XEP-0106 escaped), not emails; type as foaf:OnlineAccount
- [REPAIR is not ingestion](repair-not-ingestion.md) — fix over-merge AT ingest (EMBEDDING-only); REPAIR is a separate offline pass; ingestion reads nothing from QUERY
<!-- /coding-ethos-memory:c77664a9ea9fb786 -->

<!-- coding-ethos-memory:00cc36a80a4e6fd3 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/contact-corpus-location.md

---
name: contact-corpus-location
description: where the real contact ingestion test corpus lives (do not search the home dir for it)
metadata: 
  node_type: memory
  type: project
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

The contact-ingestion test corpus is at
`/home/paudley/Gmail/MailStore2/Archive_Root/PIM_Data/Contacts` (~40k files,
595 MB). Do NOT search the home directory for it — use this path directly.

Composition: 13,580 `.vcf` (Yahoo AB exports 2012/2015 ~8k, per-account dumps
blackcat/lx/cryptolex/strutta, vcf_dump, old_vcards-2009), 11,238 `.abcdp` +
`.abcddb`/`.abcdg` across ~20 overlapping Apple AddressBook `.abbu` snapshots
(2008–2012), plus LinkedIn export, BBDB (`bbdb/small`), CSV, contact JPEGs.

Why it matters: it is the *same ~5k real people* re-exported across many sources
and years, so it is the canonical cross-snapshot dedup test for the
[[contact-ingestion-redesign]] — ingest everything → ~5k entities with heavy
resolve→NOOP overlap. The `TUNE` work (entity count vs ~5k target, threshold
calibration) runs against this. Import formats needed: vcard, apple-addressbook,
apple-addressbook-group, bbdb, csv.
<!-- /coding-ethos-memory:00cc36a80a4e6fd3 -->

<!-- coding-ethos-memory:42f8d4d6c0caafc6 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/contact-identity-theory.md

---
name: contact-identity-theory
description: canonical doc settling that contact identity is NOT attribute equality — read before any entity-resolution proposal
metadata: 
  node_type: memory
  type: reference
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

The canonical, settled treatment of contact entity resolution is
`docs/architecture/CONTACT_IDENTITY_RESOLUTION.md`. **Read it before proposing any change to
contact identity/resolution.**

This exact discussion has recurred 3× across 5 sessions, each time collapsing back to "match on
email/phone/name" and being demolished by counterexample. The doc ends the re-derivation. Key
settled points:

- Identity is **not** attribute equality. No attribute is *necessary* (Paul→Lindsey: name+gender+
  email all change, same person) and none is *sufficient* (shared/transferred identifiers: a spouse's
  old number; `admin@axion.net` inherited by a successor). It is a formal non-identifiability result,
  not a set of edge cases — for any attribute axiom there is a false-merge and a false-split.
- Embeddings are the right *signal* but a fixed cosine threshold is *still* an axiom with the same
  failure (the measured over-split/over-merge in the current engine).
- Correct frame: a **bi-temporal, signed evidence graph** resolved by **global correlation
  clustering**, not pairwise thresholding. Identifiers are time-scoped nodes with ownership timelines;
  a shared value with *disjoint* validity is *negative* evidence.
- Formal core (user-approved): `iClaim`/`iGraph`/`ciGraph` + `idDiff` → confidence with IC (gate),
  IAC (veto), ISD (neutral); per-value `ω` weighting (IDF×kind×source-trust×temporal); `supersedes`
  is first-class (licenses identity transitions like Paul→Lindsey) but auto-population is deferred.
- Two separable concerns: **idempotency** (source-content fingerprint, identity-agnostic, trivially
  correct) vs **resolution** (the hard latent-partition problem).

Do NOT re-propose attribute-key identity (including an "exact strong-claim email/phone index" — a
shared mailbox or transferred number is not identity). Relates to [[contact-ingestion-redesign]] and
[[ingest-time-entity-resolution]] (the current greedy centroid+threshold engine is the §3 "embedding
+threshold is still an axiom" trap; the target is the locked plan's REPAIR/COMM global-clustering arc).
<!-- /coding-ethos-memory:42f8d4d6c0caafc6 -->

<!-- coding-ethos-memory:cead1fe472dba6f0 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/contact-ingestion-redesign.md

---
name: contact-ingestion-redesign
description: "Contact ingestion redesign — key model decisions (logical-contact objects, rooted-RDF=one-contact, mapping coverage)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 102dc081-3795-40be-a817-871164cd980c
---

Active work: hardening the contact importer per `docs/architecture/CONTACT_INGESTION_REDESIGN.md`.
Approved plan: `/home/paudley/.claude/plans/read-docs-architecture-contact-ingestion-agile-goose.md`.
The redesign is ~70% already built in `internal/contactio/contactio.go` — it's hardening +
consolidation, not greenfield.

Load-bearing model decisions (user-confirmed, 2026-06-01):
- **Stored object = the logical contact** (agent / contact-point / account), keyed by its OWN
  stable identity. The container file is NOT the object — it is observation provenance only
  (import-run manifest + per-observation coverage records, cloning the mail
  `archive_membership.go` pattern). Snapshot re-imports of the same people collapse to coverage
  records + one version-set per contact, not N×records deltas.
- **Import cardinality is a property of source SHAPE, not the file.** A *rooted* RDF graph (declares
  `schema:mainEntity`/`foaf:primaryTopic`, e.g. `~/Active/sites/{paudley,bii}/dist/index.ttl`) is
  EXACTLY ONE maximal contact (importance=1 case); its embedded ancestors/coauthors/orgs/works are
  that contact's claims, not separate contacts. A *collection* (N `BEGIN:VCARD`, CSV rows, Apple
  bundle, un-rooted FOAF) yields N contacts.
- **Rooted-RDF success = SEMANTIC SUPERSET, not verbatim storage.** Parse every tuple into our
  normalized local RDF* forms (every predicate/type across ~35 vocabularies must map to a
  recognized term or reject); 100% of input tuples must be REPRESENTED in the final contact + added
  provenance → strict superset. Never store raw TTL, never decompose, never drop tuples. The
  current near-passthrough RDF importer is wrong (never normalizes into local forms).
- Importers are FILESTORE-only — see [[filestore-only-import-invariant]].

The dominant effort is **mapping coverage** (~35 vocabularies), not FILESTORE wiring (which reuses
existing `si/`/`sa/` + version-set + `commitBatch` primitives — no new storage subsystem).

## Progress (2026-06-01)

Done + verified (build/vet/gofmt/tests green):
- Phase 1 seam: `internal/contactio/claim.go` — `Claim` model + `BuildRDFStarDelta` serializer;
  vCard + native import, FOAF export, importance claims migrated onto it (byte-equiv tests in
  claim_test.go). Apple/CSV/GEDCOM/BBDB still emit via writeTriple (correct; migrate when later
  phases need richer terms).
- Namespace fully de-`bcid`'d → `gmeow:` = `https://blackcatinformatics.ca/gmeow/`.
- Phase 4 (partial): vCard `X-<vendor>-<name>` → `gmeow:vendorExtension/<vendor>/<name>` rule;
  GEDCOM `_`-prefixed vendor tags → `gmeow:gedcomVendorExtension/<tag>`.
- **vCard UTF-16 decode bug fixed** (used importText, now importDecodedText like CSV/GEDCOM) — this
  was 91% of vCard failures.

Corpus dry-run (`TestCorpusDryRun`, env-gated by `GMEOW_CORPUS_DIR`, read-only, no FILESTORE) over
`~/Gmail/MailStore2/Archive_Root/PIM_Data/Contacts`: apple-addressbook 11238/11238, vcard
13471/13580 (99.2%, ~96k contacts), csv 51/53 (~46k contacts), gedcom 1/1, bbdb 1/1. ~154k contacts
parse total.

Then added + verified:
- **Per-record rejection** (Phase 4 core): `parseVCards` now poisons+skips a damaged card and
  records a `contactio.RecordRejection` (in `ImportResult.Rejected`) instead of failing the file.
  Recovered ~10k contacts from mega-exports. vCard now 13579/13580 files; ~105,784 contacts.
- **Phase 2 (vCard)**: `vcardIsAgentDenoting` — locator-only cards (email/phone/URL-only, no
  name/ORG/KIND) no longer emit `a foaf:Person`; only agent-denoting cards do.

Corpus parse coverage is now effectively COMPLETE: apple 11238/11238, vcard 13579/13580 (the 1 =
a backup with no BEGIN:VCARD), csv 51/53 (the 2 = header-only empty files), gedcom 1/1, bbdb 1/1.
~155k contacts total. The `TestCorpusDryRun` harness reports rejected-record counts.

Phase 6 DONE: Apple `.abcdg` group parser (`appleAddressBookGroupToRDF`) → foaf:Group + foaf:member
edges + distribution-list claims; ABSmartGroup (saved searches) cleanly skipped via
`errSkipNonContactDomain`. Dry-run: 580 real groups import, 186 smart-groups skipped. Projection
recognizes foaf:Group + gedcom:Family (added to `IsContactEntityType`). User directive: finish ALL
phases — the trial import will exercise 100% including QUERY projections.

Phase 5 DONE: rooted-RDF root detection — schema:mainEntity/foaf:primaryTopic → exactly ONE contact
(validated: real index.ttl→#paudley, bii→#bii); un-rooted → N contacts. RDF bundle kept as
strict-superset passthrough (preserves 100% of tuples = the stated success criterion); full
re-serialization through a normalizer deferred (needs a robust Turtle-star parser; doing it wrong
risks dropping tuples).

Phase 7 DONE: import/export moved to top-level `gmeow-admin contact import|export` (FILESTORE-only),
out of the `query` projection namespace.
Phase 8 DONE: `docs/ontology/gmail.md` Gmail People API mapping stub (API sync = new ParseRecords +
table, reuses the pipeline).
Trial enablement DONE: `contactio.FormatForPath` + CLI `--format auto` per-file detection + directory
recursion + `ErrSkipNonContactDomain` (exported) skip handling — so a mixed corpus dir imports.

Phase 3 DONE (per-logical-contact storage): contactio now produces per-contact records
(`renderedContact`) and exposes `BuildContactDeltas` → `[]ContactDelta` (one standalone Turtle
object per logical contact: prefixes + body + its import-level claim). ALL parsers refactored to
per-contact (vCard/native/CSV/BBDB/Apple-person/Apple-group one-per-contact; GEDCOM routes family
edges into each individual's record; RDF = one body, rooted→1 contact). `BuildImportObjectWithOptions`
still assembles the whole-file bundle from the same records (back-compat; Contains-based tests pass).
CLI `importContactPath` ingests EACH delta as its own FILESTORE object keyed by contact identity
(ExternalID=identity, ExternalVer=content fingerprint → dedup/version via si/sa across snapshots),
SourceHint=file (provenance), + a `VersionSetFacetKind` facet (domain `ContactVersionSetDomain`).
Per-record rejections (`ImportResult.Rejected`) are the import-run manifest. Contracts added:
`ContactIdentitySourceKind`, `ContactVersionSetDomain`.

ALL 8 phases now done + bcid→gmeow rename + UTF-16 fix + trial enablement. Whole module builds,
gofmt clean, `go test ./...` green, corpus dry-run unchanged (~155k contacts + 580 groups).

Remaining (minor): Phase 2 classification for csv/gedcom/bbdb/apple/native (low impact — those
records carry names; vCard, the locator-only-heavy format, is done).

## Live FILESTORE import test (2026-06-02, gmeow-base)

Stack topology: PROD = ~/Active.running/gmeow + /etc/gmeow/gmeow.toml + gmeow-prod DB (systemd
units gmeow-*.service, also run manually sometimes). BASE/test = ~/Active/gmeow + ./gmeow.toml +
gmeow-base DB. Default RPC sockets are SHARED (/run/gmeow/{filestore,query,scheduler}.sock) — added
a `[rpc]` section to gmeow.toml pinning gmeow-base to `/run/gmeow/base-*.sock` for isolation. Run
base services manually: `nohup ./bin/gmeow --config gmeow.toml {filestore,query,scheduler}-serve &`.
Passwordless sudo works; gmeow-base Postgres password is SOPS-encrypted (binaries decrypt). QUERY
migrate/rebuild: `gmeow-admin query rebuild --config gmeow.toml --confirm-instance local` (truncates
+ reprojects; deadlocks if scheduler is concurrently projecting — stop scheduler first).

**BUGS found + fixed during the test:**
1. **Per-contact delta ingest dropped the contact facets** → projection extracted 0 facts and
   importance 0 for ALL contacts. Root cause: CLI attached only the version-set facet, not the
   `contact_entity`/`RDFSourceBundle` facets (with `root_subject`) that the projection
   (`rdfContactRootSubjectsForSourceTx`, rdf.go) needs. Fix: `ContactDelta.Facets =
   importFacets(format, []string{identity}, level)`, CLI appends version-set facet. After fix:
   importance + facts project correctly (andrew@3 → imp 3/5 facts; #paudley@10 → imp 10/81 facts).
2. **Content-changed re-import is deduped (first-write-wins by source ALIAS kind/name/id).** A
   re-import of the same identity with different content (e.g. importance 1→10) returns the
   existing object (created=false) — the change is NOT versioned. So version-on-change is NOT
   implemented (plain Ingest alias-dedups). Max-merge works only across DIFFERENT source-names.
   Snapshot dedup (identical content, same source) works as designed. Known Phase 3 limitation.

Also: gmeow-base had STALE prior-import data (source `contacts-fixed-20260601`, old per-file-bundle
model) — wiped `./data/filestore` + `query rebuild` for a clean test (user authorized wiping ./data).

**END-TO-END TEST SUCCEEDED (clean run):** corpus `--source-name pim-corpus --import-level 3` +
index.ttl `--source-name lod-profiles --import-level 10`. Result: 25,639 files → **35,999 distinct
contacts** (164k refs deduped — snapshot dedup works), 369 rejected records, 186 smart-groups
skipped, 3 legit errors (2 empty CSV + 1 non-vcard). All 36,002 objects projected into QUERY;
search surfaces them (andrew 65, linkedin 234, smith 36…); #paudley → imp 10 / 81 facts, #bii →
imp 10 / 14 facts. **zstd dictionary trained from corpus**: `filestore train-dictionary
--all-families --confirm-instance local` → family rdf-turtle, dict id 1006, 113KB, 34,815 samples,
installed as current-by-family/rdf-turtle.

**OPEN ISSUES to address (per-contact granularity is FIXED/non-negotiable per user):**
- **Metadata bloat**: 145M filestore = 25M content + 120M Pebble metadata (4:1). Each of 36k
  objects carries 3 facets (contact_entity + rdf_source_bundle + version_set) + manifest +
  provenance + recipe + si/sa + recovery. Trim: drop `rdf_source_bundle` (projection accepts
  `contact_entity` OR it — rdf.go:376), drop `version_set` (versioning-on-change not implemented),
  trim contact_entity metadata to root_subject+format. Keeps granularity.
- **Slow projection**: ~10 objects/sec (36k → ~60 min) via project-changed; per-contact objects
  multiply projection work (parse RDF + facts + upserts per object).
- **No version-on-change**: re-import of same identity with changed content alias-dedups
  (first-write-wins); not versioned. Max-merge only across different source-names.
- **Operational gotchas (this session)**: `query rebuild` TRUNCATE blocks behind ANY concurrent
  Postgres reader (query-serve, or a `watch query breakdown`) — stop readers or use
  `project-changed` (UPSERT, lock-compatible). Don't `kill -9` a rebuild mid-TRUNCATE (orphans a
  lock). `query breakdown`/`contact *` CLI go through query-serve (base-query.sock); admin
  `migrate`/`rebuild`/`project-changed`/`train-dictionary` connect to Postgres directly.

## Pre-deploy ENTITY-RESOLUTION redesign approved (2026-06-02) — plan `~/.claude/plans/floofy-frolicking-badger.md`

Dedup failed (35,999 contacts for ~5k people; corpus ~6,390 distinct emails, ~7,116 names). Root
cause: identity keyed by attributes (per-export UID + content-fp), but emails AND names are
temporal/multi-valued/transferable — NO stable attribute key. New model: entity = a CLUSTER in high-d
embedding space, UUID anchored to centroid; resolution is a DOWNSTREAM analyzer (entity-centric,
bi-temporal + per-attribute decay, dense-retriever blocking via in-process `github.com/coder/hnsw`
persisted as a FILESTORE artifact, GNN communication-graph overlap). Multi-vector entity = profile
centroid (claim mean-pool) + a SET of name vectors (renames add vectors; layered match
128-d→768-d→nearest-name). New **EMBEDDING service** (gRPC; HTTP→endpoint :8090; batch/multiplex/cache
claim vectors by statement hash) — IMPORT/FILESTORE may call it (it's not QUERY/SCHEDULER, so the ban
holds). nomic-embed-v1.5 is **Matryoshka** (128-d coarse / 768-d fine by slicing). Embed CLAIMS not
profiles → contact vector = weighted mean-pool (incremental, no re-embed on change). Tasks #1-9.

**F3 DONE (2026-06-02): facet trim, 3→1 per object.** CORRECTION to the plan note: keep
`rdf_source_bundle` (NOT contact_entity) — it is load-bearing (triggers RDF parsing at
`query/postgres/rdf.go:80` AND carries `root_subject`). `contact_entity` was only a redundant
root-detection alternative (rdf.go:376; `ContactSourceRole` already marks contacts); `version_set`
was dead (no consumer in internal/query or internal/analysis). Edits: `importFacets`
(contactio.go) → single rdf_source_bundle facet; removed `contactDeltaFacets` + version_set append
(cli/query.go); re-added `ImportResult.ObjectDigest`. Verified live: a 1-facet object projects
name + importance(4, max-merged across source-names) + 7 facts. Full `go test ./...` green.
**>>> SUPERSEDED 2026-06-02 by [[ingest-time-entity-resolution]] <<<** The whole observation model
below (F1a observation-identity, 62k observations, downstream collapse) is obsolete. New locked model:
ingest-time entity resolution — resolve incoming claims against existing entity centroids (in-process
HNSW), write only IMMUTABLE delta records (new claims + provenance), entity = ordered stacked graph,
ULID ids, vector-cache = new-info detector, NOOP when no new claims. R0 (EMBEDDING+cache+HNSW) is now
built FIRST as an ingest prerequisite. (The observation-fingerprint code survives only as the
claim-level diff that decides delta-vs-NOOP.) Historical record of the obsolete approach follows:

**F1a (OBSOLETE): vCard observation identity.** `contactSubjectForVCard` now mints a
stable OBSERVATION id = `urn:gmeow:observation:<hash of meaningful claims>` (excludes volatile
UID/REV/VERSION/timestamps/ETags/binary via `volatileVCardProperties`/`isVolatileVCardProperty`;
normalizes+sorts+dedups values via `vcardObservationIdentity`/`normalizeFingerprintValue`). Dropped
email-first + UID-first + content-fp keying; removed unused `prefixedFirstValue`. Email still emitted
as a vcard:hasEmail claim. Dry-run now reports DISTINCT counts.

**KEY DYNAMIC measured:** vCard → **62,175 distinct observations** (UP from ~36k). This is correct &
expected — email-keying was masking content variation; honest observations ≫ entities. So **F1 alone
INCREASES the raw count; the collapse to ~5k is R1 (embedding clustering), not the import.** F1+R0+R1
must land together to show the dedup win. Some 62k inflation is format-noise over-sensitivity (extra
X-/NOTE variation not yet in the volatile set) — tunable, but R1 is the real collapse. Apple/CSV NOT
yet switched to observations (apple still 1,894 distinct via email-keying). Live gmeow-base still has
the OLD 36k (F1a only affects new imports). F1 still needs: apple/csv observation identity, shared
contact-point objects.

Remaining: finish F1 (apple/csv observations + shared contact-points), F2 (bi-temporal + decay), F4
(scalability), R0 (EMBEDDING + coder/hnsw), R1 (clustering → ~5k), R2 (comm-graph), R3 (decay/splits),
R4 (ingest anchoring).
<!-- /coding-ethos-memory:cead1fe472dba6f0 -->

<!-- coding-ethos-memory:4b0d252e24cc5805 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/filestore-only-import-invariant.md

---
name: filestore-only-import-invariant
description: Importers must talk ONLY to FILESTORE during import — never QUERY/Postgres — with hard measured perf evidence
metadata: 
  node_type: memory
  type: project
  originSessionId: 102dc081-3795-40be-a817-871164cd980c
---

Importers (mail, contacts, and all future sources incl. Gmail/Facebook API sync) MUST talk
ONLY to FILESTORE during import. They must never open/read/write QUERY, PostgreSQL, Apache AGE,
or pgvector, and must never depend on projection state for any import decision (dedupe, identity,
existence checks). Import correctness must be provable from FILESTORE objects + FILESTORE metadata
indexes alone, with QUERY offline.

**Why:** FILESTORE is the authority; QUERY is a disposable projection derived from it. Letting the
authority's contents depend on its own derivative creates a dependency cycle that breaks
rebuildability (QUERY can no longer be reconstructed from FILESTORE), determinism/provability (same
input → different result depending on stale/absent projection), and introduces async races
(projection refresh runs later via SCHEDULER). Concretely measured: an early **hybrid
FILESTORE/QUERY email importer took >10 hours to import 4k messages; the FILESTORE-only variant
took 65 seconds (~550×)** — per-record QUERY round-trips + projection/index contention on the write
path are catastrophic at corpus scale.

**How to apply:** All identity/dedupe lookups the importer needs are served by a FILESTORE metadata
index (reuse the `si/`/`sa/` Pebble namespaces + version-sets, like mail Message-ID). Keep no QUERY
client reachable from the import code path; add a guard test that runs a full import with QUERY
down. The semantic-merge graph (alias/`sameAs`/`distinctFrom`/importance-max) is a SEPARATE
downstream QUERY projection that runs after import and reads FILESTORE — the importer never invokes
it. See [[contact-ingestion-redesign]].
<!-- /coding-ethos-memory:4b0d252e24cc5805 -->

<!-- coding-ethos-memory:55b66720ffc547d4 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/im-pseudo-email-hack.md

---
name: im-pseudo-email-hack
description: "<handle>@<service>.i.blackcat.ca addresses are IM/XMPP accounts, not real emails (user's long-running hack)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

In the user's contact/mail corpus, addresses of the form `<escaped-jid>@<service>.i.blackcat.ca`
are NOT real email addresses — they are IM accounts (XMPP/JID form) that the user routed through
an email gateway for years, so they ended up stored as emails.

- The subdomain names the service: `fb` (Facebook chat), `msn`, `icq`, `gtalk`, `aim`, etc.
  e.g. `…@fb.i.blackcat.ca`, `…@msn.i.blackcat.ca`, `102998083@icq.i.blackcat.ca`.
- The local part is the real IM handle/JID, **XEP-0106 JID-escaped**: `\40`→`@`, `\5c`→`\`, `\20`→space.
  In the corpus the escapes themselves are often re-encoded (`%5c40` or `&#92;40` == `\40` == `@`),
  e.g. `bill.trembley%5c40gmail.com@msn.i.blackcat.ca` = the JID `bill.trembley@gmail.com`.

**Why:** treating these as `schema:email` mis-types them and pollutes the email identifier index with
IM handles; correct typing is `foaf:OnlineAccount` (concept `account`) with the service from the
subdomain. **How to apply:** in normalization/extraction, detect `*@*.i.blackcat.ca`, map subdomain→
service (reuse `imAccountServices`), XEP-0106-unescape the local part to recover the JID, and route to
the account concept — do NOT index as email. Fold into the deferred IM→`foaf:OnlineAccount` migration.
Related: [[normalize-before-compare]], [[published-gmeow-ontology]], [[contact-ingestion-redesign]].
<!-- /coding-ethos-memory:55b66720ffc547d4 -->

<!-- coding-ethos-memory:debdf1d09963c677 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/ingest-time-entity-resolution.md

---
name: ingest-time-entity-resolution
description: "LOCKED contact architecture (2026-06-02): ingest-time entity resolution, immutable delta records stacked into entity RDF* graphs, ULID ids, vector cache as new-info detector"
metadata: 
  node_type: memory
  type: project
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

**This is the locked contact architecture (user-decided 2026-06-02). It SUPERSEDES the two-layer
observation model in [[contact-ingestion-redesign]] (F1a observation-identity / 62k-observations work
is obsolete — we never store observations to collapse later; we resolve-then-append).**

> **UPDATE 2026-06-03 — the idDiff engine is now IMPLEMENTED** (commits 757d1ad doc, 1257057 code).
> The greedy centroid+threshold+name-gate matcher described below is REPLACED by the signed,
> weighted `idDiff` model (`internal/embedding/{claim,omega,iddiff}.go`): structured claims with
> attribute KIND (functional/set/contextual), per-VALUE embedding (cache keyed by value-hash, state
> magic GMEOWSTATE2), ω = kindBase + idf·kindScale, idDiff with IC-gate/IAC-veto/ISD-neutral +
> temporal + supersedes gating, HNSW used only for blocking (centroid is a recall key, no longer the
> decision). Idempotency is now exact via (a) entity-independent observation fingerprint as the
> FILESTORE source-dedup ExternalID and (b) a Service observation→entity memo. Temporal/supersedes
> are represented + gated but not yet populated from import (degrade to no-op); global
> correlation-clustering REPAIR is the deferred `ScoreEdges` seam. VALIDATED on the 13,459-vCard
> corpus: cold → 8,062 entities (was 10,417); FULL re-import → 0 new entities, 0 new embeds (old
> engine over-minted +135..+192 per dir). Tunables on Service: mergeGate 0.75, lambda 1.5, vetoMass
> 4.0, blockingTopN 24, tauCtx 0.6 (TauSet=match-threshold, TauFunc=name-threshold).
>
> The CURRENT greedy centroid+threshold engine described below is only the *ingest mechanics*. What
> "the same contact" formally MEANS is settled in [[contact-identity-theory]] →
> `docs/architecture/CONTACT_IDENTITY_RESOLUTION.md` (identity ≠ attribute equality; signed
> evidence-graph + global correlation clustering is the target). Read it before any resolution change.

## Model

- **Storage unit = the entity** (a resolved logical contact), id = **ULID** (time-sortable, not UUID).
  There are NO card objects, NO observation objects, NO atom/value objects.
- **Records are IMMUTABLE.** Each ingest that carries NEW information writes one immutable,
  content-addressed **delta record**: only the new claims + provenance + `(first_seen, last_seen)`
  RDF* annotations, linked to an entity ULID. Records are the append-only authoritative log.
- **An entity's full graph = the ordered STACK of its records** (fold in ULID/time order, applying
  supersedes/replaces/valid-time as RDF* annotations dictate). Entity graph, profile centroid, and
  name-vectors are all **materialized views** — rebuildable from records, so repair/retuning never
  corrupts source truth (event-sourced).
- **Rich content (PGP key, avatar) is a big prop or an attachment-part** (handled like email
  attachments) — props are NOT size-constrained (index.ttl has huge props in a massive graph). This
  removes the last reason to ever mint a value/atom object.

## Ingest flow (FILESTORE + EMBEDDING + in-process HNSW only — QUERY-ban intact)

1. Parse source → normalize to our RDF* forms → claim set.
2. For each distinct claim, look up its vector in the **claim-vector cache** (keyed by statement
   hash). Cache MISS = a genuinely-new claim → batch-embed (the embedder is hit ONLY here).
3. Mean-pool the claim vectors → candidate vector.
4. In-process **`coder/hnsw`** nearest-centroid (128-d Matryoshka coarse → 768-d fine → nearest
   name-vector) loaded from a FILESTORE artifact → candidate entity ULID(s).
5. Score vs threshold: high → that entity; none → mint new ULID; ambiguous → provisional + flag.
6. Diff incoming claims vs the entity's materialized graph → **delta**. Empty delta → **NOOP** (zero
   records written). Else write the immutable delta record + provenance; incrementally re-pool the
   centroid; update HNSW + name-vector index.

## Why this is fast (dissolves the 550× trap)

**The claim-vector cache IS the new-information detector.** A redundant re-ingest finds every
statement-hash already cached → ZERO embedder calls → and the graph-diff is simultaneously empty →
ZERO records written. The embedder fires only for new claims, which is the only case that yields a
delta. Cost ∝ NEW information, not volume ingested. So ingest-time resolution does not reintroduce
the [[filestore-only-import-invariant]] latency disaster — embedding is write-time, batched (embedder
prefers batches; cache ~10k), and mostly cache hits.

## Worked example (empty store)

1. `paudley/dist/index.ttl` → normalize → embed → mean-pool → ONE entity (ULID + full RDF* graph +
   vectors); seed HNSW.
2. 2002 vCard with a new email → matches paudley centroid → delta = {new email} → write one delta
   record (new email + provenance) → re-pool.
3. 2004 vCard, nothing new → matches → empty delta → NOOP.

## Open / deferred

- **Match threshold + confidence bands** are DATA-DRIVEN: tune by a post-ingest run over the corpus
  (~5k real entities target). No a-priori value.
- **Repair pass** (downstream, was R3): revisable splits/merges by REASSIGNING immutable records to
  different entities (records never mutate) + re-pool. Greedy-at-ingest + revisable-downstream.
- **Comm-graph (GNN) signal** (was R2): AGE `:co_occurred` overlap to catch name+email co-change.
- New deps: `github.com/oklog/ulid/v2`, `github.com/coder/hnsw`.
- New EMBEDDING service: gRPC to IMPORT/FILESTORE; HTTP to endpoint :8090; batch/multiplex/cache by
  statement hash; full 768-d + 128-d Matryoshka slices. Wraps `internal/analysis/embeddings.go`.

Build order (revised): **R0 (EMBEDDING + cache + HNSW) is now an INGEST PREREQUISITE, built first**,
then the ingest path rewrite (resolve→delta→NOOP, ULID, stacked graph), then corpus tuning, then
repair + comm-graph.

## R0 DONE (2026-06-02) — EMBEDDING service built + tested

`internal/embedding/` package: `embedding.go` (Vector=[]float32, StatementHash, Normalize, Slice
[Matryoshka 768→128 renormalized], MeanPool [weighted, unit-len], MemoryCache), `embedder.go`
(HTTPEmbedder batches OpenAI-style array input to endpoint; Resolver embeds ONLY cache misses, returns
miss-count = delta/NOOP signal), `index.go` (EntityIndex = 3 coder/hnsw graphs coarse/fine/names;
layered Search 128→768; NearestName; Snapshot/LoadEntityIndex = single versioned FILESTORE artifact),
`service.go` (Service facade). gRPC: `proto/gmeow/v1/embedding.proto` (Embed/Pool/Match/Upsert/
NearestName/Snapshot/Status), `internal/rpc/embedding_{server,client}.go`, CLI `embedding-serve`
(common.go), config `RPC.Embedding` (+resolved/validate/default `/run/gmeow/embedding.sock`),
`gmeow.toml [rpc.embedding]` = base-embedding.sock, `deploy/systemd/gmeow-embedding.service`. Deps
added: coder/hnsw v0.6.1, oklog/ulid/v2 v2.1.1. Tests green: cache-is-NOOP-detector (re-ingest=0
embeds), HNSW layered match + artifact round-trip, gRPC round-trip. Live smoke: binds base-embedding.sock.
Note: `embedding.Service` is usable IN-PROCESS (Index()/Resolver()) so I1 can use it directly, then
swap to the gRPC `EmbeddingClient` (same method shapes) with no caller change.

## I1 CORE DONE (2026-06-02) — resolution decision logic proven

`internal/contactio/resolve.go`: `claimStatement{Text,Hash,Line,IsName}` (Hash = subject-INDEPENDENT
predicate+object hash, so same email/name collides across observations), `EntityLedger` (entity→
{hash→text} materialized claim set, rebuildable from records), `EntityResolver.Resolve` = Pool→Match
(threshold)→diff vs ledger→{mint new ULID | append delta | NOOP}; on non-NOOP re-pools centroid over
ALL entity claims (cache hits) + adds name vectors + Upserts index. `resolverEmbedder` interface =
Embed/Pool/Match/Upsert — satisfied by *embedding.Service in-process now, gRPC client later (same
shape). Threshold is a ResolveConfig param (data-driven, tuned at corpus run); ULID via oklog/ulid.
Test `TestEntityResolverWorkedExample` PROVES the worked example: index.ttl→1 new entity (3-claim
graph); 2002 vCard new email→matches same entity + 1-claim delta; 2004 vCard→NOOP with ZERO new embeds.

**Fixed two coder/hnsw fragilities in `EntityIndex.Upsert`:** (1) Add panics re-adding an existing key
→ Delete-first; (2) Delete of the LAST node leaves empty-but-present layers whose entry() nil-derefs
the next Add's dim check → recreate the graph when Delete empties it.

## I1 parser-extraction + claim-bridge DONE (2026-06-02)

Extracted the Turtle/RDF* parser into a NEW dependency-free package `internal/rdfbundle` (exported
`Term{Kind,Value,Language,Datatype}`, `Statement{Subject,Predicate,Object,Hash}`, `AnnotationRecord`,
`Parse`, `StatementHash`, `TermKey`, `TypePredicate`). `internal/query/postgres/rdf.go` now keeps thin
TYPE ALIASES (`rdfTerm=rdfbundle.Term`, `rdfStatement=rdfbundle.Statement`,
`rdfAnnotation=rdfbundle.AnnotationRecord`) + wrapper funcs (`parseRDFBundle`→`rdfbundle.Parse`,
`rdfTermKey`→`rdfbundle.TermKey`); deleted ~565 lines of moved parser; field accesses recased
(.kind→.Kind etc.); parser tests moved to `internal/rdfbundle/parse_test.go` (rdf_test.go keeps only
the `rdfBundleText` UTF-8 test). Projection unchanged behaviorally; query/postgres builds + test-pkg
compiles + kept test passes.

`internal/contactio/claim_extract.go`: `claimStatementsFromBody(body)` = rdfbundle.Parse → SUBJECT-
INDEPENDENT claimStatements (Text = predicateLocalName + ": " + normalizeFingerprintValue(object);
Hash = embedding.StatementHash(Text); Line = `<pred> object` for persist; IsName by predicate
localname). Works uniformly on raw index.ttl Turtle AND generated line bodies. Test
`TestFullChainTurtleToResolution` proves parser→extract→resolver: index.ttl→1 entity; line-oriented
vCard same person+new email→matches+1-claim delta. All touched pkgs green (build/vet/test).

## I1 CLI WIRING DONE (2026-06-02) — ingest-time resolution is live in `contact import`

`internal/contactio/resolve_import.go`: exported `(*EntityResolver).ResolveImport(ctx, format,
sourceName, content, options, observedAt) ([]ResolvedRecord, ImportResult, error)` — runs
BuildContactDeltas, then per logical-contact body: claimStatementsFromBody → Resolve → on non-NOOP
builds an IMMUTABLE delta record via `buildEntityDeltaRecord` (each NEW claim re-subjected to
`urn:gmeow:entity:<ULID>` [`EntityPrefix`], + RDF* `gmeow:observedAt` transaction-time stamp per claim,
+ importance claim); NOOP records carry empty Content. result.Contacts rewritten to distinct resolved
ENTITY ids. Test `TestResolveImportProducesEntitySubjectedDeltaRecords` green (rooted TTL→1 new
entity-subjected record w/ observedAt; re-import→NOOP empty content).

`internal/cli/query.go` `newContactImportCommand`: builds an in-process embedding.Service (HTTPEmbedder
from `Analysis.Embeddings` endpoint/model + MemoryCache + EntityIndex) + EntityResolver ONCE per import
session; new `--match-threshold` flag (default 0.65). `importContactPath` now takes the resolver, calls
ResolveImport, ingests non-NOOP records (ExternalID=`urn:gmeow:entity:<ULID>:<fp16>`, ContentRoles
RDFSourceBundle+ContactSource, facets root_subject=entity IRI), skips NOOPs. NOTE: import now REQUIRES
the embedding endpoint (:8090) up — ingest-time resolution by design.

STILL DEFERRED (not needed for the single-session corpus TUNE run): (a) persist EntityLedger +
EntityIndex as FILESTORE artifacts (load at start / snapshot after) — without it, re-running a fresh
import mints NEW ULIDs (no cross-session idempotency); (b) swap in-process embedding.Service → gRPC
EmbeddingClient (needs ctx-signature adapter: client Match/Upsert take ctx). (c) I2 projection
(query_contact_entities) to surface entities + fold (first_seen,last_seen) from the observedAt stamps.

## I1b + I1c DONE (2026-06-02) — resolution single-homed in EMBEDDING + persisted

**I1b (gRPC swap / resolution moved to EMBEDDING):** EntityResolver+EntityLedger MOVED from contactio
into `internal/embedding/resolve.go` (operate on `embedding.ClaimInput{Text,Hash,IsName}`); `Service`
now owns cache+index+ledger+newID (`SetIDSource` for tests) and exposes
`Resolve(ctx,claims,threshold)->Resolution{Entity,NewClaimHashes,Similarity,IsNew,IsNoop}` (mutex-
serialized: match→mint→diff→upsert atomic). Added `Resolve` RPC (proto+server+client). contactio
`resolve.go`/`resolve_test.go` DELETED; `claimStatement` moved to claim_extract.go; `ResolveImport` is
now a FREE func taking a `contactio.Resolver` interface (Resolve only) — satisfied by *embedding.Service
AND *rpc.EmbeddingClient. CLI `--embedding-rpc` bool picks gRPC client (persisted) vs in-process
engine. gRPC Resolve round-trip tested.

**I1c (persistence):** `internal/embedding/state.go` — `Service.SnapshotState()/LoadState()` frame
claim-vector cache + EntityIndex.Snapshot + ledger into one `GMEOWSTATE1` artifact. `MemoryCache`
snapshot/load + `Resolver.Cache()` accessor added. `embedding-serve --state-file <path>` loads at
startup, snapshots on shutdown (defer). Test proves cross-restart idempotency: resolve→snapshot→cold
LoadState→re-resolve = NOOP on same entity. All touched pkgs green (build/vet/test).

OPERATIONAL: for a persisted/realistic run, start `gmeow embedding-serve --config gmeow.toml
--state-file ./data/resolution.state` (needs model endpoint :8090 up), then
`gmeow contact import <corpus> --embedding-rpc --source-name ... --import-level ... --match-threshold T`.
Without --embedding-rpc, import uses an in-process non-persistent engine (still needs :8090).

## I2 DONE (2026-06-02) — entities surface via the existing contact projection + observedAt fold

KEY REALIZATION: no new `query_contact_entities` table needed — **entity = contact** in this model. A
new entity's FIRST delta record carries `<urn:gmeow:entity:ULID> rdf:type foaf:Person` (the type claim
is in the first observation's delta), so the EXISTING contact projection
(insertRDFRows→query_rdf_statements; rdfContactRootSubjectsForSourceTx detects it via
IsContactEntityType; contactentity.FactsForContacts keys facts by the entity IRI subject) surfaces each
entity as a contact in `query_contact_rollups`/`query_contact_facts`, contact_id = entity IRI. Distinct
rollups = entity count (the TUNE measurement). Verified at the record level
(TestResolveImportProducesEntitySubjectedDeltaRecords asserts the rdf:type foaf:Person triple is in the
delta record). NEW bi-temporal piece: `applyTemporalAnnotation` (contactentity.go) now folds
`gmeow:observedAt` → fact ValidFrom (first_seen) when no explicit valid-time
(TestFactsForContactsFoldsObservedAtIntoValidFrom green). last_seen advancing on re-observation is
deferred (re-observations NOOP; needs a "touch" path). Live DB end-to-end projection verification
happens at the TUNE run.

ALL THREE pre-TUNE items DONE: I1b (gRPC swap), I1c (persistence), I2 (projection+observedAt fold).
Full module builds, vet clean, all touched-pkg tests green.

NOW READY FOR TUNE: (1) bring up model endpoint :8090; (2) `gmeow embedding-serve --config gmeow.toml
--state-file ./data/resolution.state &`; (3) wipe ./data (filestore + query) for a clean run; (4)
`gmeow contact import <corpus>+index.ttl --embedding-rpc --source-name ... --import-level ...
--match-threshold T`; (5) stop embedding-serve (snapshots state); (6) `query migrate` + `query rebuild`
(or project-changed) to project; (7) `query breakdown` / count distinct contact rollups = ENTITIES,
compare vs ~5k; tune T and repeat. Watch: import now needs :8090; greedy match is order-sensitive
(rich index.ttl first seeds strong centroids).

## LIVE STACK RUN (2026-06-02) — pipeline validated end-to-end; FOUR bugs found+fixed

Ran the full stack (model :8090 + filestore-serve + embedding-serve --state-file + gmeow-admin contact
import --embedding-rpc) over real Google-contacts vCard exports. CRITICAL: pass `--config gmeow.toml` to
gmeow-admin or it dials DEFAULT sockets, not base-*. Each delta record is its own filestore object;
projection (`query migrate` + `query rebuild --confirm-instance local`) surfaces entity as
contact_id=`urn:gmeow:entity:<ULID>` in query_contact_facts with valid_from=observedAt (fold CONFIRMED live).

FOUR bugs the live run exposed (all FIXED):
1. **coder/hnsw Delete+Add churn corrupts the graph** → nil-deref mid-import. FIX (index.go): never
   Delete; authoritative `centroids map[entity]Vector`; new entities Add immediately; updates rewrite the
   map; rebuild graphs every rebuildEvery=256 updates; isUsableVector guard vs zero/NaN. Test:
   TestServiceResolveChurnDoesNotCorruptIndex.
2. **name-confirmation BACKWARDS → catastrophic over-merge** (950→444, "Abby Smith"+"Buck May" merged).
   NearestName ANN-searched the GLOBAL names graph then filtered to entity — when names DIVERGE the
   entity's name isn't in the incoming's top-K → found=false → "can't disconfirm" → ACCEPT. FIX: replaced
   names graph with `entityNames map[entity][]Vector`; NearestName = DIRECT max-cosine over the entity's
   OWN names. After fix: 950→**725** (distinct people separate; sane ~1.3:1 within one snapshot).
3. **embedding endpoint 8192-token physical batch limit** → 500 on a record with many/long claims (13911
   tokens). FIX (embedder.go): HTTPEmbedder.Embed now truncates each text (maxTextRunes=1600) and chunks
   into sub-batches (maxBatchTexts=48, maxBatchTokenEst=5000), concatenating in order.
   TestHTTPEmbedderChunksLargeBatches.
4. **no snapshot on SIGTERM** → I1c persistence didn't actually run on `kill` (rpc.Serve doesn't wire
   signals → ctx). FIX (cli/embedding.go): signal.NotifyContext(SIGINT,SIGTERM) so the snapshot defer runs.

STATUS: within-snapshot dedup VALIDATED (950 cards → 725 entities at threshold 0.65, projects to QUERY
correctly). Cross-snapshot run (2011+2012 = 3265 cards) was still embedding when the session paused
(endpoint-bound, ~12+ min first pass). Full module green (build/vet/test). NEXT: finish cross-snapshot +
full-corpus threshold sweep (the remaining empirical TUNE); rooted-RDF root-subject-weighting still
needed for the entangled index.ttl LOD profiles (separate from the vCard path).

## CROSS-SNAPSHOT RUN BLOCKED by model endpoint stall + BUG 5 FIXED (2026-06-02)

Cross-snapshot import (2011+2012=3265 cards) ran 13m then the MODEL ENDPOINT :8090 STALLED: 2011→716
entities OK, 2012 file aborted with "context deadline exceeded (Client.Timeout 120s while awaiting
headers)"; curl :8090 now returns 000 (endpoint unresponsive/down). So cross-snapshot dedup number is
INCONCLUSIVE — blocked on the model server, not the resolver. (716 from the 2011 file is consistent with
the earlier 725; within-snapshot dedup stands.)

**BUG 5 (FIXED): a transient endpoint error aborts the WHOLE file** (loses all progress). FIX
(embedder.go): `embedBatch` now retries `embedBatchOnce` with backoff (embedRetryBackoffs 2s/5s/10s) on
transient failures (network/timeout err, 5xx, 429); permanent errors (4xx, malformed) fail fast; respects
ctx cancellation. Tests: TestHTTPEmbedderRetriesTransientFailures + TestHTTPEmbedderChunksLargeBatches.
Binaries rebuilt (./bin/gmeow{,-admin}) with all 5 fixes. NOTE retry can't revive a DEAD endpoint — the
:8090 model server must be restarted before resuming any live import.

RUNNING SERVICES at pause: filestore-serve (new binary) + query-serve + an OLD embedding-serve (pre-
retry/signal binary, holds 716 entities in memory, NOT snapshotted since it lacks the signal fix). To
resume: restart :8090 model; restart embedding-serve with NEW binary (`./bin/gmeow --config gmeow.toml
embedding-serve --state-file ./data/resolution.state`); wipe ./data for a clean tuning baseline.

NEXT (still TODO for real TUNE): full-corpus threshold sweep (0.65 is a guess; warm cache makes re-runs
fast now); per-RECORD error isolation in ResolveImport (currently one record's hard error still aborts
the file even with retry); root-subject-weighted resolution for entangled rooted-RDF (index.ttl LOD).

## GENTLE ENDPOINT PROFILE (2026-06-02) — after my unthrottled run CRASHED the :8090 model server

User report: the prior sustained, large-batch import (48 texts/5000 tok per request, back-to-back, 120s
timeout, eager retry) CRASHED the local model server. FIX (embedder.go) — HTTPEmbedder is now GENTLE by
default: maxBatchTexts 48→12, maxBatchTokenEst 5000→1500 (small per-request memory), `pace()` serializes
requests + enforces defaultMinInterval=200ms between them (never concurrent/back-to-back), timeout
120s→60s (fail-fast on stall, don't let work pile up), retry backoffs 2/5/10s→5/15/30s (give a
struggling server room). VALIDATED: 20-card sample → 14 entities in 8.5s, endpoint stayed 200 after.
RULE: keep imports SMALL/paced; do NOT launch big unthrottled background runs at :8090. For the full
corpus, prefer many small paced batches and watch endpoint health between.

## OPS LESSON (2026-06-02): never wipe ./data/filestore while filestore-serve runs
`rm -rf ./data/filestore` while filestore-serve held the dir open CORRUPTED Pebble (metadata/000002.log
rename → no such file or directory) and CRASHED filestore mid-import (import then failed "dial
base-filestore.sock: connection refused"). The model endpoint was fine (200) — gentle profile worked;
this was self-inflicted. RESET SEQUENCE: stop filestore-serve AND embedding-serve FIRST, then
`rm -rf ./data/filestore ./data/resolution.state`, then restart both. (resolution.state is embedding's
own file, safe to rm only when embedding-serve is stopped too.) Also: filestore spams best-effort
"notify scheduler ... connection refused" when scheduler is down — harmless per FILESTORE-only invariant.

## TUNING SUCCESS (2026-06-02): separate name-threshold + fast warm-cache sweep loop

Added SEPARATE `--name-threshold` (default 0.85) distinct from `--match-threshold` (centroid, 0.65), a
`Reset` RPC + `gmeow-admin contact reset-entities` (clears index+ledger, KEEPS claim cache). Resolve RPC
now takes (threshold, nameThreshold); name gate floors at >= centroid threshold.

FAST TUNING LOOP (proven): TERM embedding-serve (snapshots 126MB cache+entities via signal fix) → restart
new binary (loads warm cache) → `contact reset-entities` (keep cache) → stop+wipe filestore → re-import
at new thresholds. With warm cache, re-resolving 3265 cards took **9.1s** (vs 14min cold) — threshold
sweeps are now interactive.

RESULT — match=0.72 name=0.88 vs lenient 0.65/0.65 on the 2 Google exports (3265 cards):
- 2011: 950→808 entities (was 711); 2012: 2315→2109 (was 1866). Stricter = MORE entities = less merging.
- MERGE QUALITY (1000-fact projection sample): entities with >=3 distinct names dropped 7→1, and the ONE
  remaining is a LEGIT same-person variant cluster ("Adrianna Frances Goodson"/"Adrianna Francis
  Goodson"/"Adrianna Goodson" — typo+middle-name). names-per-entity histogram {1:88, 2:3, 3:1}. The
  garbage merges (Alex Hessami+Hewitt, three Allans, six random people) are GONE. The stricter NAME gate
  (0.88) is the fix: same-first-name people separate, true variants still merge. CLEAN RESOLUTION.

Good calibration on this data: match≈0.72, name≈0.88. (Re-validate on broader corpus formats.) Endpoint
stayed healthy (gentle profile + warm cache = near-zero endpoint load on the sweep). query contact facts
API caps at 1000 rows — for true total entity count need a COUNT(DISTINCT contact_id) surface (TODO).
<!-- /coding-ethos-memory:debdf1d09963c677 -->

<!-- coding-ethos-memory:816a983ec40d1fb1 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/normalize-before-compare.md

---
name: normalize-before-compare
description: contact resolution principle — normalize attributes AND values to a canonical space BEFORE idDiff comparison
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

**Normalization is required BEFORE comparison.** (User lesson, 2026-06-03.)

In contact entity resolution, two claims that mean the same thing must be reduced to the SAME
canonical form before `idDiff` compares them — otherwise identical information from different
importers never matches and entities over-split.

**Why:** The first full-PIM_Data run over-split to 18k+ entities because `idDiff` grouped claims by
the RAW predicate local-name. The same person's email was `hasEmail` from the vcard importer but
`email`/`Email` from the Apple importer; name was `FN` vs `givenName`/`familyName`; phone `hasTelephone`
vs `cellPhone`. Different buckets ⇒ never compared ⇒ apple records didn't merge with their vcard
counterparts. The fix: `canonicalAttr()` in `internal/embedding/claim.go` maps cross-vocabulary
predicates to concepts (email/phone/url/name/nickname/im/sameas) before grouping + kind classification.

**How to apply:**
- BOTH axes need normalization before comparison: the ATTRIBUTE (predicate → canonical concept) and
  the VALUE (already partial: phone→digits, url→trim-slash in `claim_extract.go normalizeClaimObject`;
  extend as needed — e.g. name-part composition is still a gap, givenName+familyName not composed to a
  full name, so apple name-parts stay contextual rather than matching a vcard FN).
- Any new importer/source MUST map its predicates through the canonical concept space, or its claims
  silently won't resolve against existing entities.
- This is a general principle for any embedding/graph comparison: canonicalize the comparison space
  first; never compare raw surface forms. Relates to [[contact-identity-theory]] and
  [[ingest-time-entity-resolution]] (the idDiff engine).
<!-- /coding-ethos-memory:816a983ec40d1fb1 -->

<!-- coding-ethos-memory:ff7cd2d90a03fbc8 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/published-gmeow-ontology.md

---
name: published-gmeow-ontology
description: The gmeow ontology namespace is already published with content-negotiation + Cloudflare-worker SPARQL+ endpoints
metadata: 
  node_type: memory
  type: reference
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

The canonical ontology namespace is `gmeow:` = `https://blackcatinformatics.ca/gmeow/` (a slash
namespace, per-term content negotiation — NOT the old `.../gmeow/ontology#` fragment form). **There
is no `bcid` anywhere** — the prefix is `gmeow:` and the only term IRI form is
`https://blackcatinformatics.ca/gmeow/<localname>`. It is **already a published, dereferenceable
ontology** with extensive content negotiation and full Cloudflare-worker-based SPARQL+ endpoints;
`gmeow:` IRIs resolve and are queryable live.

On 2026-06-01 the codebase was fully de-`bcid`'d: the namespace was rebased from
`.../gmeow/ontology#` to `.../gmeow/`, and every `bcid`/`BCID` identifier + Turtle prefix label was
renamed to `gmeow`/`Gmeow` (`bcidPrefix`→`gmeowPrefix`, `BCID*`→`Gmeow*`, `@prefix gmeow:`) across
contactio.go, contactentity.go consumer constants, and tests, in lockstep.

**Implication for contact ingestion / `docs/ontology/`:** the published ontology is the CANONICAL
term authority — not repo markdown. New `gmeow:` terms must be registered in the published ontology
before code relies on them. The repo `docs/ontology/` tables are only the per-format → predicate
mapping layer + the mapping-table↔code completeness test; they index which terms the importer uses
and why a local term beat a standard one, and must not diverge from the published ontology. Term
*definition* validation (live SPARQL vs committed snapshot) is an open decision — leaning snapshot
for hermetic CI. See [[contact-ingestion-redesign]].

Note: the sample LOD graphs bind their own unrelated, site-local prefixes —
`~/Active/sites/paudley/dist/index.ttl` uses `<https://patrickaudley.com/lod#>` and bii uses
`<https://blackcatinformatics.ca/>`. Those are the site authors' namespaces, distinct from the
`gmeow:` ontology terms.
<!-- /coding-ethos-memory:ff7cd2d90a03fbc8 -->

<!-- coding-ethos-memory:4a06a96d98947498 -->
## Imported from ~/.claude/projects/-home-paudley-Active-gmeow/memory/repair-not-ingestion.md

---
name: repair-not-ingestion
description: "REPAIR is a separate offline pass, NOT part of ingestion; ingestion must resolve correctly at ingest and read nothing from QUERY"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 264bd2f7-7e0d-40bd-ad98-82a07af384ea
---

REPAIR (the global correlation-clustering re-partition) is **not** part of the ingestion process, and must not be leaned on as the answer to ingest-time over-merge. Over-merge / greedy-accretion blobs are an **ingestion-quality** problem to fix **at ingest** (in EMBEDDING: idDiff, ω, residual value-match, accumulation guards) — REPAIR is a distinct, deferred, offline global pass, never a crutch for ingestion.

Ingestion may **not** use any data in QUERY. PERIOD. The ingest path (importers → resolve → FILESTORE delta records) reads only FILESTORE + EMBEDDING (cache + entity index). See [[filestore-only-import-invariant]] and [[ingest-time-entity-resolution]].

**Why:** the user corrected this twice after I repeatedly framed REPAIR as the fix for ingest blobs and reached toward query-side data. The locked architecture keeps ingestion self-sufficient and QUERY-free (the hybrid was 550× slower).

**How to apply:** when an ingest measurement shows over-merge, propose and build an **ingest-time** fix (the residual value-match `R = X − X·Wₖ·Wₖᵀ` per [[contact-identity-theory]] §4.1.1 is one; better idDiff scoring / accumulation bounds are others). Do not say "REPAIR will handle it." Do not read QUERY during ingest.
<!-- /coding-ethos-memory:4a06a96d98947498 -->
