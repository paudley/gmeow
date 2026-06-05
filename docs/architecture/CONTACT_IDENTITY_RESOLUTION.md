<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc. -->
<!-- SPDX-License-Identifier: AGPL-3.0-only -->

# Contact Identity Resolution — Formal Foundations

> **Canonical and load-bearing.** This is the settled, authoritative treatment of *what it means for
> two contact observations to denote the same person or organization*. This exact discussion has
> recurred at least three times across five working sessions, each time collapsing back to a
> common-sense model that is **provably wrong** and each time re-derived from scratch. This document
> exists so it is never re-derived again. **Read it before proposing any change to entity
> resolution.** If a proposal reduces identity to attribute equality (match on email / phone / name),
> it is wrong by §2 — stop and re-read.

## 1. The problem is formal, not common-sense

Contact resolution is the task of recovering, from a stream of partial, noisy, time-varying
**observations** (vCards, address-book rows, FOAF graphs, mail headers, manual edits), the **latent
equivalence relation** "refers to the same real-world agent." We never observe the agent. We observe
projections of the agent at moments in time, through lossy and inconsistent sources, and must
estimate the partition of observations into agents.

Essentially every contact manager ever built models this as **attribute equality**: two records are
the same contact iff they share an email, or a phone number, or a normalized name. This is intuitive,
it is what users expect, and it is **formally incorrect**. The rest of this document is why, and what
the correct formulation is.

The error is a category error: treating identity as a **decidable predicate over observed attributes**
when it is the estimation of a **latent variable**. No predicate over the attributes will do, because —
as §2 proves — the attributes neither determine nor are determined by identity.

## 2. No attribute axiom can define identity (the impossibility)

### Set-identity framing

Ask the analogous mathematical question first: *what is the identity of a set of real numbers?* Of
*two sets of thirty random natural numbers?* Set identity is plainly **not** element-by-element
equality in the sense we need here — a set that gains or loses one element is not thereby a "different
set" for the purpose of saying *which collection it is*, and you cannot recover "is this the same
collection" from any fixed predicate over individual elements. The identity lives in the **structure
and overlap** of the elements weighted by how distinguishing each element is — not in equality of any
element or subset. Hold this analogy; §4 makes it literal.

### No attribute is *necessary*

Every attribute we can record can change while the person persists. Concretely: a friend transitions
**Paul X → Lindsey Y**. Legal name changes. Gender changes M→F. Email changes. (The phone number
happened to stay — coincidence, not identity.) Same person throughout. There is therefore **no
attribute whose persistence is necessary** for identity. Name is not it; gender is not it; email is
not it; phone is not it.

### No attribute is *sufficient*

Every identifier we can record is shared and/or transferred across people. A person can hold **twelve
active phone numbers**, one of which previously belonged to their spouse. A role mailbox like
`admin@axion.net` is **inherited by whoever next holds the position**. Family plans share numbers;
households share addresses; thousands of people share the name "John Smith." So **no attribute match
is sufficient** to conclude same-person: same number, different people; same mailbox, different
people; same name, different people.

### The impossibility statement

These are not edge cases to be patched — they are an impossibility, in the spirit the user names after
Church–Turing and Gödel: *for any identity axiom you fix, a counterexample exists.*

Precisely: the map from agents to observations is **many-to-many and time-varying**. One agent
projects to many divergent observations (across time, sources, life changes); one observed value
attaches to many agents (sharing, reassignment, roles). This map is therefore **non-injective in both
directions**, so the latent equivalence relation is **non-identifiable** from any finite, decidable
predicate `A` over observed attributes:

- there exists a **false-merge** pair — `A(o₁, o₂)` holds but `o₁`, `o₂` are different agents (shared
  / transferred identifier); and
- there exists a **false-split** pair — `A(o₁, o₂)` fails but `o₁`, `o₂` are the same agent (everything
  recorded changed).

Equivalently, both of Leibniz's principles fail **under observational projection**: the *identity of
indiscernibles* fails (two different people can present indiscernible sparse cards), and the
*indiscernibility of identicals* fails (one person presents wildly discernible cards over a life).

**Conclusion.** There is no rule-set. We are not evaluating a predicate; we are **estimating a latent
variable** from lossy evidence. Every design decision downstream follows from accepting this.

## 3. Why embeddings — and why embedding-plus-threshold is *still* an axiom

Semantic embeddings are the right **signal**, for a precise reason: an embedding integrates the *whole
gestalt* of an observation into a point in a continuous space, so that **no single attribute is
decisive**. Identity becomes a question of *position and trajectory in a manifold* rather than
predicate satisfaction, which is exactly the robustness §2 demands — any one attribute can change and
the point barely moves; the surrounding context carries it. This is why the embedding approach
outperforms attribute matching, and why it is the foundation we build on.

But here is the trap, and it is the one this codebase fell into: **a fixed cosine threshold τ is
itself an attribute axiom** — the axiom "manifold distance < τ" — and it inherits the *exact same
impossibility* from §2. Different people who share heavy context (a person and their own company; two
colleagues at one firm) embed **close** → false merge. The same person across a major transition
(Paul→Lindsey; a career and city change) embeds **far** → false split. A threshold on an embedding is
a better axiom than a threshold on a string, but it is still an axiom, and §2 guarantees it fails on
both sides. §5 documents this happening, measured, in the current implementation.

The embedding is necessary machinery and the richest single signal. It is **not** the definition of
identity.

## 4. The formal frame: an evidence graph and a global partition

Because identity is not a local pairwise predicate, it must be a **global property of an evidence
graph**. The decisive reason is **transitivity**: any pairwise same-person scorer will produce
triples where `a~b` and `b~c` but `a≁c`, which is not a valid equivalence relation and therefore not a
valid partition into people. You cannot threshold your way out of that; you must optimize a partition
globally.

- **Nodes** are observations (and, after folding, candidate entities).
- **Edges** carry **multi-modal, temporally-annotated, signed** evidence: embedding similarity,
  communication co-occurrence, name continuity, and shared identifiers **within overlapping validity**.
- **Edges may be negative.** A shared identifier whose ownership intervals are **disjoint** is evidence
  of *difference* (sequential holders), not sameness. This is the representation that finally makes the
  spouse's old number and the inherited `admin@` mailbox come out right.
- **Resolution is correlation clustering / community detection**: the partition that **maximizes global
  signed agreement** (equivalently, minimizes disagreement) over the whole graph — a maximum-likelihood
  estimate of the latent equivalence relation, not a thresholded decision.

This makes the set-identity analogy of §2 literal: an entity *is* a community in the evidence graph,
and "same person" is a question of weighted overlap structure, not element equality. And it makes an
identifier a **node with an ownership timeline**, not an attribute of a person — consistent with the
bi-temporal `(first_seen, last_seen)` RDF\* model and the temporal-validity rule in `AGENTS.md`
(observation time, real-world validity, and interaction evidence are distinct; a bounded legacy
endpoint must not invalidate the broader identity).

### 4.1 Formal core

The following is the normative model (user-approved 2026-06-03). The origin sketch it was refined from
is preserved verbatim in the Appendix.

**Types.**

- **`iClaim` `c = ⟨attr, value, meta⟩`** — one atomic RDF\* claim.
  - *Required meta:* `vec(value) ∈ ℝᵈ` (embedding of the value); `ulid` (stable, addressable).
  - *Optional meta:* validity interval `[first_seen, last_seen]`; `source_key` (import set, message-id,
    …); `supersedes → {ulid…}` (claims this corrects or replaces).
  - *Schema-derived:* the attribute's **kind** and a per-value **identifying weight** `ω(value)` (below).
- **`iGraph` `G`** — the claim-set asserted by **one observation** (one card / row / record). The raw,
  unresolved unit.
- **`ciGraph` `Γ`** — the **folded resolution** of all `iGraph`s assigned to an entity: claims unioned,
  `supersedes` applied (superseded claims **retained but shadowed**; the current view is the
  non-superseded frontier), bi-temporal. The materialized entity.

**Attribute kinds.** Every `attr` is one of:

- **`functional`** — single-valued *at a time* (legal-name-at-t, gender-at-t, birthdate).
- **`set`** — multi-valued (emails, phones, URLs, nicknames).
- **`contextual`** — free signal (notes, affiliations, skills, co-occurring people).

**Identifying weight (per *value*, not per attribute).**

```
ω(value) ≈ IDF(value over corpus) × kind_factor(attr) × source_trust × temporal_validity
```

A role address or a common given name has `ω ≈ 0`; a rare personal address or an unusual full name has
high `ω`. This is why "a strong name *antimatch* outweighs a strong role-email *match*" is **emergent**
rather than a hand-tuned rule: the role email simply carries almost no identifying mass.

**Pairwise comparison.** A **value-match** between two values requires both:

1. `cosine(vec₁, vec₂) ≥ τ_attr` — fuzzy equality via the embedding (handles `Bob`≈`Robert`, domain
   variants, transliterations); and
2. **temporal co-validity** — overlapping validity intervals (or both unbounded).

Temporal gating is **intrinsic**, not optional: a value can only *correlate* identities while it is
contemporaneously held. Each attribute then contributes a **signed, `ω`-weighted** quantity by kind:

- **`set`** — score by **identifying-mass overlap** (a weighted Jaccard over `ω`), graded and signed.
  High overlap (four of five emails shared) → **strong positive**. **Zero overlap between two
  non-trivial sets → negative**, not neutral (two populated, disjoint email-sets are evidence
  *against*). This is the literal set-identity measure of §2.
- **`functional`** — two **co-valid**, **non-`supersedes`-linked**, non-matching values →
  **contradiction (IAC)** (e.g. two distinct contemporaneous birthdates). A co-valid match → IC.
- **`contextual`** — soft, low-`ω` cosine contributions: the **shared-context web** that connects an
  agent's observations even when hard identifiers diverge. Individually weak, collectively decisive.
- **`supersedes`** — a value-pair linked by a `supersedes` claim is **neither IC nor IAC**. It is an
  **evolution edge**: its presence **reclassifies a would-be functional contradiction into a licensed
  transition** (the Paul→Lindsey name/gender change); its **absence leaves the antimatch standing as
  IAC**. `supersedes` is **first-class in the schema and represented end-to-end**. *Auto-populating* it
  is out of scope for the current pass — it may be entered manually, or **learned** (e.g. inferred from
  retained social profiles that bridge the old and new identity) — but the model and storage must carry
  it now.

**The difference function.**

```
idDiff(Γa, Γb) → confidence

  IC  = Σ ω·(co-valid functional matches)
      + Σ   (set positive-overlap mass)
      + Σ   (contextual soft-similarity)

  IAC = Σ ω·(co-valid functional contradictions, not superseded)
      + Σ   (set expected-but-absent overlap)

  ISD = unmatched mass with no expectation            → neutral
```

- **Source-independence.** Evidence is **deduplicated by `source_key` before summing**: N copies of a
  value from N exports of the *same* source count **once**. Idempotency thus falls *out of* the
  confidence model — it is not a separate mechanism (see §5 on the distinct, identity-agnostic
  source-content idempotency fix).
- **Decision.** `confidence = σ(Σ ω·IC − λ·Σ ω·IAC)`; **merge ⇔ confidence ≥ gate ∧ the IAC veto is not
  tripped.** Positive **IC weight is the gate** (you need sufficient identifying mass *for* sameness);
  **IAC is the veto/penalty**; **ISD is neutral** — *except* the `set` "expected-but-absent overlap"
  term, which is counted as IAC. **Absence of conflict never merges on its own.**

**Global step (the partition).** `idDiff` yields **signed edge weights** `w(a, b)` over candidate
entities. **Identity is the partition that maximizes global signed agreement (correlation
clustering)** — *not* pairwise `idDiff` thresholding, which would reintroduce the transitivity
violation. Because each `ciGraph` is *defined by* the assignment that `idDiff` informs, resolution is
an **iterative, EM-like fixpoint**: assign → fold → re-score → re-assign, to convergence. Temporal
validity and `supersedes` live on the edges, so the resulting partition is inherently bi-temporal.

### 4.1.1 Residual value-match — removing shared structure before cosine

The value-match cosine of §4.1 is computed in a **residual** embedding space, not the raw space.
A structured identifier's embedding is **dominated by its shared structural component** — an email
`@domain`, an account/URL host (`facebook.com/`), vCard boilerplate — so two *different* identifiers
that merely share a provider embed close. Measured on the corpus: distinct same-host accounts sit at
**0.63 mean / 0.77 max** raw cosine, *above* the 0.72 match threshold → a false-merge blob. This is a
**representation** failure, not a metric one: no Lₚ distance on the whole-value vector separates them
(for unit vectors cosine ≡ dot, and euclidean is monotone in cosine — only the *representation* moved),
and **IDF cannot suppress it** (each value is unique → high `ω`). Decomposing the value by hand
*backfires* — the host becomes an exact-match claim whose cold-start `ω` is high, growing the blob.

The fix removes the shared structure before comparing: project each value embedding onto its residual
after subtracting the top-k principal directions of the value-embedding set,

```
R = X − X·Wₖ·Wₖᵀ            (k ≈ 20)
```

— Audley (2025), *"Emergent Knowledge Graphs from Nonlinear Semantic Residuals"*
(`audley_2025_emergent_knowledge_graphs`; github.com/paudley/nonlinear-semantic-graphs; ABTT-adjacent,
*Mu & Viswanath, "All-but-the-Top"*). The top directions encode exactly the shared boilerplate;
removing them separates the false pairs (**0.63 → 0.03 mean, 0.24 max** at k≈20) while **preserving
genuine matches** (a true name variant stays **0.90**, a Bob/Robert nickname **0.65**). `idDiff`
compares in this residual space; the HNSW **blocking/centroid keys stay raw** (recall is deliberately
over-inclusive; the residual re-rank is the precision filter). A value that is *pure* boilerplate has a
near-zero residual and correlates with nothing — exactly right.

**This is an ingestion-time transform with no QUERY dependency.** The principal basis is estimated
**entirely from the EMBEDDING claim-vector cache** (rebuilt geometrically as the cache grows) — ingest
reads nothing from QUERY, consistent with the FILESTORE/EMBEDDING-only ingest invariant — and it is
**not** the global REPAIR pass. At corpus scale it collapsed the structured-identifier over-merge from
**862 distinct names in one entity to 31** (28×) on a 15k single-source import, ~doubling the resolved
entity count. The residue — greedy-ingest **accretion** entities that match on *accumulated* weak
residual-reduced signals — is a separate **ingestion-quality** concern fixed **at ingest**, never
deferred to REPAIR (REPAIR is a distinct, offline, QUERY-independent global pass, not a crutch for
ingestion).

### 4.2 The canonical cases, resolved correctly

| case | mechanism | outcome |
| --- | --- | --- |
| Re-import the identical card | same `source_key` ⇒ evidence dedups; IC adds nothing | **NOOP** |
| You + spouse, shared old number | number co-validity **disjoint** ⇒ not IC; lone low-`ω` transfer signal against otherwise-disjoint sets | **separate** |
| `admin@axion.net` inherited by successor | shared value, **disjoint** validity ⇒ transfer, not IC | **separate** |
| Paul → Lindsey, **no** supersedes | name-token **divergence** (each name has a distinctive token the other lacks) ⇒ IC withheld + IAC; gender functional antimatch ⇒ separate | **separate** (correct conservative default) |
| Paul → Lindsey, **with** name `supersedes` | transition reclassified to evolution; merge carried by shared-context IC + retained-handle overlap | **same** |
| Two strangers, sparse disjoint cards | no IC, no IAC, all ISD | **separate** (IC gate unmet) |

The two shifts that make these all correct: **time and `supersedes` turn value-equality into a *signed*
quantity** (a shared value can be positive, neutral, or negative), and **identity is the *global
partition* over those signed edges**, not the pairwise score itself.

#### 4.2.1 Names: role-free idf-subsumption (the `KindName` attribute)

A personal name is **not** a single functional string (comparing `"Patrick"` against `"Patrick Audley"`
as opaque values makes a *less complete* name read as a *contradiction*). It is decomposed into
normalized, role-free **tokens** (`ontology.NameTokens`: casefolded, honorific/generational affixes
stripped per the published `gmeow:Honorific` / generational vocabularies, initials kept). There is **no
privileged "surname" slot** — a token discriminates purely by **rarity (idf)**: `Audley` is strong,
`Patrick` (a common given *and* surname) weak. Two names are compared by **subsumption** (`nameScore`,
`internal/embedding/iddiff.go`):

- **Completion / subset** — one side's substantive tokens are all matched (`Patrick` ⊂ `Patrick Audley`;
  a middle name added; an honorific stripped): the shared tokens **corroborate**, idf-weighted. No penalty.
- **Divergence** — **both** sides carry a substantive (non-initial) unmatched token (`Patrick Audley`
  vs `Patrick Smith`; vs `Susan Audley`): the names denote different people, so the shared tokens are
  coincidence (a shared family name, a common given) and **withhold IC**, plus an IAC penalty by the
  weaker distinctive side. A `supersedes` link licenses the divergence (a name change) with neither.

Withholding IC on divergence (not merely penalising) is the anti-over-merge guard: a shared **rare**
surname cannot by itself fuse two clearly-different full names. `KindName` keeps idf **uncapped** (rarity
is the whole signal); the **divergence gate**, not a cap, replaces the contextual-`ω` cap as the
blob defence.

**Emission (gmeow names model).** A name is never a bare property: each record emits a co-equal
`gmeow:PersonName` appellation (`gmeow:hasName`) carrying a `gmeow:fullName` surface form and its
components as **typed `gmeow:NamePart` nodes** — `gmeow:hasNamePart` → (`gmeow:namePartType`
*given/surname/middle/nickname/honorific…* + `gmeow:partText`). This is the *only* canonical home for a
component: the published ontology retired the flat `givenNamePart`/`surnamePart` shortcuts, so a flat
given/family rendering exists only as a projection-layer downcast. The extractor grounds `gmeow:fullName`
and every `gmeow:partText` to `name-token` claims (the `namePartType` is irrelevant — comparison is
role-free; honorific/generational part texts self-strip), so the engine never compares whole-name
strings. The projection surfaces `gmeow:fullName` as the display name and a `namePartNickname` part as an
alias; a `gmeow:displayable false` appellation (deadname) is suppressed (`fnSelectDisplayName`).

## 5. The current implementation: state vs. target

The shipped engine (`internal/embedding/`, `internal/cli/query.go`) is a **single, greedy, ingest-time
realization of the embedding-plus-threshold axiom** — i.e. exactly the §3 trap. This section names
where, **as evidence of the theory, not as a defect list to paper over**. These are the gaps a future
implementation closes by moving toward §4.

- **One mean-pooled centroid per entity, which drifts.** On every accepted claim the entity centroid
  is re-pooled over *all* of its ledger claims (`internal/embedding/resolve.go` re-pool over
  `s.ledger.texts(entity)`; `internal/embedding/index.go` centroid map). As an entity absorbs
  heterogeneous cards, its mean drifts, so the *same person's* individual card later scores **below**
  the gate against the drifted centroid. This is a lossy projection of the per-claim evidence the §4
  model keeps separate.
- **A local, pairwise, greedy decision.** `Service.Resolve` (`resolve.go`) does a `k = 1` nearest-match,
  a **fixed cosine `threshold`**, and a name-gate (`nameThreshold`) — order-sensitive and with **no
  global partition**, so transitivity is not enforced.
- **Identifiers are ordinary pooled claims** with **no temporal validity gating** and **no negative
  edges** — so a shared/transferred value can only ever *help* a merge, never correctly *split* one.
- **Idempotency is entity-coupled.** The FILESTORE source key is
  `ExternalID = urn:gmeow:entity:<ULID> + content-fingerprint` (`internal/cli/query.go`), so unstable
  resolution *defeats* source-dedup: if the same card resolves to a different entity, it is re-written.
- **Measured this session.** At ≈10.4k entities, re-importing an already-ingested directory minted
  **+135 to +192 duplicate entities** (over-split) at the inherited `match-threshold 0.72`; loosening
  the gate to 0.5–0.65 cut over-splitting but trades it for over-merging. That is §2's impossibility
  observed live: the threshold cannot be right, because no threshold can.

**Target.** Embeddings become **one signed edge signal** in the bi-temporal evidence graph of §4;
identifiers become **time-scoped, possibly-negative** edges; resolution becomes a **revisable, global
clustering** rather than greedy ingest-time thresholding. This is the arc of the locked plan's
**REPAIR** (revisable splits/merges by reassigning immutable records) and **COMM** (communication-graph
overlap) phases — they are not afterthoughts, they are the global step.

**Two separable concerns — do not conflate them again.**

1. **Idempotency** is **identity-agnostic** and independently, trivially correct: a **source-content
   fingerprint** ("have I already ingested *this exact observation*?") decoupled from the resolved
   entity ULID makes a re-ingest a NOOP regardless of how resolution evolves. This can and should land
   on its own.
2. **Resolution** is the **latent-partition estimation** — the hard problem this document governs. It
   is *not* solved by anything that resembles attribute keying.

## 6. Canonical status and decision log

- This discussion has recurred at least **three times across five sessions**. **This document is now
  the canonical reference.** Future sessions consult it instead of re-deriving.
- **Attribute-equality identity** — email, phone, or name as a merge *key* — is **formally wrong (§2)
  and must never be re-proposed.** (Including the seductive "exact strong-claim identity index"
  proposed and rejected this session: a shared mailbox or transferred number is not identity.)
- Future work **refines the evidence graph and the clustering objective** (better `ω`, more edge
  modalities, learned `supersedes`, the global solver). It **never reverts to attribute keys.**
- This document **consolidates** prior fragments that each captured a piece but never the whole:
  `AGENTS.md` (temporal-validity rule), `docs/architecture/CONTACT_INGESTION_REDESIGN.md` ("dedupe
  cannot be field equality"), and the resolution-engine comments in `internal/embedding/resolve.go`.
  See also `docs/architecture/CONTACT_INGESTION_REDESIGN.md` for the operational ingest model and
  `docs/architecture/QUERY.md` for the projection surface.

## Appendix — origin sketch (verbatim)

The formal core in §4.1 was refined from this sketch by the maintainer; preserved here as the
derivation of record.

```
A. let type iClaim be a small rdf* claim of an attribute and a value, with:
  a) optional attached metadata:
     i) temporality (first_seen,last_seen)
     ii) source key (import set name, email message id, whatever)
     iii) superscedes claim data (for a claim that corrects or supersedes another claim)
  b) required attached metadata:
     i) vector embedding of value
     ii) ULID
B. let type iGraph be an rdf* graph of claims that are grouped around an identity.
C. let type ciGraph be a collapsed, fully resolved, graph of all the iGraphs for a given identity.
D. this a function idDiff(ciGraph a, ciGraph b) = confidence value where:
  a) this function compares both graphs and builds the intersection as IC (identity correlation) and
     the value conflicts as IAC (identity anticorrelation), and symmetric difference as ISD (non-overlap identity).
  c) if there is no IAC - this is purely an additive claim, we have no evidence to reject.
  d) if there is no IC - this means we've ended up comparing two contacts, likey with only cosine matching,
     this care requires the most care and we should proceed with the most caution
  e) Not all values have equal weighting. if we have a strong email match but a strong name antimatch, name is weighted higher.
  f) More IC = higher confidence, more IAC = lower confidence.
```

Refinements applied: temporality and `supersedes` made load-bearing (value-equality becomes a *signed*
quantity); `set` attributes scored by weighted-overlap mass (zero overlap is negative, not neutral);
weighting moved to per-value identifying power (IDF × kind × source-trust × temporal-validity);
evidence deduplicated by independent `source_key`; the IC-gate / IAC-veto / ISD-neutral decision rule
made explicit; and identity defined as the **global partition** over `idDiff` edges rather than the
pairwise score.
