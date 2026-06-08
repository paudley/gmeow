<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: AGPL-3.0-only
-->

# `gmeow:` Term Registry

Namespace: `https://blackcatinformatics.ca/gmeow/` (prefix `gmeow:` in code). This is a
slash namespace — each term (e.g. `https://blackcatinformatics.ca/gmeow/importanceLevel`)
dereferences individually via the published ontology's per-term content negotiation.

> **Canonical definitions live in the published ontology, not this file.** The namespace domain
> already serves the ontology with full content negotiation and Cloudflare-worker SPARQL+
> endpoints — `gmeow:` IRIs are dereferenceable and queryable. A new `gmeow:` term is "real" only
> once it is registered in the **published** ontology; this file is a convenience index of which
> terms the importer uses and *why* a local term was chosen over a standard one. It must not
> diverge from the published ontology.

Every locally-used term MUST resolve in the published ontology and SHOULD appear here with a
justification (why no standard term fits) before code relies on it. The completeness test enforces
parity between the terms the code emits and this index; term *definitions* are validated against
the published ontology (see the open question in the design doc about live vs. snapshot
validation).

## Semantic / cross-format terms

| Term | Meaning | gmeow_reason |
|---|---|---|
| `importanceLevel` | 0–10 import/source importance; projection takes the max across imports | no standard 0–10 contact-importance predicate |
| `confidence` | per-claim confidence 0..1 (RDF* annotation) | PROV has no compact scalar-confidence term |
| `mappedFrom` | source property a claim was derived from (RDF* annotation, audit) | provenance of the *mapping step*, not covered by `prov:wasDerivedFrom` alone |
| `vendorExtension/<vendor>/<name>` | deterministic mapping of an `X-<vendor>-<name>` property | preserves vendor extensions as distinct queryable predicates without an opaque bucket |
| `hasAgreement` | temporally-scoped agreement relationship | no standard term |
| `hasMet` | temporally-scoped "has met" relationship | no standard term |
| `hasUsed` | temporally-scoped "has used" relationship | no standard term |
| `hasWorkedWith` | temporally-scoped "has worked with" relationship | `rel:` lacks this distinction |

## Source-property terms (to backfill)

The code currently defines ~180 `gmeow:` source-property terms across vCard / Apple / BBDB / CSV /
LinkedIn / provider-specific dialects (e.g. `vcardEmail`, `appleAddressBookUID`, `bbdbTimestamp`,
`linkedInProfileURL`, `fastmailTags`, `plaxoMicroBlog`). These exist to preserve source fields
losslessly where no standard predicate is a clean fit. **They are not yet documented here.**

These will be enumerated per-format in the mapping tables (see [README](README.md)); the
completeness test will fail until every one is listed with its `gmeow_reason`. Backfill is tracked
as part of Phases 1–6. Where a standard term *does* fit, the term should be retired in favor of the
standard rather than documented.
