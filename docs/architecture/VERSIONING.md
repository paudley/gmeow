# FILESTORE Versioning Architecture

FILESTORE owns logical version history for entities that can be observed more
than once with the same identity. Mail archive import is the first case, and
contacts from vCard/FOAF will use the same model.

## Model

A versioned entity has a version set. The version set stores the logical domain,
logical id, canonical version id, version count, and maximum observed change
scale. Canonical versions are always full non-diff objects or compounds.
Alternate versions may be full records or delta-backed records.

Version records are immutable FILESTORE objects with the `version_record` facet.
They record representation (`full` or `delta`), target hash, base version and
base digest for deltas, canonicalization version, source provenance, and input
fingerprints used by analysis. Delta objects use `version_delta`/patch roles and
must include base and target integrity hashes.

The initial implementation stores deltas against the canonical full version
rather than chaining deltas. Alternate version reads can be slower, but
canonical reads remain fast and complete.

## Scale

Facets classify each observed change as `trivial`, `minor`, or `major`.
FILESTORE stores the scale but does not decide domain semantics.

For mail:

- `trivial`: source path, import path, backup generation, non-semantic mail
  server transit/storage headers, or duplicate provenance.
- `minor`: body text, subject, selected headers, attachment metadata, or modest
  attachment/body differences within the same logical message.
- `major`: unrelated content under the same Message-ID, meaningful attachment
  set changes, or identity uncertainty.

## Canonical Promotion

Every version set supports canonical promotion. Promotion selects or writes a
full canonical version and records the previous canonical id, new canonical id,
promotion reason, scale, policy, and provenance. The old canonical remains
addressable as historical version data.

For mail archive import, a longer post-attachment-removal body may promote the
canonical version. Major conflicts are retained for review unless the mail facet
policy marks the incoming version as higher-quality canonical content.

## Low-Noise Import

Archive import supports a low-noise mode for bulk older Gmail/archive imports.
Low-noise mode compares normalized RFC Message-ID plus canonical
post-attachment-removal body-line fingerprint.

If both match an existing version set, import writes no per-message FILESTORE
record and increments report counters only. If only trivial fields differ,
import also skips per-message writes. Missing Message-IDs and meaningful body
differences import normally.

## Analysis

Full analysis runs by default only for canonical versions. Non-canonical
versions do not trigger full analysis automatically. Scheduler decisions should
use version scale and analyzer input fingerprints:

- `trivial`: inherit existing analysis.
- `minor`: enqueue changed-input analyzers only when promoted or forced.
- `major`: mark for review; run full analysis only when promoted or forced.

Analysis annotations that are version-aware include version set id, version id,
analysis scope (`canonical`, `version_delta`, or `inherited`), input
fingerprint, and inherited-from version id where applicable.
