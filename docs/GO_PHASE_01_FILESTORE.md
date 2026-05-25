# Go Migration Phase 01 - FILESTORE

## Goal

Implement FILESTORE as the authoritative datastore: bytes, object identity, dedupe, facets,
provenance, relationships, compound objects, immutable recovery sidecars, and atomic annotations.

## Build

- Implement BLAKE3 object directories under `objects/blake3/aa/bb/<digest>/`.
- Implement byte-bearing object writes:
  - canonical uncompressed BLAKE3 identity;
  - zstd `blob.zst`;
  - immutable `recovery.json`;
  - media type and source hints.
- Implement compound object writes:
  - required stable `object_id`;
  - digest `BLAKE3("compound:{object_id}")`;
  - canonical compound envelope as blob payload when no natural source bytes exist.
- Implement manifest and annotation IO:
  - `manifest.json.zst`;
  - `analysis.json.zst`;
  - `overlays.json.zst`;
  - atomic write/replace behavior.
- Enforce at least one facet on every object.
- Implement dedupe and merge rules inside FILESTORE only.
- Implement structure queries such as `GetStructure(digest)`.
- Implement `gmeow-admin filestore verify` with operator-facing reports.

## Retire Python Equivalent

After Go FILESTORE passes functional write/read/rebuild fixture tests, remove the Python object-store
implementation and tests that are no longer authoritative:

- `src/gmeow/object_store.py`
- FILESTORE-like code in `src/gmeow/resilience.py`
- FILESTORE-specific helpers in `src/gmeow/metadata.py` when superseded by manifests
- Python tests that assert the old object path or sidecar format

Keep Python modules that still serve Gmail, QUERY, analysis, or interface behavior until their
phase replaces them.

## Functional Proof

- Writing the same file bytes twice creates one object directory.
- Two compound writes with the same stable object ID resolve to one digest.
- Compound creation without a stable object ID is rejected before writing.
- Every object directory contains `blob.zst` and `recovery.json`.
- Normal FILESTORE reads ignore `recovery.json`.
- Corrupt bytes or mismatched hashes are reported by verify.
- A compound object can return role-mapped parts and subobject digests.

## Exit Gate

- FILESTORE tests cover file objects, compound objects, facets, provenance, relationships, overlays,
  dedupe, and recovery sidecars.
- No SOURCE, QUERY, ANALYSIS, or INTERFACE package computes object identity directly.
- Python object-store code is gone once the Go FILESTORE owns all object persistence.
