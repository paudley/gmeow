// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"math"
	"testing"

	"blackcat.ca/gmeow/internal/filestore"
)

func TestStorageBreakdownChunkCountClampsForProto(t *testing.T) {
	response := ToPBStorageBreakdown(filestore.StorageBreakdownReport{
		Files: []filestore.StorageBreakdownFile{
			{
				ChunkCount:   -1,
				DictFamily:   "structured-json",
				DictFamilies: []string{"structured-json", "logs-line-oriented"},
			},
			{ChunkCount: math.MaxInt32 + 1},
		},
	})

	files := response.GetFiles()
	if len(files) != 2 {
		t.Fatalf("expected two files, got %d", len(files))
	}
	if files[0].GetChunkCount() != 0 {
		t.Fatalf(
			"expected negative chunk count to clamp to 0, got %d",
			files[0].GetChunkCount(),
		)
	}
	if files[1].GetChunkCount() != math.MaxInt32 {
		t.Fatalf(
			"expected large chunk count to clamp to MaxInt32, got %d",
			files[1].GetChunkCount(),
		)
	}
	if files[0].GetDictionaryFamily() != "structured-json" ||
		len(files[0].GetDictionaryFamilies()) != 2 {
		t.Fatalf("dictionary families did not round-trip: %#v", files[0])
	}
}
