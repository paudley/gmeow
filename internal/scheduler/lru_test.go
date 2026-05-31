// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"fmt"
	"testing"
)

func TestLRUCacheAddAndContains(t *testing.T) {
	cache := newLRUCache(3)
	cache.Add("a")
	cache.Add("b")
	cache.Add("c")

	if !cache.Contains("a") || !cache.Contains("b") || !cache.Contains("c") {
		t.Fatal("expected all three keys to be present")
	}

	if cache.Len() != 3 {
		t.Fatalf("expected length 3, got %d", cache.Len())
	}
}

func TestLRUCacheEvictsOldest(t *testing.T) {
	cache := newLRUCache(2)
	cache.Add("a")
	cache.Add("b")
	cache.Add("c")

	if cache.Contains("a") {
		t.Fatal("expected 'a' to be evicted")
	}

	if !cache.Contains("b") || !cache.Contains("c") {
		t.Fatal("expected 'b' and 'c' to remain")
	}
}

func TestLRUCacheAccessRefreshesEntry(t *testing.T) {
	cache := newLRUCache(2)
	cache.Add("a")
	cache.Add("b")

	cache.Contains("a")
	cache.Add("c")

	if !cache.Contains("a") {
		t.Fatal("expected 'a' to survive after access refresh")
	}

	if cache.Contains("b") {
		t.Fatal("expected 'b' to be evicted (oldest after 'a' was refreshed)")
	}
}

func TestLRUCacheRemove(t *testing.T) {
	cache := newLRUCache(10)
	cache.Add("a")
	cache.Add("b")
	cache.Remove("a")

	if cache.Contains("a") {
		t.Fatal("expected 'a' to be removed")
	}

	if cache.Len() != 1 {
		t.Fatalf("expected length 1 after removal, got %d", cache.Len())
	}
}

func TestLRUCacheDuplicateAdd(t *testing.T) {
	cache := newLRUCache(3)
	cache.Add("a")
	cache.Add("b")
	cache.Add("a")

	if cache.Len() != 2 {
		t.Fatalf("duplicate add should not increase length, got %d", cache.Len())
	}
}

func TestLRUCacheRemoveNonexistent(t *testing.T) {
	cache := newLRUCache(3)
	cache.Add("a")
	cache.Remove("z")

	if cache.Len() != 1 {
		t.Fatalf("removing nonexistent key should be a no-op, got length %d", cache.Len())
	}
}

func TestLRUCachePermanentPositive(t *testing.T) {
	cache := newLRUCache(100)

	for i := range 50 {
		cache.Add(fmt.Sprintf("digest-%d", i))
	}

	for i := range 50 {
		if !cache.Contains(fmt.Sprintf("digest-%d", i)) {
			t.Fatalf("expected digest-%d to remain in cache (well within capacity)", i)
		}
	}
}
