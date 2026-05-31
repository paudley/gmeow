// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import "testing"

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewLRU[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)

	// Touch "a" so "b" becomes least-recently-used.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a present")
	}

	c.Put("c", 3) // evicts "b"

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("expected a=1, got %d ok=%t", v, ok)
	}
	if v, ok := c.Get("c"); !ok || v != 3 {
		t.Fatalf("expected c=3, got %d ok=%t", v, ok)
	}
}

func TestLRURemoveAndUpdate(t *testing.T) {
	c := NewLRU[string, string](4)
	c.Put("k", "v1")
	c.Put("k", "v2")

	if v, _ := c.Get("k"); v != "v2" {
		t.Fatalf("expected updated value v2, got %q", v)
	}

	c.Remove("k")

	if _, ok := c.Get("k"); ok {
		t.Fatal("expected k removed")
	}
	if c.Len() != 0 {
		t.Fatalf("expected empty cache, got len %d", c.Len())
	}
}

func TestSizedLRUEvictsByBytes(t *testing.T) {
	c := NewSizedLRU[string](10)
	c.Put("a", make([]byte, 6))
	c.Put("b", make([]byte, 3)) // total 9

	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a present")
	}

	c.Put(
		"c",
		make([]byte, 5),
	) // total would be 14 -> evict LRU ("b") -> 11 -> evict "a"? a touched last

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b evicted by byte pressure")
	}
	if c.Bytes() > 10 {
		t.Fatalf("expected total bytes <= 10, got %d", c.Bytes())
	}
}

func TestSizedLRUSkipsOversizedValue(t *testing.T) {
	c := NewSizedLRU[string](4)
	c.Put("big", make([]byte, 100))

	if _, ok := c.Get("big"); ok {
		t.Fatal("oversized value must not be cached")
	}
	if c.Bytes() != 0 {
		t.Fatalf("expected 0 bytes, got %d", c.Bytes())
	}
}
