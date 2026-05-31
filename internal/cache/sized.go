// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cache

import "sync"

type sizedNode[K comparable] struct {
	key        K
	value      []byte
	prev, next *sizedNode[K]
}

// SizedLRU is a goroutine-safe LRU bounded by the total byte size of its values.
// It is for immutable byte payloads (decompressed chunks, small object content):
// cached slices are treated as read-only and must not be mutated by callers.
type SizedLRU[K comparable] struct {
	mu       sync.Mutex
	maxBytes int
	curBytes int
	items    map[K]*sizedNode[K]
	head     *sizedNode[K]
	tail     *sizedNode[K]
}

// NewSizedLRU returns a byte-bounded LRU holding at most maxBytes of values.
func NewSizedLRU[K comparable](maxBytes int) *SizedLRU[K] {
	if maxBytes < 1 {
		maxBytes = 1
	}

	return &SizedLRU[K]{
		maxBytes: maxBytes,
		items:    make(map[K]*sizedNode[K]),
	}
}

// Get returns the cached payload for key and whether it was present.
func (c *SizedLRU[K]) Get(key K) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return nil, false
	}

	c.moveToFront(entry)

	return entry.value, true
}

// Put caches value under key. Values larger than the whole budget are skipped so
// one oversized payload cannot evict everything; otherwise the least-recently-used
// entries are evicted until the new value fits.
func (c *SizedLRU[K]) Put(key K, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(value) > c.maxBytes {
		// Too big to cache; also drop any existing entry so a later Get cannot
		// return a now-stale prior value for this key.
		if entry, ok := c.items[key]; ok {
			c.unlink(entry)
			c.curBytes -= len(entry.value)
			delete(c.items, key)
		}

		return
	}

	if entry, ok := c.items[key]; ok {
		c.curBytes += len(value) - len(entry.value)
		entry.value = value
		c.moveToFront(entry)
		c.evictToFit()

		return
	}

	entry := &sizedNode[K]{key: key, value: value}
	c.items[key] = entry
	c.curBytes += len(value)
	c.pushFront(entry)
	c.evictToFit()
}

// Remove drops key if present.
func (c *SizedLRU[K]) Remove(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return
	}

	c.unlink(entry)
	c.curBytes -= len(entry.value)
	delete(c.items, key)
}

// Bytes reports the current total size of cached values.
func (c *SizedLRU[K]) Bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.curBytes
}

func (c *SizedLRU[K]) evictToFit() {
	for c.curBytes > c.maxBytes && c.tail != nil {
		c.removeTail()
	}
}

func (c *SizedLRU[K]) pushFront(entry *sizedNode[K]) {
	entry.prev = nil
	entry.next = c.head

	if c.head != nil {
		c.head.prev = entry
	}

	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

func (c *SizedLRU[K]) moveToFront(entry *sizedNode[K]) {
	if c.head == entry {
		return
	}

	c.unlink(entry)
	c.pushFront(entry)
}

func (c *SizedLRU[K]) unlink(entry *sizedNode[K]) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}

	entry.prev = nil
	entry.next = nil
}

func (c *SizedLRU[K]) removeTail() {
	if c.tail == nil {
		return
	}

	entry := c.tail
	c.unlink(entry)
	c.curBytes -= len(entry.value)
	delete(c.items, entry.key)
}
