// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

// Package cache provides small goroutine-safe LRU caches for the immutable and
// permanent-positive data that pervades Gmeow (content-addressed objects and
// chunks, permanent annotation answers). Because the cached facts never change
// for a given key, the caches need no invalidation; the write-through callers
// that do cache mutable data invalidate explicitly via Remove.
package cache

import "sync"

type node[K comparable, V any] struct {
	key        K
	value      V
	prev, next *node[K, V]
}

// LRU is a goroutine-safe, count-bounded least-recently-used cache.
type LRU[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	items    map[K]*node[K, V]
	head     *node[K, V]
	tail     *node[K, V]
}

// NewLRU returns a count-bounded LRU holding at most capacity entries.
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	if capacity < 1 {
		capacity = 1
	}

	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*node[K, V], capacity),
	}
}

// Get returns the value for key and whether it was present, refreshing recency.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		var zero V

		return zero, false
	}

	c.moveToFront(entry)

	return entry.value, true
}

// Put inserts or updates key, evicting the least-recently-used entry when full.
func (c *LRU[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		entry.value = value
		c.moveToFront(entry)

		return
	}

	entry := &node[K, V]{key: key, value: value}
	c.items[key] = entry
	c.pushFront(entry)

	if len(c.items) > c.capacity {
		c.removeTail()
	}
}

// Add inserts key only if absent, leaving an existing entry (and its value)
// untouched. Lock-free readers of a write-through cache use it to populate
// without clobbering a fresher value an authoritative writer may have just Put.
func (c *LRU[K, V]) Add(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.moveToFront(entry)

		return
	}

	entry := &node[K, V]{key: key, value: value}
	c.items[key] = entry
	c.pushFront(entry)

	if len(c.items) > c.capacity {
		c.removeTail()
	}
}

// Remove drops key if present. Write-through callers use it to invalidate.
func (c *LRU[K, V]) Remove(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return
	}

	c.unlink(entry)
	delete(c.items, key)
}

// Len reports the number of cached entries.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.items)
}

func (c *LRU[K, V]) pushFront(entry *node[K, V]) {
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

func (c *LRU[K, V]) moveToFront(entry *node[K, V]) {
	if c.head == entry {
		return
	}

	c.unlink(entry)
	c.pushFront(entry)
}

func (c *LRU[K, V]) unlink(entry *node[K, V]) {
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

func (c *LRU[K, V]) removeTail() {
	if c.tail == nil {
		return
	}

	entry := c.tail
	c.unlink(entry)
	delete(c.items, entry.key)
}
