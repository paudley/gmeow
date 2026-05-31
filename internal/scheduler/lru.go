// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import "sync"

type lruEntry struct {
	key        string
	prev, next *lruEntry
}

// lruCache is a bounded set of strings with LRU eviction. It tracks permanent-
// positive answers to "is this object fully annotated?" — once true, always
// true (annotations are never removed), so caching the positive is sound.
type lruCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*lruEntry
	head     *lruEntry
	tail     *lruEntry
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*lruEntry, capacity),
	}
}

func (c *lruCache) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return false
	}

	c.moveToFront(entry)

	return true
}

func (c *lruCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.moveToFront(entry)

		return
	}

	entry := &lruEntry{key: key}
	c.items[key] = entry
	c.pushFront(entry)

	if len(c.items) > c.capacity {
		c.removeTail()
	}
}

func (c *lruCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return
	}

	c.unlink(entry)
	delete(c.items, key)
}

func (c *lruCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.items)
}

func (c *lruCache) pushFront(entry *lruEntry) {
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

func (c *lruCache) moveToFront(entry *lruEntry) {
	if c.head == entry {
		return
	}

	c.unlink(entry)
	c.pushFront(entry)
}

func (c *lruCache) unlink(entry *lruEntry) {
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

func (c *lruCache) removeTail() {
	if c.tail == nil {
		return
	}

	entry := c.tail
	c.unlink(entry)
	delete(c.items, entry.key)
}
