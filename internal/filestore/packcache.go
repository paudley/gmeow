// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"container/list"
	"os"
	"sync"
)

// defaultPackCacheSize bounds how many pack file descriptors the store keeps
// open at once. At full-corpus scale there are far more packs than a process
// should hold descriptors for, so the cache is an LRU; an object's chunks tend
// to cluster in a few recently written packs, so a modest cache captures the
// locality.
const defaultPackCacheSize = 128

// packCache caches read-only handles to pack files so reassembling an object
// with many chunks does not open the same pack once per chunk. Sealed packs are
// immutable and the active pack is append-only, so a cached descriptor stays
// valid for positioned reads. os.File.ReadAt is safe for concurrent use, so one
// handle is shared by many readers. Handles are reference-counted: an evicted or
// invalidated handle is closed only once its last in-flight reader releases it,
// so a concurrent repack or LRU eviction can never close a descriptor mid-read.
type packCache struct {
	mu      sync.Mutex
	max     int
	entries map[uint64]*packCacheEntry
	lru     *list.List // element value is *packCacheEntry, front = most recent
}

type packCacheEntry struct {
	file    *os.File
	elem    *list.Element
	id      uint64
	refs    int
	evicted bool
}

func newPackCache(max int) *packCache {
	if max <= 0 {
		max = defaultPackCacheSize
	}

	return &packCache{
		max:     max,
		entries: make(map[uint64]*packCacheEntry),
		lru:     list.New(),
	}
}

// acquire returns a read-only handle to the pack with the given id, opening it
// (via openPack) on a miss. The returned release function must be called when
// the caller is done reading; it drops the reference and closes the descriptor
// if the entry has since been evicted or invalidated.
func (cache *packCache) acquire(
	id uint64,
	openPack func(uint64) (*os.File, error),
) (*os.File, func(), error) {
	cache.mu.Lock()

	if entry, ok := cache.entries[id]; ok {
		entry.refs++
		cache.lru.MoveToFront(entry.elem)
		cache.mu.Unlock()

		return entry.file, cache.releaser(entry), nil
	}

	cache.mu.Unlock()

	// Open outside the lock so a slow open does not block other readers.
	file, err := openPack(id)
	if err != nil {
		return nil, nil, err
	}

	cache.mu.Lock()
	// Another goroutine may have inserted the same pack while we opened it.
	if entry, ok := cache.entries[id]; ok {
		entry.refs++
		cache.lru.MoveToFront(entry.elem)
		cache.mu.Unlock()
		_ = file.Close()

		return entry.file, cache.releaser(entry), nil
	}

	entry := &packCacheEntry{id: id, file: file, refs: 1}
	entry.elem = cache.lru.PushFront(entry)
	cache.entries[id] = entry
	cache.evictLocked()
	cache.mu.Unlock()

	return file, cache.releaser(entry), nil
}

func (cache *packCache) releaser(entry *packCacheEntry) func() {
	return func() {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		entry.refs--
		if entry.refs == 0 && entry.evicted {
			_ = entry.file.Close()
		}
	}
}

// evictLocked closes the least-recently-used entries that have no in-flight
// readers until the cache is within its bound. Entries still in use are skipped
// and closed later by their releaser. Caller holds cache.mu.
func (cache *packCache) evictLocked() {
	for len(cache.entries) > cache.max {
		elem := cache.lru.Back()
		if elem == nil {
			return
		}
		entry, _ := elem.Value.(*packCacheEntry)
		cache.lru.Remove(elem)
		delete(cache.entries, entry.id)
		entry.evicted = true
		if entry.refs == 0 {
			_ = entry.file.Close()
		}
	}
}

// invalidate drops a cached handle (used when a pack is superseded by repack).
// The descriptor is closed once its last reader releases it.
func (cache *packCache) invalidate(id uint64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.entries[id]
	if !ok {
		return
	}
	cache.lru.Remove(entry.elem)
	delete(cache.entries, id)
	entry.evicted = true
	if entry.refs == 0 {
		_ = entry.file.Close()
	}
}

// closeAll closes every cached descriptor. Called from FilesystemStore.Close.
func (cache *packCache) closeAll() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for id, entry := range cache.entries {
		cache.lru.Remove(entry.elem)
		delete(cache.entries, id)
		entry.evicted = true
		if entry.refs == 0 {
			_ = entry.file.Close()
		}
	}
}
