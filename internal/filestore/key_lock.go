// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

func (store *FilesystemStore) lockKey(key string) func() {
	store.locksMu.Lock()
	if store.keyLocks == nil {
		store.keyLocks = map[string]*filesystemKeyLock{}
	}
	lock := store.keyLocks[key]
	if lock == nil {
		lock = &filesystemKeyLock{}
		store.keyLocks[key] = lock
	}
	lock.refs++
	store.locksMu.Unlock()

	lock.mu.Lock()

	return func() {
		lock.mu.Unlock()

		store.locksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(store.keyLocks, key)
		}
		store.locksMu.Unlock()
	}
}
