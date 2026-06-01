// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	dictionariesDir    = "dictionaries"
	currentDictMarker  = "current"
	currentFamiliesDir = "current-by-family"
	dictRegistryFile   = "registry.json"
	dictFilesDir       = "dicts"
)

type dictionaryCache struct {
	mu           sync.Mutex
	active       map[string]activeDictionary
	loaded       bool
	encoders     map[string]*zstd.Encoder
	decoders     map[string]*zstd.Decoder
	bootstrapMu  sync.Mutex
	bootstrapped bool
}

type activeDictionary struct {
	id     string
	family string
	bytes  []byte
}

type dictionaryRegistry struct {
	SchemaVersion int                                 `json:"schema_version"`
	Dictionaries  map[string]dictionaryRegistryRecord `json:"dictionaries"`
}

type dictionaryRegistryRecord struct {
	ID          string    `json:"id"`
	Family      string    `json:"family"`
	Version     string    `json:"version"`
	Filename    string    `json:"filename"`
	Description string    `json:"description,omitempty"`
	InstalledAt time.Time `json:"installed_at"`
	Samples     int       `json:"samples,omitempty"`
}

type defaultDictionary struct {
	id          string
	family      string
	version     string
	description string
	bytes       []byte
}

func defaultDictionarySeeds() []defaultDictionary {
	return []defaultDictionary{
		{
			id:          "1001",
			family:      DictionaryFamilyMailHeaders,
			version:     "bootstrap-v1",
			description: "Bootstrap dictionary seed for RFC 822-style mail headers.",
			bytes: []byte(
				"From: To: Cc: Bcc: Date: Subject: Message-ID: In-Reply-To: References:\r\n" +
					"Content-Type: text/plain; charset=utf-8\r\nMIME-Version: 1.0\r\n",
			),
		},
		{
			id:          "1002",
			family:      DictionaryFamilyMailBodyPlain,
			version:     "bootstrap-v1",
			description: "Bootstrap dictionary seed for plain-text mail bodies.",
			bytes: []byte(
				"hello thanks regards wrote forwarded message original message " +
					"unsubscribe mailing list attachment meeting update\r\n\r\n",
			),
		},
		{
			id:          "1003",
			family:      DictionaryFamilyMailBodyHTML,
			version:     "bootstrap-v1",
			description: "Bootstrap dictionary seed for HTML mail bodies.",
			bytes: []byte(
				"<html><body><div><p><br><table><tr><td><span style=\"font-family\">" +
					"</span></td></tr></table></div></body></html>",
			),
		},
		{
			id:          "1004",
			family:      DictionaryFamilyGmeowMailJSON,
			version:     "bootstrap-v1",
			description: "Bootstrap dictionary seed for Gmeow mail JSON records.",
			bytes: []byte(
				`{"schema_version":0,"message_id":"","thread_id":"","labels":[],"headers":{},` +
					`"body_media_type":"text/plain","attachments":[],"parts":[]}`,
			),
		},
		{
			id:          "1005",
			family:      DictionaryFamilyPatchText,
			version:     "bootstrap-v1",
			description: "Bootstrap dictionary seed for patch/diff text.",
			bytes:       []byte("diff --git a/ b/\nindex --- +++ @@ -1,1 +1,1 @@\n"),
		},
	}
}

func (store *FilesystemStore) activeDictionary(
	family string,
) (string, []byte, error) {
	family = sanitizeDictionaryFamily(family)
	if family == "" || family == DictionaryFamilyOpaqueBinary {
		return "", nil, nil
	}
	if err := store.ensureDictionaryBootstrap(); err != nil {
		return "", nil, err
	}

	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if !store.dicts.loaded {
		active, err := store.loadActiveDictionariesLocked()
		if err != nil {
			return "", nil, err
		}
		store.dicts.active = active
		store.dicts.loaded = true
	}

	dictionary, ok := store.dicts.active[family]
	if !ok {
		return "", nil, nil
	}

	return dictionary.id, dictionary.bytes, nil
}

func (store *FilesystemStore) ensureDictionaryBootstrap() error {
	store.dicts.bootstrapMu.Lock()
	defer store.dicts.bootstrapMu.Unlock()

	if store.dicts.bootstrapped {
		return nil
	}

	if err := store.bootstrapDefaultDictionaries(); err != nil {
		return err
	}
	store.dicts.bootstrapped = true

	return nil
}

func (store *FilesystemStore) bootstrapDefaultDictionaries() error {
	if store.hasDictionaryState() {
		return nil
	}

	now := time.Now().UTC()
	registry := dictionaryRegistry{
		SchemaVersion: 1,
		Dictionaries:  map[string]dictionaryRegistryRecord{},
	}
	for _, dictionary := range defaultDictionarySeeds() {
		if !defaultEnabledDictionaryFamily(dictionary.family) {
			continue
		}
		path := store.dictionaryPath(dictionary.id)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return err
		}
		if err := atomicWriteFile(path, dictionary.bytes, 0o640); err != nil {
			return err
		}

		filename := filepath.Join(dictFilesDir, dictionary.id+".dict")
		registry.Dictionaries[dictionary.id] = dictionaryRegistryRecord{
			ID:          dictionary.id,
			Family:      dictionary.family,
			Version:     dictionary.version,
			Filename:    filename,
			Description: dictionary.description,
			InstalledAt: now,
		}
	}

	if err := store.writeDictionaryRegistry(registry); err != nil {
		return err
	}
	for _, dictionary := range defaultDictionarySeeds() {
		if !defaultEnabledDictionaryFamily(dictionary.family) {
			continue
		}
		if err := atomicWriteFile(
			store.dictionaryCurrentPath(dictionary.family),
			[]byte(dictionary.id),
			0o640,
		); err != nil {
			return err
		}
	}

	return nil
}

func (store *FilesystemStore) hasDictionaryState() bool {
	return store.registryHasDictionaryState() ||
		fileHasTrimmedContent(
			filepath.Join(store.root, dictionariesDir, currentDictMarker),
		) ||
		directoryHasEntries(store.dictionaryCurrentDir()) ||
		directoryHasEntries(filepath.Join(store.root, dictionariesDir, dictFilesDir))
}

func (store *FilesystemStore) registryHasDictionaryState() bool {
	data, err := os.ReadFile(store.dictionaryRegistryPath())
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		return true
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return false
	}

	var registry dictionaryRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return true
	}

	return len(registry.Dictionaries) > 0
}

func fileHasTrimmedContent(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	return strings.TrimSpace(string(data)) != ""
}

func directoryHasEntries(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}

	return len(entries) > 0
}

func (store *FilesystemStore) loadActiveDictionariesLocked() (
	map[string]activeDictionary,
	error,
) {
	registry, err := store.readDictionaryRegistry()
	if err != nil {
		return nil, err
	}

	active := map[string]activeDictionary{}
	currentDir := store.dictionaryCurrentDir()
	entries, err := os.ReadDir(currentDir)
	if errors.Is(err, os.ErrNotExist) {
		return active, nil
	}
	if err != nil {
		if info, statErr := os.Stat(currentDir); statErr == nil && !info.IsDir() {
			return active, nil
		}
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		family := sanitizeDictionaryFamily(entry.Name())
		if family == "" {
			continue
		}
		marker := filepath.Join(currentDir, entry.Name())
		dictionary, ok, err := store.loadActiveDictionaryMarker(registry, marker, family)
		if err != nil {
			return nil, err
		}
		if ok {
			active[family] = dictionary
		}
	}

	return active, nil
}

func (store *FilesystemStore) loadActiveDictionaryMarker(
	registry dictionaryRegistry,
	marker string,
	family string,
) (activeDictionary, bool, error) {
	data, err := os.ReadFile(marker)
	if err != nil {
		return activeDictionary{}, false, err
	}
	id := strings.TrimSpace(string(data))
	if id == "" {
		return activeDictionary{}, false, nil
	}
	record, ok := registry.Dictionaries[id]
	if !ok {
		return activeDictionary{}, false, fmt.Errorf(
			"dictionary %q has no registry record",
			id,
		)
	}
	if record.Family != family {
		return activeDictionary{}, false, fmt.Errorf(
			"dictionary %q registry family %q does not match marker family %q",
			id,
			record.Family,
			family,
		)
	}
	dictBytes, err := store.readDictionaryBytes(id)
	if err != nil {
		return activeDictionary{}, false, err
	}
	if len(dictBytes) == 0 {
		return activeDictionary{}, false, fmt.Errorf("dictionary %q is empty", id)
	}

	return activeDictionary{
		id:     id,
		family: family,
		bytes:  dictBytes,
	}, true, nil
}

// reloadDictionaries forces the next activeDictionary call to re-read the
// current family markers, so a freshly trained dictionary takes effect for new
// chunks without a process restart. Cached codecs are dropped without closing
// them to avoid racing in-flight EncodeAll/DecodeAll calls.
func (store *FilesystemStore) reloadDictionaries() {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()
	store.dicts.loaded = false
	store.dicts.active = nil
	store.dicts.encoders = nil
	store.dicts.decoders = nil
}

func (store *FilesystemStore) dictionaryRegistryPath() string {
	return filepath.Join(store.root, dictionariesDir, dictRegistryFile)
}

func (store *FilesystemStore) dictionaryCurrentDir() string {
	return filepath.Join(store.root, dictionariesDir, currentFamiliesDir)
}

func (store *FilesystemStore) dictionaryCurrentPath(family string) string {
	return filepath.Join(store.dictionaryCurrentDir(), sanitizeDictionaryFamily(family))
}

func (store *FilesystemStore) dictionaryPath(id string) string {
	return filepath.Join(store.root, dictionariesDir, dictFilesDir, id+".dict")
}

func (store *FilesystemStore) legacyDictionaryPath(id string) string {
	return filepath.Join(store.root, dictionariesDir, id+".dict")
}

func (store *FilesystemStore) readDictionaryRegistry() (
	dictionaryRegistry,
	error,
) {
	registry := dictionaryRegistry{
		SchemaVersion: 1,
		Dictionaries:  map[string]dictionaryRegistryRecord{},
	}
	data, err := os.ReadFile(store.dictionaryRegistryPath())
	if errors.Is(err, os.ErrNotExist) {
		return registry, nil
	}
	if err != nil {
		return registry, err
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		return registry, err
	}
	if registry.Dictionaries == nil {
		registry.Dictionaries = map[string]dictionaryRegistryRecord{}
	}

	return registry, nil
}

func (store *FilesystemStore) writeDictionaryRegistry(
	registry dictionaryRegistry,
) error {
	if registry.SchemaVersion == 0 {
		registry.SchemaVersion = 1
	}
	if registry.Dictionaries == nil {
		registry.Dictionaries = map[string]dictionaryRegistryRecord{}
	}
	encoded, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return err
	}

	return atomicWriteFile(store.dictionaryRegistryPath(), encoded, 0o640)
}

func (store *FilesystemStore) dictEncoder(
	id string,
	dict []byte,
) (*zstd.Encoder, error) {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if store.dicts.encoders == nil {
		store.dicts.encoders = map[string]*zstd.Encoder{}
	}
	if encoder, ok := store.dicts.encoders[id]; ok {
		return encoder, nil
	}

	var options []zstd.EOption
	if len(dict) > 0 {
		rawID, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid dictionary id %q: %w", id, err)
		}
		options = append(options, zstd.WithEncoderDictRaw(uint32(rawID), dict))
	}
	encoder, err := zstd.NewWriter(nil, options...)
	if err != nil {
		return nil, err
	}
	store.dicts.encoders[id] = encoder

	return encoder, nil
}

func (store *FilesystemStore) dictDecoder(id string) (*zstd.Decoder, error) {
	store.dicts.mu.Lock()
	defer store.dicts.mu.Unlock()

	if store.dicts.decoders == nil {
		store.dicts.decoders = map[string]*zstd.Decoder{}
	}
	if decoder, ok := store.dicts.decoders[id]; ok {
		return decoder, nil
	}

	var options []zstd.DOption
	if id != "" {
		dict, err := store.readDictionaryBytes(id)
		if err != nil {
			return nil, err
		}
		rawID, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid dictionary id %q: %w", id, err)
		}
		options = append(options, zstd.WithDecoderDictRaw(uint32(rawID), dict))
	}
	decoder, err := zstd.NewReader(nil, options...)
	if err != nil {
		return nil, err
	}
	store.dicts.decoders[id] = decoder

	return decoder, nil
}

func (store *FilesystemStore) readDictionaryBytes(id string) ([]byte, error) {
	dict, err := os.ReadFile(store.dictionaryPath(id))
	if err == nil {
		return dict, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	return os.ReadFile(store.legacyDictionaryPath(id))
}

func (store *FilesystemStore) compressChunkContent(
	chunk []byte,
	family string,
) ([]byte, string, string, error) {
	id, dict, err := store.activeDictionary(family)
	if err != nil {
		return nil, "", "", err
	}
	encoder, err := store.dictEncoder(id, dict)
	if err != nil {
		return nil, "", "", err
	}
	selectedFamily := ""
	if family := sanitizeDictionaryFamily(family); family != "" &&
		family != DictionaryFamilyOpaqueBinary {
		selectedFamily = family
	}

	return encoder.EncodeAll(chunk, nil), id, selectedFamily, nil
}

func (store *FilesystemStore) decompressChunkContent(
	compressed []byte,
	dictID string,
) ([]byte, error) {
	decoder, err := store.dictDecoder(dictID)
	if err != nil {
		return nil, err
	}

	return decoder.DecodeAll(compressed, nil)
}
