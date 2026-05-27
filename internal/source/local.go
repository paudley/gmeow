// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

type LocalFilesystemAdapter struct {
	root string
	name string
}

func NewLocalFilesystemAdapter(name, root string) (*LocalFilesystemAdapter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errRequired("local source name")
	}

	if strings.TrimSpace(root) == "" {
		return nil, errRequired("local source root")
	}

	return &LocalFilesystemAdapter{name: name, root: root}, nil
}

func (adapter *LocalFilesystemAdapter) Name() string {
	return adapter.name
}

func (*LocalFilesystemAdapter) Kind() string {
	return "filesystem"
}

func (*LocalFilesystemAdapter) Capabilities() []string {
	return []string{CapabilityBackfill}
}

func (adapter *LocalFilesystemAdapter) Pull(
	ctx context.Context,
	_ IngestService,
	request PullRequest,
) ([]IngestObject, contracts.SourceCursor, error) {
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}

	paths, err := adapter.paths(ctx)
	if err != nil {
		return nil, contracts.SourceCursor{}, err
	}

	objects := make([]IngestObject, 0, min(limit, len(paths)))
	for _, path := range paths {
		if len(objects) >= limit {
			break
		}

		file, err := os.Open(path)
		if err != nil {
			return nil, contracts.SourceCursor{}, err
		}

		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()

			return nil, contracts.SourceCursor{}, err
		}

		relative, err := filepath.Rel(adapter.root, path)
		if err != nil {
			_ = file.Close()

			return nil, contracts.SourceCursor{}, err
		}

		objects = append(objects, IngestObject{
			ObservedAt:   stat.ModTime().UTC(),
			Reader:       closeAfterRead(file),
			MediaType:    mediaTypeFor(path, file),
			SourceKind:   adapter.Kind(),
			SourceName:   adapter.name,
			ExternalID:   filepath.ToSlash(relative),
			ExternalVer:  stat.ModTime().UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
			SourceHint:   filepath.ToSlash(relative),
			ContentRoles: []string{"source"},
			Facets: []contracts.Facet{{
				Kind: "file",
				Metadata: map[string]any{
					"display_name": filepath.Base(path),
					"path":         filepath.ToSlash(relative),
				},
			}},
		})
	}

	cursor := contracts.SourceCursor{
		SchemaVersion: contracts.SchemaVersionPhase00,
		SourceKind:    adapter.Kind(),
		SourceName:    adapter.name,
		Cursor:        map[string]any{"scanned": len(objects)},
	}

	return objects, cursor, nil
}

func (adapter *LocalFilesystemAdapter) paths(ctx context.Context) ([]string, error) {
	paths := []string{}
	err := filepath.WalkDir(
		adapter.root,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			if entry.IsDir() {
				return nil
			}

			paths = append(paths, path)

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	sort.Strings(paths)

	return paths, nil
}

func mediaTypeFor(path string, reader io.Reader) string {
	extension := strings.TrimSpace(mime.TypeByExtension(filepath.Ext(path)))
	if extension != "" {
		return extension
	}

	buffer := make([]byte, 512)
	n, _ := reader.Read(buffer)
	if seeker, ok := reader.(io.Seeker); ok {
		_, _ = seeker.Seek(0, io.SeekStart)
	}

	return http.DetectContentType(buffer[:n])
}

type readCloserReader struct {
	io.Reader
	close func() error
}

func (reader readCloserReader) Read(data []byte) (int, error) {
	return reader.Reader.Read(data)
}

func (reader readCloserReader) Close() error {
	return reader.close()
}

func closeAfterRead(file *os.File) io.Reader {
	return readCloserReader{Reader: file, close: file.Close}
}

func errRequired(name string) error {
	return &requiredError{name: name}
}

type requiredError struct {
	name string
}

func (err *requiredError) Error() string {
	return err.name + " is required"
}
