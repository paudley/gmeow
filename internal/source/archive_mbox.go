// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

func forEachMboxMessageOffset(path string, fn func(offset, size int64) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	var size int64
	var offset int64
	inMessage := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "From ") {
			if inMessage {
				if err := fn(offset, size); err != nil {
					return err
				}
				offset++
				size = 0
			}
			inMessage = true
			continue
		}
		if inMessage {
			size += int64(len(line) + 1)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if inMessage {
		return fn(offset, size)
	}

	return nil
}

func parseMboxFile(path, root string) ([]archiveMessage, error) {
	messages := []archiveMessage{}
	err := forEachMboxMessage(path, root, func(message archiveMessage) error {
		messages = append(messages, message)
		return nil
	})

	return messages, err
}

func forEachMboxMessage(path, root string, fn func(archiveMessage) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	var current bytes.Buffer
	offset := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "From ") {
			if current.Len() > 0 {
				message, err := parseArchiveMessage(
					current.Bytes(),
					path,
					root,
					ArchiveImportFormatMbox,
					offset,
				)
				if err != nil {
					return fmt.Errorf("parse mbox message %s#%d: %w", path, offset, err)
				}
				if err := fn(message); err != nil {
					return err
				}
				current.Reset()
				offset++
			}
			continue
		}
		if strings.HasPrefix(line, ">From ") {
			line = strings.TrimPrefix(line, ">")
		}
		current.WriteString(line)
		current.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if current.Len() > 0 {
		message, err := parseArchiveMessage(
			current.Bytes(),
			path,
			root,
			ArchiveImportFormatMbox,
			offset,
		)
		if err != nil {
			return fmt.Errorf("parse mbox message %s#%d: %w", path, offset, err)
		}
		if err := fn(message); err != nil {
			return err
		}
	}

	return nil
}

func parseMboxMessageAt(path, root string, wantedOffset int) (archiveMessage, error) {
	file, err := os.Open(path)
	if err != nil {
		return archiveMessage{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	var current bytes.Buffer
	offset := -1
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "From ") {
			if offset == wantedOffset && current.Len() > 0 {
				return parseArchiveMessage(
					current.Bytes(),
					path,
					root,
					ArchiveImportFormatMbox,
					wantedOffset,
				)
			}
			offset++
			current.Reset()
			continue
		}
		if offset != wantedOffset {
			continue
		}
		if strings.HasPrefix(line, ">From ") {
			line = strings.TrimPrefix(line, ">")
		}
		current.WriteString(line)
		current.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return archiveMessage{}, err
	}
	if offset == wantedOffset && current.Len() > 0 {
		return parseArchiveMessage(
			current.Bytes(),
			path,
			root,
			ArchiveImportFormatMbox,
			wantedOffset,
		)
	}

	return archiveMessage{}, fmt.Errorf(
		"mbox message offset %d not found in %s",
		wantedOffset,
		path,
	)
}
