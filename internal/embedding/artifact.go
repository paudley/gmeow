// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// artifactMagic versions the EntityIndex snapshot format. Bump on layout change.
const artifactMagic = "GMEOWHNSW1"

func writeArtifactHeader(w io.Writer, dim, coarseDim, nameCount int) error {
	if _, err := w.Write([]byte(artifactMagic)); err != nil {
		return err
	}

	return writeUint32s(w, uint32(dim), uint32(coarseDim), uint32(nameCount))
}

func readArtifactHeader(r io.Reader) (dim, coarseDim, nameCount int, err error) {
	magic := make([]byte, len(artifactMagic))
	if _, err = io.ReadFull(r, magic); err != nil {
		return 0, 0, 0, fmt.Errorf("read artifact magic: %w", err)
	}

	if string(magic) != artifactMagic {
		return 0, 0, 0, errors.New("unrecognized entity-index artifact magic")
	}

	d, err := readUint32s(r, 3)
	if err != nil {
		return 0, 0, 0, err
	}

	return int(d[0]), int(d[1]), int(d[2]), nil
}

func writeUint32s(w io.Writer, values ...uint32) error {
	for _, v := range values {
		err := binary.Write(w, binary.LittleEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func readUint32s(r io.Reader, n int) ([]uint32, error) {
	out := make([]uint32, n)
	for i := range out {
		err := binary.Read(r, binary.LittleEndian, &out[i])
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func writeLenBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		return err
	}

	_, err := w.Write(data)

	return err
}

func readLenBytes(r io.Reader) ([]byte, error) {
	var n uint64

	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}

	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}

func writeLenString(w io.Writer, s string) error {
	return writeLenBytes(w, []byte(s))
}

func readLenString(r io.Reader) (string, error) {
	data, err := readLenBytes(r)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
