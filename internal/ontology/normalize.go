// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

import "strings"

// Value normalization — ONE function per concept, the single home for what was
// scattered across contactio (normalize{Phone,URL,Fingerprint}Value) and
// contactentity (NormalizeIdentity/NormalizeAlias). The persisted record keeps
// the source value; these produce the canonical form used for embedding/keying/
// comparison so formatting variants of the same value collapse.

// NormText lower-cases and collapses whitespace — the default.
func NormText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

// NormEmail canonicalizes an email locator: strip a mailto: scheme and any
// display-name, lower-case the address. Accepts a bare address, a "Name <addr>"
// form, or a mailto: IRI.
func NormEmail(value string) string {
	v := strings.TrimSpace(value)
	v = strings.TrimPrefix(strings.TrimPrefix(v, "mailto:"), "MAILTO:")

	// "Display Name <addr>" → addr
	if open := strings.LastIndex(v, "<"); open >= 0 {
		if shut := strings.Index(v[open:], ">"); shut >= 0 {
			v = v[open+1 : open+shut]
		}
	}

	return strings.ToLower(strings.TrimSpace(v))
}

// NormPhone reduces a phone to its dialable digits, dropping a leading
// North-American "1" so "+1 555…" and "555…" collapse. Strips a tel: scheme.
// Falls back to NormText when no digits remain (junk value, preserved as text).
func NormPhone(value string) string {
	v := strings.TrimPrefix(strings.TrimSpace(value), "tel:")

	var digits strings.Builder

	for _, r := range v {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}

	number := digits.String()
	if len(number) == 11 && number[0] == '1' {
		number = number[1:]
	}

	if number == "" {
		return NormText(value)
	}

	return number
}

// NormURL lower-cases and trims a trailing slash so "http://x.com" and
// "http://x.com/" collapse.
func NormURL(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), "/")
}
