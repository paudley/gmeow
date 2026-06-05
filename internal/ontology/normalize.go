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

// minPhoneDigits is the fewest dialable digits a value must have to be a phone
// IDENTIFIER. Below this (e.g. "0", "1", "2", a misrouted flag or extension) the
// value carries no identity and is rejected — it must never seed the identifier
// index, where degenerate values like "0" otherwise pool unrelated people. Seven
// is the NANP subscriber-number length (the local minimum).
const minPhoneDigits = 7

// NormPhone reduces a phone to its dialable digits, dropping a leading
// North-American "1" so "+1 555…" and "555…" collapse. Strips a tel: scheme.
// Returns "" for a value with too few digits to be a real number (junk), so the
// claim is dropped rather than admitted as a degenerate identifier.
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

	if len(number) < minPhoneDigits {
		return ""
	}

	return number
}

// imPseudoEmailSuffix marks the user's long-running hack where IM/XMPP accounts
// were gatewayed into email addresses (see memory im-pseudo-email-hack): an
// address "<xep0106-escaped-jid>@<service>.i.blackcat.ca" is an IM account, not a
// real email.
const imPseudoEmailSuffix = ".i.blackcat.ca"

// IMAccountFromPseudoEmail detects an "<escaped-jid>@<service>.i.blackcat.ca"
// pseudo-email and returns the canonical account comparison value "service:jid"
// (true). The caller re-types the claim from email to account so it never seeds
// the email identifier index, and two observations of the same IM account
// converge. Returns ok=false for an ordinary email.
func IMAccountFromPseudoEmail(email string) (string, bool) {
	v := strings.ToLower(strings.TrimSpace(email))

	at := strings.LastIndex(v, "@")
	if at < 0 {
		return "", false
	}

	local, host := v[:at], v[at+1:]
	if local == "" || !strings.HasSuffix(host, imPseudoEmailSuffix) {
		return "", false
	}

	service := strings.Trim(strings.TrimSuffix(host, imPseudoEmailSuffix), ".")
	if service == "" {
		return "", false
	}

	return service + ":" + decodeJIDEscapes(local), true
}

// decodeJIDEscapes reverses the XEP-0106 JID escaping (and the corpus's re-encoded
// "%5c"/"&#92;" backslash forms) so "bill.trembley%5c40gmail.com" → the real JID
// "bill.trembley@gmail.com".
func decodeJIDEscapes(s string) string {
	s = strings.NewReplacer("%5c", "\\", "&#92;", "\\", "&#92", "\\", "%40", "@").
		Replace(s)

	return strings.NewReplacer(
		`\40`, "@", `\5c`, "\\", `\20`, " ", `\26`, "&", `\2f`, "/",
		`\3a`, ":", `\3c`, "<", `\3e`, ">", `\27`, "'", `\22`, `"`,
	).Replace(s)
}

// NormURL canonicalizes a URL: decode the backslash corruptions vendors emit
// (an HTML entity "&#92;" or percent-encoding "%5c"/"%5C", with or without a
// trailing ";"), drop stray backslashes, lower-case, and trim a trailing slash —
// so "http://x.com", "http%5c://x.com", and "http&#92;//x.com" collapse to one
// key instead of fracturing the same locator across distinct entities.
func NormURL(value string) string {
	v := strings.TrimSpace(value)
	for _, esc := range []string{"&#92;", "&#92", "%5c", "%5C"} {
		v = strings.ReplaceAll(v, esc, "")
	}
	v = strings.ReplaceAll(v, "\\", "")

	return strings.TrimSuffix(strings.ToLower(v), "/")
}
