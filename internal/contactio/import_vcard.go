// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"errors"
	"fmt"
	"io"
	"mime/quotedprintable"
	"sort"
	"strings"

	"blackcat.ca/gmeow/internal/ontology"
)

func vcardToRDF(content string) ([]renderedContact, []RecordRejection, error) {
	cards, rejections := parseVCards(content)
	if len(cards) == 0 && len(rejections) == 0 {
		return nil, nil, errors.New("vCard import has no cards")
	}

	records := make([]renderedContact, 0, len(cards))
	for _, card := range cards {
		records = append(
			records,
			renderedContact{identity: card.subject, body: vcardContactBody(card)},
		)
	}

	return records, rejections, nil
}

func vcardContactBody(card vcardContact) string {
	// Entity classification: only an agent-denoting card (has a name, org, or
	// explicit KIND) is asserted to be a foaf:Person. A locator-only card
	// (email-only, phone-only, URL-only, …) is a contact-point observation and
	// must not mint a person — see the contact ingestion redesign.
	var claims []Claim
	if vcardIsAgentDenoting(card.values) {
		claims = append(
			claims,
			Claim{
				Subject:   card.subject,
				Predicate: rdfType,
				Object:    termIRI(foafPrefix + "Person"),
			},
		)
	}

	// Each vCard line either maps to a canonical concept (emitted as a
	// standards-first node structure via the shared emitter, with gmeow:mappedFrom
	// provenance) or is genuine source-metadata / a vendor extension preserved
	// losslessly via its documented source-property predicate.
	for _, line := range card.lines {
		if line.value == "" {
			continue
		}
		if mapping, ok := ontology.VCardMapping(line.name); ok {
			claims = emitCanonical(claims, card.subject, mapping, line.value, "vcard:"+line.name)

			continue
		}
		claims = appendVCardSourceLineClaim(claims, card.subject, line)
	}

	return BuildRDFStarDelta(claims)
}

// appendVCardSourceLineClaim preserves a non-concept vCard line (source metadata
// or vendor extension) under its documented source-property predicate, with
// provider-lifecycle and parameter annotations.
func appendVCardSourceLineClaim(
	claims []Claim,
	subject string,
	line vcardLine,
) []Claim {
	predicate := vcardSourcePropertyPredicate(line.name)
	if predicate == "" {
		return claims
	}

	claim := Claim{Subject: subject, Predicate: predicate, Object: termLiteral(line.value)}
	if lifecycle, found := providerLifecycleByPredicate[predicate]; found &&
		lifecycle.ValidUntil != "" {
		claim = claim.withAnnotation(timePrefix+"hasEnd", termTypedDate(lifecycle.ValidUntil))
	}

	for param, values := range line.params {
		paramPredicate := vcardParamPredicates[param]
		for _, value := range values {
			if value == "" {
				continue
			}
			claim = claim.withAnnotation(paramPredicate, termLiteral(value))
		}
	}

	return append(claims, claim)
}

// parseVCards splits content into vCard records and parses each independently.
// A problem inside one card (unmapped property, malformed/unfolded line, missing
// END) rejects only that card and is recorded as a RecordRejection; the remaining
// cards still import. This is the logical-record boundary: one damaged record in a
// large multi-card export must not discard the rest.
func parseVCards(content string) ([]vcardContact, []RecordRejection) {
	lines := unfoldVCardLines(content)
	cards := []vcardContact{}
	rejections := []RecordRejection{}

	var current map[string][]string
	var currentLines []vcardLine
	recordIndex := 0 // 1-based index of the current card within the file
	poison := ""     // non-empty => current card is rejected with this reason

	finish := func() {
		if current == nil {
			return
		}
		if poison != "" {
			rejections = append(rejections, RecordRejection{
				Format: FormatVCard,
				Index:  recordIndex,
				Reason: poison,
			})
		} else {
			cards = append(cards, vcardContact{
				lines:   append([]vcardLine{}, currentLines...),
				subject: contactSubjectForVCard(current),
				values:  current,
			})
		}
		current = nil
		currentLines = nil
		poison = ""
	}

	for lineNumber, line := range lines {
		name, value, found := strings.Cut(line, ":")
		if !found {
			if current != nil && poison == "" {
				poison = fmt.Sprintf("line %d has no ':' separator", lineNumber+1)
			}
			continue
		}

		parsed, err := parseVCardLineHead(name)
		if err != nil {
			if current != nil && poison == "" {
				poison = fmt.Sprintf("line %d: %v", lineNumber+1, err)
			}
			continue
		}
		name = parsed.name
		value = decodeVCardValue(value, parsed.params)

		switch name {
		case "BEGIN":
			if strings.EqualFold(value, "VCARD") {
				if current != nil { // previous card never ended
					if poison == "" {
						poison = "unterminated vCard"
					}
					finish()
				}
				current = map[string][]string{}
				currentLines = []vcardLine{}
				recordIndex++
				poison = ""
			}
		case "END":
			if strings.EqualFold(value, "VCARD") && current != nil {
				finish()
			}
		default:
			if current == nil || poison != "" || value == "" {
				continue
			}
			if !supportedVCardProperty(name) {
				poison = fmt.Sprintf("unsupported vCard property %q on line %d", name, lineNumber+1)
				continue
			}
			current[name] = append(current[name], value)
			currentLines = append(currentLines, vcardLine{
				name:   name,
				params: parsed.params,
				value:  value,
			})
		}
	}

	if current != nil { // unterminated trailing card
		if poison == "" {
			poison = "unterminated vCard"
		}
		finish()
	}

	return cards, rejections
}

var supportedVCardProperties = map[string]bool{
	"ADR":                                  true,
	"AIM":                                  true,
	"EMAIL":                                true,
	"FN":                                   true,
	"N":                                    true,
	"NICKNAME":                             true,
	"NOTE":                                 true,
	"ORG":                                  true,
	"TEL":                                  true,
	"TITLE":                                true,
	"UID":                                  true,
	"URL":                                  true,
	"VERSION":                              true,
	"BDAY":                                 true,
	"CATEGORIES":                           true,
	"CLASS":                                true,
	"FBURL":                                true,
	"GEO":                                  true,
	"GOOGLE-TALK":                          true,
	"GTALK":                                true,
	"ICQ":                                  true,
	"IMPP":                                 true,
	"JABBER":                               true,
	"KEY":                                  true,
	"KIND":                                 true,
	"LABEL":                                true,
	"LOGO":                                 true,
	"MAILER":                               true,
	"MSN":                                  true,
	"PHOTO":                                true,
	"PRODID":                               true,
	"PROFILE":                              true,
	"REV":                                  true,
	"ROLE":                                 true,
	"SORT-STRING":                          true,
	"SOUND":                                true,
	"SOURCE":                               true,
	"TZ":                                   true,
	"X-ABADR":                              true,
	"X-XMPP":                               true,
	"YAHOO":                                true,
	"@FRIEND @TRACKED=0A=0AE-MAIL ADDRESS": true,
	"@FRIEND @TRACKED=0A=0AIM":             true,
	"@WORK-BOATS=0A=0AE-MAIL ADDRESS":      true,
	"@WORK-BOATS=0A=0AIM":                  true,
	"=0AIM":                                true,
	"#K5631C2&AMP":                         true,
	"ESS":                                  true,
	"[REF]=0A[REF]=0ACATEGORIES":           true,
	"X-ABLABEL":                            true,
	"X-ABRELATEDNAMES":                     true,
	"X-ABSHOWAS":                           true,
	"X-ABUID":                              true,
	"X-AIM":                                true,
	"X-CAT-IDS":                            true,
	"X-CID":                                true,
	"X-CREATED":                            true,
	"X-ETAG":                               true,
	"X-FC-LIST-ID":                         true,
	"X-FC-TAGS":                            true,
	"X-GENDER":                             true,
	"X-GTALK":                              true,
	"X-ID":                                 true,
	"X-JABBER":                             true,
	"X-MYSPACE":                            true,
	"X-MODIFIED":                           true,
	"X-MSN":                                true,
	"X-MAIDENNAME":                         true,
	"X-PHONETIC-LAST-NAME":                 true,
	"X-PRIMARY-PHONE":                      true,
	"X-SOCIALPROFILE":                      true,
	"X-WINDOWS-LIVE":                       true,
	"X-YAHOO":                              true,
	"ADDRESS":                              true,
	"BIRTHDAY":                             true,
	"BUSINESS STREET":                      true,
	"CITY":                                 true,
	"COUNTRY":                              true,
	"IM":                                   true,
	"PIN":                                  true,
	"STATE":                                true,
	"WEB ID":                               true,
	"X-ABDATE":                             true,
	"X-AIM-ID":                             true,
	"X-EVOLUTION-FILE-AS":                  true,
	"X-FC-CLUSTER-ID":                      true,
	"X-FC-ENRICHED-ETAG":                   true,
	"X-FC-REMOTE-ACCOUNT":                  true,
	"X-ICQ":                                true,
	"X-ICQ-ID":                             true,
	"X-IM":                                 true,
	"X-MSN-ID":                             true,
	"X-SKYPE":                              true,
	"X-SKYPE-ID":                           true,
	"X-YAHOO-ID":                           true,
}

func supportedVCardProperty(name string) bool {
	// Any X- vendor-extension property is supported by rule: it maps
	// deterministically to a distinct gmeow:vendorExtension/<vendor>/<name>
	// predicate (see vcardVendorExtensionPredicate), so an unrecognized vendor
	// dialect never fails the file. This is a documented mapping with infinite
	// domain, not an opaque catch-all bucket.
	return supportedVCardProperties[name] ||
		strings.HasPrefix(name, "X-") ||
		legacyEncodedVCardFragmentPredicate(name) != ""
}

var vcardSourcePropertyPredicates = map[string]string{
	"ADR":                                  gmeowPrefix + "vcardAddress",
	"AIM":                                  gmeowPrefix + "aimIdentity",
	"BDAY":                                 schemaPrefix + "birthDate",
	"CATEGORIES":                           gmeowPrefix + "vcardCategories",
	"CLASS":                                gmeowPrefix + "vcardClass",
	"EMAIL":                                gmeowPrefix + "vcardEmail",
	"FBURL":                                gmeowPrefix + "vcardFreeBusyURL",
	"FN":                                   gmeowPrefix + "vcardFormattedName",
	"GEO":                                  schemaPrefix + "geo",
	"GOOGLE-TALK":                          gmeowPrefix + "googleTalkIdentity",
	"GTALK":                                gmeowPrefix + "googleTalkIdentity",
	"ICQ":                                  gmeowPrefix + "icqIdentity",
	"IMPP":                                 vcardPrefix + "hasInstantMessage",
	"JABBER":                               gmeowPrefix + "jabberIdentity",
	"KEY":                                  gmeowPrefix + "vcardKey",
	"KIND":                                 gmeowPrefix + "vcardKind",
	"LABEL":                                gmeowPrefix + "vcardLabel",
	"LOGO":                                 gmeowPrefix + "vcardLogo",
	"MAILER":                               gmeowPrefix + "vcardMailer",
	"MSN":                                  gmeowPrefix + "msnIdentity",
	"N":                                    gmeowPrefix + "vcardNameComponents",
	"NICKNAME":                             gmeowPrefix + "vcardNickname",
	"NOTE":                                 gmeowPrefix + "vcardNote",
	"ORG":                                  gmeowPrefix + "vcardOrganization",
	"PHOTO":                                schemaPrefix + "image",
	"PRODID":                               gmeowPrefix + "vcardProductID",
	"PROFILE":                              gmeowPrefix + "vcardProfile",
	"REV":                                  gmeowPrefix + "vcardRevision",
	"ROLE":                                 schemaPrefix + "roleName",
	"SORT-STRING":                          gmeowPrefix + "vcardSortString",
	"SOUND":                                gmeowPrefix + "vcardSound",
	"SOURCE":                               gmeowPrefix + "vcardSource",
	"TEL":                                  gmeowPrefix + "vcardTelephone",
	"TITLE":                                gmeowPrefix + "vcardTitle",
	"TZ":                                   gmeowPrefix + "vcardTimeZone",
	"UID":                                  gmeowPrefix + "vcardUID",
	"URL":                                  gmeowPrefix + "vcardURL",
	"VERSION":                              gmeowPrefix + "vcardVersion",
	"X-XMPP":                               gmeowPrefix + "jabberIdentity",
	"YAHOO":                                gmeowPrefix + "yahooIdentity",
	"@FRIEND @TRACKED=0A=0AE-MAIL ADDRESS": gmeowPrefix + "legacyEncodedEmailLabelFragment",
	"@FRIEND @TRACKED=0A=0AIM":             gmeowPrefix + "legacyEncodedIMLabelFragment",
	"@WORK-BOATS=0A=0AE-MAIL ADDRESS":      gmeowPrefix + "legacyEncodedEmailLabelFragment",
	"@WORK-BOATS=0A=0AIM":                  gmeowPrefix + "legacyEncodedIMLabelFragment",
	"=0AIM":                                gmeowPrefix + "legacyEncodedIMFragment",
	"#K5631C2&AMP":                         gmeowPrefix + "legacyEncodedContinuationFragment",
	"ESS":                                  gmeowPrefix + "legacyEncodedContinuationFragment",
	"[REF]=0A[REF]=0ACATEGORIES":           gmeowPrefix + "legacyEncodedCategoryFragment",
	"X-ABADR":                              gmeowPrefix + "appleAddressBookAddress",
	"X-ABLABEL":                            gmeowPrefix + "appleAddressBookLabel",
	"X-ABRELATEDNAMES":                     gmeowPrefix + "appleAddressBookRelatedName",
	"X-ABSHOWAS":                           gmeowPrefix + "appleAddressBookShowAs",
	"X-ABUID":                              gmeowPrefix + "appleAddressBookUID",
	"X-AIM":                                gmeowPrefix + "aimIdentity",
	"X-CAT-IDS":                            gmeowPrefix + "contactCategoryIDs",
	"X-CID":                                gmeowPrefix + "contactSourceID",
	"X-CREATED":                            gmeowPrefix + "sourceCreatedAt",
	"X-ETAG":                               gmeowPrefix + "sourceETag",
	"X-FC-LIST-ID":                         gmeowPrefix + "fastmailContactListID",
	"X-FC-TAGS":                            gmeowPrefix + "fastmailTags",
	"X-GENDER":                             schemaPrefix + "gender",
	"X-GTALK":                              gmeowPrefix + "googleTalkIdentity",
	"X-ID":                                 gmeowPrefix + "sourceRecordID",
	"X-JABBER":                             gmeowPrefix + "jabberIdentity",
	"X-MYSPACE":                            gmeowPrefix + "myspaceProfile",
	"X-MODIFIED":                           gmeowPrefix + "sourceModifiedAt",
	"X-MSN":                                gmeowPrefix + "msnIdentity",
	"X-MAIDENNAME":                         gmeowPrefix + "maidenName",
	"X-PHONETIC-LAST-NAME":                 gmeowPrefix + "phoneticLastName",
	"X-PRIMARY-PHONE":                      gmeowPrefix + "primaryPhone",
	"X-SOCIALPROFILE":                      gmeowPrefix + "socialProfile",
	"X-WINDOWS-LIVE":                       gmeowPrefix + "msnIdentity",
	"X-YAHOO":                              gmeowPrefix + "yahooIdentity",
	"ADDRESS":                              schemaPrefix + "address",
	"BIRTHDAY":                             schemaPrefix + "birthDate",
	"BUSINESS STREET":                      gmeowPrefix + "legacyBusinessStreetFragment",
	"CITY":                                 gmeowPrefix + "sourceCity",
	"COUNTRY":                              gmeowPrefix + "sourceCountry",
	"IM":                                   vcardPrefix + "hasInstantMessage",
	"PIN":                                  gmeowPrefix + "pinIdentity",
	"STATE":                                gmeowPrefix + "sourceState",
	"WEB ID":                               gmeowPrefix + "legacyWebIDFragment",
	"X-ABDATE":                             gmeowPrefix + "appleAddressBookDate",
	"X-AIM-ID":                             gmeowPrefix + "aimIdentity",
	"X-EVOLUTION-FILE-AS":                  gmeowPrefix + "evolutionFileAs",
	"X-FC-CLUSTER-ID":                      gmeowPrefix + "fastmailClusterID",
	"X-FC-ENRICHED-ETAG":                   gmeowPrefix + "fastmailEnrichedETag",
	"X-FC-REMOTE-ACCOUNT":                  gmeowPrefix + "fastmailRemoteAccount",
	"X-ICQ":                                gmeowPrefix + "icqIdentity",
	"X-ICQ-ID":                             gmeowPrefix + "icqIdentity",
	"X-IM":                                 vcardPrefix + "hasInstantMessage",
	"X-MSN-ID":                             gmeowPrefix + "msnIdentity",
	"X-SKYPE":                              gmeowPrefix + "skypeIdentity",
	"X-SKYPE-ID":                           gmeowPrefix + "skypeIdentity",
	"X-YAHOO-ID":                           gmeowPrefix + "yahooIdentity",
}

func vcardSourcePropertyPredicate(name string) string {
	if predicate := legacyEncodedVCardFragmentPredicate(name); predicate != "" {
		return predicate
	}
	if strings.HasPrefix(name, "X-FC-GOOGLE-URI-") {
		return gmeowPrefix + "fastmailGoogleURI"
	}
	if strings.HasPrefix(name, "X-FC-STASH-GDATA") {
		return gmeowPrefix + "fastmailGDataStash"
	}
	if predicate := vcardSourcePropertyPredicates[name]; predicate != "" {
		return predicate
	}
	if strings.HasPrefix(name, "X-") {
		return vcardVendorExtensionPredicate(name)
	}

	return ""
}

// vcardVendorExtensionPredicate deterministically maps an X- vendor-extension
// property to a distinct, queryable gmeow predicate. X-<vendor>-<name> becomes
// gmeow:vendorExtension/<vendor>/<name>; a single-segment X-<name> becomes
// gmeow:vendorExtension/<name>. The IRI is percent-encoded by iri() on render.
func vcardVendorExtensionPredicate(name string) string {
	rest := strings.TrimPrefix(name, "X-")
	if vendor, local, found := strings.Cut(rest, "-"); found && local != "" {
		return gmeowPrefix + "vendorExtension/" + vendor + "/" + local
	}

	return gmeowPrefix + "vendorExtension/" + rest
}

func legacyEncodedVCardFragmentPredicate(name string) string {
	upper := strings.ToUpper(strings.TrimSpace(name))
	switch {
	case strings.Contains(upper, "=0A") && strings.Contains(upper, "E-MAIL ADDRESS"):
		return gmeowPrefix + "legacyEncodedEmailLabelFragment"
	case strings.Contains(upper, "=0A") && strings.Contains(upper, "CATEGORIES"):
		return gmeowPrefix + "legacyEncodedCategoryFragment"
	case strings.Contains(upper, "=0AIM"):
		return gmeowPrefix + "legacyEncodedIMLabelFragment"
	case strings.Contains(upper, "=0A"),
		strings.Contains(upper, "&AMP"),
		upper == "",
		upper == "ESS",
		upper == "ITY",
		upper == "MP",
		upper == "OUNTRY",
		upper == "RESS":
		return gmeowPrefix + "legacyEncodedContinuationFragment"
	default:
		return ""
	}
}

var vcardParamPredicates = map[string]string{
	"BASE64":              gmeowPrefix + "vcardBase64Parameter",
	"CHARSET":             gmeowPrefix + "vcardCharsetParameter",
	"ENCODING":            gmeowPrefix + "vcardEncodingParameter",
	"GEO":                 gmeowPrefix + "vcardGeoParameter",
	"INDEX":               gmeowPrefix + "vcardIndexParameter",
	"LANGUAGE":            gmeowPrefix + "vcardLanguageParameter",
	"MEDIATYPE":           gmeowPrefix + "vcardMediaTypeParameter",
	"PID":                 gmeowPrefix + "vcardPIDParameter",
	"PREF":                gmeowPrefix + "vcardPreferenceParameter",
	"TYPE":                gmeowPrefix + "vcardTypeParameter",
	"VALUE":               gmeowPrefix + "vcardValueParameter",
	"X-SERVICE-TYPE":      gmeowPrefix + "vcardServiceTypeParameter",
	"N":                   gmeowPrefix + "vcardNameParameter",
	"X-APPLE-OMIT-YEAR":   gmeowPrefix + "appleOmitYearParameter",
	"X-DISPLAYNAME":       gmeowPrefix + "displayNameParameter",
	"X-EVOLUTION-UI-SLOT": gmeowPrefix + "evolutionUISlotParameter",
	"X-USERID":            gmeowPrefix + "userIDParameter",
}

var providerLifecycleByPredicate = map[string]providerLifecycle{
	gmeowPrefix + "aimIdentity": {
		ValidUntil: "2017-12-15",
		Confidence: "high",
		SourceURL:  "https://www.osnews.com/story/30129/aim-will-be-discontinued-on-december-15-2017/",
		Caveat:     "AIM instant messaging endpoint only; does not apply to AOL Mail, AOL accounts, or @aim.com email.",
	},
	gmeowPrefix + "googleTalkIdentity": {
		ValidUntil: "2017-06-26",
		Confidence: "high",
		SourceURL:  "https://workspaceupdates.googleblog.com/2017/03/updates-in-g-suite-to-streamline-hangouts-and-gmail.html",
		Caveat:     "Google Talk UI/XMPP federation endpoint only; does not apply to Google accounts, Gmail, Hangouts, Chat, or email.",
	},
	gmeowPrefix + "icqIdentity": {
		ValidUntil: "2024-06-26",
		Confidence: "high",
		SourceURL:  "https://www.tomshardware.com/software/applications/90s-instant-messaging-service-shuts-down-after-28-years-icq-will-stop-working-from-june-26",
		Caveat:     "ICQ messaging endpoint only.",
	},
	gmeowPrefix + "msnIdentity": {
		ValidUntil: "2013-04-30",
		Confidence: "medium",
		SourceURL:  "https://learn.microsoft.com/en-us/previous-versions/office/lync-server-2013/lync-server-2013-support-for-public-instant-messenger-connectivity",
		Caveat:     "MSN/Windows Live Messenger endpoint only; does not apply to Microsoft accounts, Hotmail/Outlook email, Skype-migrated identities, or regional exceptions.",
	},
	gmeowPrefix + "skypeIdentity": {
		ValidUntil: "2025-05-05",
		Confidence: "high",
		SourceURL:  "https://support.microsoft.com/en-us/skype/faq-and-known-issues-with-skype-d1ebea21-a059-48a5-8da7-0c0ea98ebf20",
		Caveat:     "Skype service endpoint only; does not prove the person, Microsoft account, or Teams continuity ended.",
	},
	gmeowPrefix + "yahooIdentity": {
		ValidUntil: "2018-07-17",
		Confidence: "high",
		SourceURL:  "https://tech.yahoo.com/article/2018-06-08-yahoo-messenger-discontinued-july-17th.html",
		Caveat:     "Yahoo Messenger endpoint only; does not apply to Yahoo ID, Yahoo Mail, or other Yahoo services.",
	},
}

// vcardIsAgentDenoting reports whether a vCard asserts an agent (person or
// organization) rather than a bare locator. A card is agent-denoting if it has a
// formatted name (FN), a structured name (N) with any non-empty component, an
// organization (ORG), or an explicit KIND. Cards carrying only contact points
// (EMAIL/TEL/URL/ADR/IMPP) are locator-only and do not create agents.
func vcardIsAgentDenoting(values map[string][]string) bool {
	if firstNonEmpty(values["FN"]...) != "" {
		return true
	}
	for _, n := range values["N"] {
		if strings.TrimSpace(strings.ReplaceAll(n, ";", "")) != "" {
			return true
		}
	}
	if firstNonEmpty(values["ORG"]...) != "" {
		return true
	}

	return firstNonEmpty(values["KIND"]...) != ""
}

// contactSubjectForVCard derives a stable OBSERVATION identity for a card — the
// hash of its meaningful claims, excluding volatile per-export/per-snapshot noise
// (UID, REV, VERSION, timestamps, ETags, binary photos). It deliberately does NOT
// key on email or name: those are temporal, multi-valued, and transferable (a
// person changes email/name; a role address transfers between people), so keying
// by them both fragments one entity and merges distinct ones. Identical card
// content across snapshots collapses to one observation; same-entity resolution
// across differing observations is the downstream resolver's job (not the import's).
func contactSubjectForVCard(values map[string][]string) string {
	if identity := vcardObservationIdentity(values); identity != "" {
		return identity
	}
	// All-volatile / empty card: fall back to the full content fingerprint so the
	// observation is still stable per exact content.
	return "urn:gmeow:observation:" + shortHash([]byte(stableVCardFingerprint(values)))
}

// volatileVCardProperties are excluded from the observation fingerprint because
// they change across exports/snapshots without changing what the card asserts.
var volatileVCardProperties = map[string]bool{
	"UID": true, "REV": true, "VERSION": true, "PRODID": true,
	"PHOTO": true, "LOGO": true, "SOUND": true, "KEY": true, "CLIENTPIDMAP": true,
	"X-ABUID": true, "X-CREATED": true, "X-MODIFIED": true, "X-ETAG": true,
	"X-CID": true, "X-ID": true, "X-CAT-IDS": true, "X-ABSHOWAS": true,
	"X-FC-ENRICHED-ETAG": true, "X-FC-CLUSTER-ID": true, "X-FC-REMOTE-ACCOUNT": true,
	"X-FC-LIST-ID": true,
}

func isVolatileVCardProperty(name string) bool {
	upper := strings.ToUpper(strings.TrimSpace(name))
	if volatileVCardProperties[upper] {
		return true
	}

	return strings.HasPrefix(upper, "X-FC-GOOGLE-URI-") ||
		strings.HasPrefix(upper, "X-FC-STASH-GDATA")
}

// vcardObservationIdentity hashes the card's meaningful (non-volatile) claims,
// canonicalized (sorted properties, normalized + sorted + deduped values) so the
// same content yields the same id regardless of property/value order or
// formatting. Returns "" when no meaningful content remains.
func vcardObservationIdentity(values map[string][]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if isVolatileVCardProperty(key) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		normalized := make([]string, 0, len(values[key]))
		for _, value := range values[key] {
			if folded := normalizeFingerprintValue(value); folded != "" {
				normalized = append(normalized, folded)
			}
		}
		if len(normalized) == 0 {
			continue
		}
		normalized = uniqueStrings(normalized)
		sort.Strings(normalized)
		parts = append(parts, strings.ToUpper(key)+"="+strings.Join(normalized, "|"))
	}

	if len(parts) == 0 {
		return ""
	}

	return "urn:gmeow:observation:" + shortHash([]byte(strings.Join(parts, ";")))
}

func normalizeFingerprintValue(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func stableVCardFingerprint(values map[string][]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strings.Join(values[key], "|"))
	}

	return strings.Join(parts, ";")
}

func unfoldVCardLines(content string) []string {
	rawLines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	lines := []string{}
	quotedPrintableContinuation := false

	for _, raw := range rawLines {
		if strings.HasPrefix(raw, " ") || strings.HasPrefix(raw, "\t") {
			if len(lines) > 0 {
				lines[len(lines)-1] += raw[1:]
			}

			continue
		}
		if len(lines) > 0 && quotedPrintableContinuation {
			lines[len(lines)-1] = strings.TrimSuffix(lines[len(lines)-1], "=") + raw
			quotedPrintableContinuation = strings.HasSuffix(lines[len(lines)-1], "=")
			continue
		}

		if strings.TrimSpace(raw) != "" {
			lines = append(lines, strings.TrimRight(raw, "\r"))
			quotedPrintableContinuation = isQuotedPrintableVCardLine(lines[len(lines)-1]) &&
				strings.HasSuffix(lines[len(lines)-1], "=")
		}
	}

	return lines
}

func isQuotedPrintableVCardLine(line string) bool {
	head, _, found := strings.Cut(line, ":")
	if !found {
		return false
	}

	return strings.Contains(strings.ToUpper(head), "ENCODING=QUOTED-PRINTABLE")
}

func decodeVCardValue(value string, params map[string][]string) string {
	if hasVCardParamValue(params, "ENCODING", "QUOTED-PRINTABLE") {
		decoded, err := io.ReadAll(quotedprintable.NewReader(strings.NewReader(value)))
		if err == nil {
			value = string(decoded)
		}
	}

	return unescapeVCardValue(value)
}

func hasVCardParamValue(params map[string][]string, key, value string) bool {
	for _, item := range params[key] {
		if strings.EqualFold(strings.TrimSpace(item), value) {
			return true
		}
	}

	return false
}

func unescapeVCardValue(value string) string {
	replacer := strings.NewReplacer(
		`\n`,
		"\n",
		`\N`,
		"\n",
		`\,`,
		",",
		`\;`,
		";",
		`\\`,
		`\`,
	)

	return strings.TrimSpace(replacer.Replace(value))
}

func parseVCardLineHead(value string) (vcardLine, error) {
	parts := strings.Split(value, ";")
	name := vcardPropertyName(parts[0])
	params := map[string][]string{}
	for _, rawParam := range parts[1:] {
		rawParam = strings.TrimSpace(rawParam)
		if rawParam == "" {
			continue
		}
		key, rawValue, found := strings.Cut(rawParam, "=")
		if !found {
			key = "TYPE"
			rawValue = rawParam
		}
		key = strings.ToUpper(strings.TrimSpace(key))
		if vcardParamPredicates[key] == "" {
			return vcardLine{}, fmt.Errorf("unsupported vCard parameter %q", key)
		}
		for _, item := range strings.Split(rawValue, ",") {
			item = strings.TrimSpace(item)
			if item != "" {
				params[key] = append(params[key], item)
			}
		}
	}

	return vcardLine{name: name, params: params}, nil
}

func vcardPropertyName(value string) string {
	property := strings.Split(value, ";")[0]
	if _, after, found := strings.Cut(property, "."); found {
		property = after
	}

	return strings.ToUpper(strings.TrimSpace(property))
}
