// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
)

func insertMailIdentityRows(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
) error {
	metadata, ok := mailIdentityFacetMetadata(manifest)
	if !ok {
		return nil
	}
	messageID := strings.TrimSpace(stringFromAny(metadata["rfc_message_id"]))
	if messageID == "" {
		return nil
	}
	generated := boolFromAny(metadata["generated_message_id"])
	collision := boolFromAny(metadata["message_id_collision"])
	variant := manifestHasFacetKind(manifest, contracts.MailVariantFacetKind)
	maxScale := stringFromAny(metadata["max_scale"])
	versionCount := intFromAny(metadata["version_count"])
	for _, provenance := range manifest.Provenance {
		if provenance.SourceKind == contracts.MailIdentitySourceKind {
			continue
		}
		if provenance.SourceKind == "" || provenance.SourceName == "" {
			continue
		}
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO query_mail_identities(
			   message_id, object_digest, source_kind, source_name, generated, collision, variant,
			   max_scale, version_count
			 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
			 ON CONFLICT(message_id, object_digest, source_kind, source_name) DO UPDATE SET
			   generated = excluded.generated,
			   collision = excluded.collision,
			   variant = excluded.variant,
			   max_scale = excluded.max_scale,
			   version_count = excluded.version_count`,
			messageID,
			manifest.ObjectDigest,
			provenance.SourceKind,
			provenance.SourceName,
			generated,
			collision,
			variant,
			maxScale,
			versionCount,
		); err != nil {
			return fmt.Errorf("insert mail identity projection: %w", err)
		}
	}

	return nil
}

func mailIdentityFacetMetadata(manifest contracts.Manifest) (map[string]any, bool) {
	if metadata, ok := mailMessageFacetMetadata(manifest); ok {
		return metadata, true
	}
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == contracts.MailArchiveMembershipFacetKind {
			return firstMap(facet.Metadata, facet.Attributes), true
		}
	}

	return nil, false
}

func (index *Index) MailArchiveMissingGmail(
	ctx context.Context,
	request contracts.MailIdentityReportRequest,
) (contracts.MailIdentityReportResponse, error) {
	limit := normalizedLimit(request.Limit)
	offset := request.Offset
	if offset < 0 {
		offset = 0
	}

	args := []any{contracts.MailArchiveSourceKind, "gmail"}
	where := []string{
		"a.source_kind = $1",
		"NOT EXISTS (SELECT 1 FROM query_mail_identities g WHERE g.message_id = a.message_id AND g.source_kind = $2)",
	}
	if len(request.SourceNames) > 0 {
		args = append(args, request.SourceNames)
		where = append(where, fmt.Sprintf("a.source_name = ANY($%d)", len(args)))
	}
	if !request.IncludeGenerated {
		where = append(where, "a.generated = false")
	}
	if request.CollisionsOnly {
		where = append(where, "a.collision = true")
	}
	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(`
		SELECT a.message_id,
		       min(a.object_digest) AS canonical_digest,
		       array_agg(DISTINCT a.source_name ORDER BY a.source_name) AS source_names,
		       array_agg(DISTINCT a.object_digest ORDER BY a.object_digest)
		         FILTER (WHERE a.variant = false) AS archive_digests,
		       array_agg(DISTINCT a.object_digest ORDER BY a.object_digest)
		         FILTER (WHERE a.variant = true) AS variant_digests,
		       bool_or(a.generated) AS generated,
		       bool_or(a.collision) AS collision,
		       CASE max(CASE a.max_scale
		         WHEN 'major' THEN 2
		         WHEN 'minor' THEN 1
		         WHEN 'trivial' THEN 0
		         ELSE -1
		       END)
		         WHEN 2 THEN 'major'
		         WHEN 1 THEN 'minor'
		         WHEN 0 THEN 'trivial'
		         ELSE ''
		       END AS max_scale,
		       max(a.version_count) AS version_count,
		       count(*) OVER() AS total
		  FROM query_mail_identities a
		 WHERE %s
		 GROUP BY a.message_id
		 ORDER BY a.message_id
		 LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.MailIdentityReportResponse{}, fmt.Errorf(
			"query missing Gmail mail identities: %w",
			err,
		)
	}
	defer rows.Close()

	items := []contracts.MailIdentityReportItem{}
	total := 0
	for rows.Next() {
		var item contracts.MailIdentityReportItem
		var archiveDigests []string
		var variantDigests []string
		if err := rows.Scan(
			&item.MessageID,
			&item.CanonicalDigest,
			&item.SourceNames,
			&archiveDigests,
			&variantDigests,
			&item.Generated,
			&item.Collision,
			&item.MaxScale,
			&item.VersionCount,
			&total,
		); err != nil {
			return contracts.MailIdentityReportResponse{}, fmt.Errorf(
				"scan missing Gmail mail identity: %w",
				err,
			)
		}
		item.ArchiveDigests = objectDigestSlice(archiveDigests)
		item.VariantDigests = objectDigestSlice(variantDigests)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return contracts.MailIdentityReportResponse{}, fmt.Errorf(
			"iterate missing Gmail mail identities: %w",
			err,
		)
	}

	return contracts.MailIdentityReportResponse{
		Items:  items,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}

func (index *Index) ResolveMailIdentity(
	ctx context.Context,
	request contracts.MailIdentityResolveRequest,
) (contracts.MailIdentityResolveResponse, error) {
	messageID := strings.TrimSpace(request.MessageID)
	if messageID == "" {
		return contracts.MailIdentityResolveResponse{}, fmt.Errorf("message_id is required")
	}
	limit := normalizedLimit(request.Limit)
	rows, err := index.pool.Query(
		ctx,
		`SELECT object_digest, count(*) OVER() AS total
		   FROM (
		     SELECT DISTINCT object_digest
		       FROM query_mail_identities
		      WHERE message_id = $1
		      ORDER BY object_digest
		   ) matches
		  LIMIT $2`,
		messageID,
		limit,
	)
	if err != nil {
		return contracts.MailIdentityResolveResponse{}, fmt.Errorf(
			"resolve mail identity: %w",
			err,
		)
	}
	defer rows.Close()

	digests := []contracts.ObjectDigest{}
	total := 0
	for rows.Next() {
		var digest contracts.ObjectDigest
		if err := rows.Scan(&digest, &total); err != nil {
			return contracts.MailIdentityResolveResponse{}, fmt.Errorf(
				"scan mail identity resolver: %w",
				err,
			)
		}
		digests = append(digests, digest)
	}
	if err := rows.Err(); err != nil {
		return contracts.MailIdentityResolveResponse{}, fmt.Errorf(
			"iterate mail identity resolver: %w",
			err,
		)
	}

	return contracts.MailIdentityResolveResponse{
		MessageID: messageID,
		Digests:   digests,
		Total:     total,
		Limit:     limit,
	}, nil
}

func objectDigestSlice(values []string) []contracts.ObjectDigest {
	result := make([]contracts.ObjectDigest, 0, len(values))
	for _, value := range values {
		if value != "" {
			result = append(result, contracts.ObjectDigest(value))
		}
	}

	return result
}

func boolFromAny(value any) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}

	return false
}

func manifestHasFacetKind(manifest contracts.Manifest, kind string) bool {
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == kind {
			return true
		}
	}

	return false
}
