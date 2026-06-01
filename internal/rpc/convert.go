// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"encoding/json"
	"fmt"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	pb "blackcat.ca/gmeow/internal/rpc/gen/gmeow/v1"
)

func ToPBSourceObjectRef(ref contracts.SourceObjectRef) *pb.SourceObjectRef {
	return &pb.SourceObjectRef{
		SourceKind:      ref.SourceKind,
		SourceName:      ref.SourceName,
		ExternalId:      ref.ExternalID,
		ExternalVersion: ref.ExternalVersion,
	}
}

func EncodeMapForSource(value map[string]any) ([]byte, error) {
	return encodeMap(value)
}

func DecodeMapForSource(value []byte) (map[string]any, error) {
	return decodeMap(value)
}

func FromPBSourceObjectRef(ref *pb.SourceObjectRef) contracts.SourceObjectRef {
	if ref == nil {
		return contracts.SourceObjectRef{}
	}

	return contracts.SourceObjectRef{
		SourceKind:      ref.GetSourceKind(),
		SourceName:      ref.GetSourceName(),
		ExternalID:      ref.GetExternalId(),
		ExternalVersion: ref.GetExternalVersion(),
	}
}

func ToPBSourceIngestClaim(claim contracts.SourceIngestClaim) *pb.SourceIngestClaim {
	return &pb.SourceIngestClaim{
		SourceObject: ToPBSourceObjectRef(claim.SourceObject),
		ClaimId:      claim.ClaimID,
		AcquiredAt:   formatTime(claim.AcquiredAt),
	}
}

func FromPBSourceIngestClaim(
	claim *pb.SourceIngestClaim,
) (contracts.SourceIngestClaim, error) {
	if claim == nil {
		return contracts.SourceIngestClaim{}, nil
	}

	acquiredAt, err := parseTime(claim.GetAcquiredAt())
	if err != nil {
		return contracts.SourceIngestClaim{}, fmt.Errorf("parse acquired_at: %w", err)
	}

	return contracts.SourceIngestClaim{
		SourceObject: FromPBSourceObjectRef(claim.GetSourceObject()),
		ClaimID:      claim.GetClaimId(),
		AcquiredAt:   acquiredAt,
	}, nil
}

func ToPBManifest(manifest contracts.Manifest) (*pb.Manifest, error) {
	analysis, err := encodeMap(manifest.Analysis)
	if err != nil {
		return nil, err
	}

	overlays, err := encodeMap(manifest.Overlays)
	if err != nil {
		return nil, err
	}

	facets, err := ToPBFacets(manifest.Facets)
	if err != nil {
		return nil, err
	}

	provenance, err := ToPBProvenance(manifest.Provenance)
	if err != nil {
		return nil, err
	}

	relationships := ToPBRelationships(manifest.Relationships)

	parts, err := ToPBCompoundParts(manifest.Compound.Parts)
	if err != nil {
		return nil, err
	}

	graph, err := ToPBGraphFacts(manifest.Graph)
	if err != nil {
		return nil, err
	}

	return &pb.Manifest{
		SchemaVersion:    int32(manifest.SchemaVersion),
		Digest:           string(manifest.ObjectDigest),
		ObjectId:         manifest.ObjectID,
		IdentityStrategy: manifest.IdentityStrategy,
		MediaType:        manifest.MediaType,
		Size:             manifest.Size,
		Compression:      manifest.Compression,
		ContentRoles:     append([]string{}, manifest.ContentRoles...),
		Facets:           facets,
		Titles:           ToPBTitles(manifest.Titles),
		Timestamps:       ToPBTimestamps(manifest.Timestamps),
		Provenance:       provenance,
		Relationships:    relationships,
		Compound: &pb.Compound{
			IsCompound: manifest.Compound.IsCompound,
			Parts:      parts,
		},
		AnalysisJson: analysis,
		Graph:        graph,
		Keywords:     append([]string{}, manifest.Keywords...),
		Embeddings:   ToPBEmbeddingRefs(manifest.Embeddings),
		OverlaysJson: overlays,
		CreatedAt:    formatTime(manifest.CreatedAt),
		UpdatedAt:    formatTime(manifest.UpdatedAt),
	}, nil
}

func FromPBManifest(manifest *pb.Manifest) (contracts.Manifest, error) {
	if manifest == nil {
		return contracts.Manifest{}, nil
	}

	analysis, err := decodeMap(manifest.GetAnalysisJson())
	if err != nil {
		return contracts.Manifest{}, err
	}

	overlays, err := decodeMap(manifest.GetOverlaysJson())
	if err != nil {
		return contracts.Manifest{}, err
	}

	facets, err := FromPBFacets(manifest.GetFacets())
	if err != nil {
		return contracts.Manifest{}, err
	}

	provenance, err := FromPBProvenance(manifest.GetProvenance())
	if err != nil {
		return contracts.Manifest{}, err
	}

	parts, err := FromPBCompoundParts(manifest.GetCompound().GetParts())
	if err != nil {
		return contracts.Manifest{}, err
	}

	graph, err := FromPBGraphFacts(manifest.GetGraph())
	if err != nil {
		return contracts.Manifest{}, err
	}

	createdAt, err := parseTime(manifest.GetCreatedAt())
	if err != nil {
		return contracts.Manifest{}, fmt.Errorf("parse created_at: %w", err)
	}

	updatedAt, err := parseTime(manifest.GetUpdatedAt())
	if err != nil {
		return contracts.Manifest{}, fmt.Errorf("parse updated_at: %w", err)
	}

	return contracts.Manifest{
		SchemaVersion:    contracts.SchemaVersion(manifest.GetSchemaVersion()),
		ObjectDigest:     contracts.ObjectDigest(manifest.GetDigest()),
		ObjectID:         manifest.GetObjectId(),
		IdentityStrategy: manifest.GetIdentityStrategy(),
		MediaType:        manifest.GetMediaType(),
		Size:             manifest.GetSize(),
		Compression:      manifest.GetCompression(),
		ContentRoles:     append([]string{}, manifest.GetContentRoles()...),
		Facets:           facets,
		Titles:           FromPBTitles(manifest.GetTitles()),
		Timestamps:       FromPBTimestamps(manifest.GetTimestamps()),
		Provenance:       provenance,
		Relationships:    FromPBRelationships(manifest.GetRelationships()),
		Compound: contracts.Compound{
			IsCompound: manifest.GetCompound().GetIsCompound(),
			Parts:      parts,
		},
		Analysis:   analysis,
		Graph:      graph,
		Keywords:   append([]string{}, manifest.GetKeywords()...),
		Embeddings: FromPBEmbeddingRefs(manifest.GetEmbeddings()),
		Overlays:   overlays,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
}

func ToPBFacets(facets []contracts.Facet) ([]*pb.Facet, error) {
	out := make([]*pb.Facet, 0, len(facets))
	for _, facet := range facets {
		metadata, err := encodeMap(facet.Metadata)
		if err != nil {
			return nil, err
		}

		attributes, err := encodeMap(facet.Attributes)
		if err != nil {
			return nil, err
		}

		out = append(out, &pb.Facet{
			Kind:           facet.Kind,
			Name:           facet.Name,
			Version:        facet.Version,
			MetadataJson:   metadata,
			AttributesJson: attributes,
		})
	}

	return out, nil
}

func FromPBFacets(facets []*pb.Facet) ([]contracts.Facet, error) {
	out := make([]contracts.Facet, 0, len(facets))
	for _, facet := range facets {
		metadata, err := decodeMap(facet.GetMetadataJson())
		if err != nil {
			return nil, err
		}

		attributes, err := decodeMap(facet.GetAttributesJson())
		if err != nil {
			return nil, err
		}

		out = append(out, contracts.Facet{
			Kind:       facet.GetKind(),
			Name:       facet.GetName(),
			Version:    facet.GetVersion(),
			Metadata:   metadata,
			Attributes: attributes,
		})
	}

	return out, nil
}

func ToPBProvenance(provenance []contracts.Provenance) ([]*pb.Provenance, error) {
	out := make([]*pb.Provenance, 0, len(provenance))
	for _, item := range provenance {
		metadata, err := encodeMap(item.Metadata)
		if err != nil {
			return nil, err
		}

		attributes, err := encodeMap(item.Attributes)
		if err != nil {
			return nil, err
		}

		out = append(out, &pb.Provenance{
			SourceName:       item.SourceName,
			SourceKind:       item.SourceKind,
			ExternalId:       item.ExternalID,
			ExternalVersion:  item.ExternalVersion,
			ObservedAt:       formatTime(item.ObservedAt),
			CapabilitiesSeen: append([]string{}, item.CapabilitiesSeen...),
			MetadataJson:     metadata,
			AttributesJson:   attributes,
		})
	}

	return out, nil
}

func FromPBProvenance(provenance []*pb.Provenance) ([]contracts.Provenance, error) {
	out := make([]contracts.Provenance, 0, len(provenance))
	for _, item := range provenance {
		observedAt, err := parseTime(item.GetObservedAt())
		if err != nil {
			return nil, fmt.Errorf("parse provenance observed_at: %w", err)
		}

		metadata, err := decodeMap(item.GetMetadataJson())
		if err != nil {
			return nil, err
		}

		attributes, err := decodeMap(item.GetAttributesJson())
		if err != nil {
			return nil, err
		}

		out = append(out, contracts.Provenance{
			SourceName:       item.GetSourceName(),
			SourceKind:       item.GetSourceKind(),
			ExternalID:       item.GetExternalId(),
			ExternalVersion:  item.GetExternalVersion(),
			ObservedAt:       observedAt,
			CapabilitiesSeen: append([]string{}, item.GetCapabilitiesSeen()...),
			Metadata:         metadata,
			Attributes:       attributes,
		})
	}

	return out, nil
}

func ToPBRelationships(relationships []contracts.Relationship) []*pb.Relationship {
	out := make([]*pb.Relationship, 0, len(relationships))
	for _, relationship := range relationships {
		out = append(out, &pb.Relationship{
			Type:   relationship.Type,
			From:   string(relationship.From),
			To:     string(relationship.To),
			Role:   relationship.Role,
			Order:  int32(relationship.Order),
			Source: relationship.Source,
		})
	}

	return out
}

func FromPBRelationships(relationships []*pb.Relationship) []contracts.Relationship {
	out := make([]contracts.Relationship, 0, len(relationships))
	for _, relationship := range relationships {
		out = append(out, contracts.Relationship{
			Type:   relationship.GetType(),
			From:   contracts.ObjectDigest(relationship.GetFrom()),
			To:     contracts.ObjectDigest(relationship.GetTo()),
			Role:   relationship.GetRole(),
			Order:  int(relationship.GetOrder()),
			Source: relationship.GetSource(),
		})
	}

	return out
}

func ToPBCompoundParts(parts []contracts.CompoundPart) ([]*pb.CompoundPart, error) {
	out := make([]*pb.CompoundPart, 0, len(parts))
	for _, part := range parts {
		metadata, err := encodeMap(part.Metadata)
		if err != nil {
			return nil, err
		}

		out = append(out, &pb.CompoundPart{
			Digest:       string(part.Digest),
			Role:         part.Role,
			Order:        int32(part.Order),
			Required:     part.Required,
			MetadataJson: metadata,
		})
	}

	return out, nil
}

func FromPBCompoundParts(parts []*pb.CompoundPart) ([]contracts.CompoundPart, error) {
	out := make([]contracts.CompoundPart, 0, len(parts))
	for _, part := range parts {
		metadata, err := decodeMap(part.GetMetadataJson())
		if err != nil {
			return nil, err
		}

		out = append(out, contracts.CompoundPart{
			Digest:   contracts.ObjectDigest(part.GetDigest()),
			Role:     part.GetRole(),
			Order:    int(part.GetOrder()),
			Required: part.GetRequired(),
			Metadata: metadata,
		})
	}

	return out, nil
}

func ToPBAnnotation(annotation contracts.Annotation) (*pb.Annotation, error) {
	data, err := encodeMap(annotation.Data)
	if err != nil {
		return nil, err
	}

	return &pb.Annotation{
		SchemaVersion:   int32(annotation.SchemaVersion),
		ObjectDigest:    string(annotation.ObjectDigest),
		AnalyzerName:    annotation.AnalyzerName,
		AnalyzerVersion: annotation.AnalyzerVer,
		Kind:            annotation.Kind,
		GeneratedAt:     formatTime(annotation.GeneratedAt),
		DataJson:        data,
	}, nil
}

func FromPBAnnotation(annotation *pb.Annotation) (contracts.Annotation, error) {
	if annotation == nil {
		return contracts.Annotation{}, nil
	}

	generatedAt, err := parseTime(annotation.GetGeneratedAt())
	if err != nil {
		return contracts.Annotation{}, fmt.Errorf("parse generated_at: %w", err)
	}

	data, err := decodeMap(annotation.GetDataJson())
	if err != nil {
		return contracts.Annotation{}, err
	}

	return contracts.Annotation{
		SchemaVersion: contracts.SchemaVersion(annotation.GetSchemaVersion()),
		ObjectDigest:  contracts.ObjectDigest(annotation.GetObjectDigest()),
		AnalyzerName:  annotation.GetAnalyzerName(),
		AnalyzerVer:   annotation.GetAnalyzerVersion(),
		Kind:          annotation.GetKind(),
		GeneratedAt:   generatedAt,
		Data:          data,
	}, nil
}

func ToPBAnnotations(annotations []contracts.Annotation) ([]*pb.Annotation, error) {
	out := make([]*pb.Annotation, 0, len(annotations))
	for _, annotation := range annotations {
		converted, err := ToPBAnnotation(annotation)
		if err != nil {
			return nil, err
		}

		out = append(out, converted)
	}

	return out, nil
}

func FromPBAnnotations(annotations []*pb.Annotation) ([]contracts.Annotation, error) {
	out := make([]contracts.Annotation, 0, len(annotations))
	for _, annotation := range annotations {
		converted, err := FromPBAnnotation(annotation)
		if err != nil {
			return nil, err
		}

		out = append(out, converted)
	}

	return out, nil
}

func ToPBProjectionObject(
	object filestore.ProjectionObject,
) (*pb.ProjectionObject, error) {
	manifest, err := ToPBManifest(object.Manifest)
	if err != nil {
		return nil, err
	}

	annotations, err := ToPBAnnotations(object.Annotations)
	if err != nil {
		return nil, err
	}

	findings := make([]*pb.ProjectionFinding, 0, len(object.Findings))
	for _, finding := range object.Findings {
		findings = append(findings, &pb.ProjectionFinding{
			Digest:  string(finding.Digest),
			Path:    finding.Path,
			Code:    finding.Code,
			Message: finding.Message,
		})
	}

	return &pb.ProjectionObject{
		Digest:      string(object.Digest),
		Path:        object.Path,
		Manifest:    manifest,
		Annotations: annotations,
		Findings:    findings,
	}, nil
}

func FromPBProjectionObject(
	object *pb.ProjectionObject,
) (filestore.ProjectionObject, error) {
	if object == nil {
		return filestore.ProjectionObject{}, nil
	}

	manifest, err := FromPBManifest(object.GetManifest())
	if err != nil {
		return filestore.ProjectionObject{}, err
	}

	annotations, err := FromPBAnnotations(object.GetAnnotations())
	if err != nil {
		return filestore.ProjectionObject{}, err
	}

	findings := make([]filestore.ProjectionFinding, 0, len(object.GetFindings()))
	for _, finding := range object.GetFindings() {
		findings = append(findings, filestore.ProjectionFinding{
			Digest:  contracts.ObjectDigest(finding.GetDigest()),
			Path:    finding.GetPath(),
			Code:    finding.GetCode(),
			Message: finding.GetMessage(),
		})
	}

	return filestore.ProjectionObject{
		Digest:      contracts.ObjectDigest(object.GetDigest()),
		Path:        object.GetPath(),
		Manifest:    manifest,
		Annotations: annotations,
		Findings:    findings,
	}, nil
}

func ToPBStorageBreakdown(
	report filestore.StorageBreakdownReport,
) *pb.StorageBreakdownResponse {
	files := make([]*pb.StorageBreakdownFile, 0, len(report.Files))
	for _, file := range report.Files {
		files = append(files, &pb.StorageBreakdownFile{
			ObjectDigest:   string(file.ObjectDigest),
			Role:           file.Role,
			Path:           file.Path,
			LogicalBytes:   file.LogicalBytes,
			AllocatedBytes: file.AllocatedBytes,
			Estimated:      file.Estimated,
			DictId:         file.DictID,
			DictIds:        append([]string{}, file.DictIDs...),
			ChunkCount:     int32(file.ChunkCount),
			ReferencedBy:   string(file.ReferencedBy),
			CompoundRole:   file.CompoundRole,
			CompoundOrder:  int32(file.CompoundOrder),
			RecursivePart:  file.RecursivePart,
		})
	}

	return &pb.StorageBreakdownResponse{
		RootDigest:            string(report.RootDigest),
		Files:                 files,
		TotalAllocatedBytes:   report.TotalAllocatedBytes,
		TotalLogicalBytes:     report.TotalLogicalBytes,
		EstimatedAllocated:    report.EstimatedAllocated,
		FileCount:             int32(report.FileCount),
		ReferencedObjectCount: int32(report.ReferencedObjectCount),
		RecursiveParts:        report.RecursiveParts,
	}
}

func FromPBStorageBreakdown(
	response *pb.StorageBreakdownResponse,
) filestore.StorageBreakdownReport {
	files := make([]filestore.StorageBreakdownFile, 0, len(response.GetFiles()))
	for _, file := range response.GetFiles() {
		files = append(files, filestore.StorageBreakdownFile{
			ObjectDigest:   contracts.ObjectDigest(file.GetObjectDigest()),
			Role:           file.GetRole(),
			Path:           file.GetPath(),
			LogicalBytes:   file.GetLogicalBytes(),
			AllocatedBytes: file.GetAllocatedBytes(),
			Estimated:      file.GetEstimated(),
			DictID:         file.GetDictId(),
			DictIDs:        append([]string{}, file.GetDictIds()...),
			ChunkCount:     int(file.GetChunkCount()),
			ReferencedBy:   contracts.ObjectDigest(file.GetReferencedBy()),
			CompoundRole:   file.GetCompoundRole(),
			CompoundOrder:  int(file.GetCompoundOrder()),
			RecursivePart:  file.GetRecursivePart(),
		})
	}

	return filestore.StorageBreakdownReport{
		Files:                 files,
		RootDigest:            contracts.ObjectDigest(response.GetRootDigest()),
		TotalAllocatedBytes:   response.GetTotalAllocatedBytes(),
		TotalLogicalBytes:     response.GetTotalLogicalBytes(),
		EstimatedAllocated:    response.GetEstimatedAllocated(),
		FileCount:             int(response.GetFileCount()),
		ReferencedObjectCount: int(response.GetReferencedObjectCount()),
		RecursiveParts:        response.GetRecursiveParts(),
	}
}

func ToPBResolvePath(
	report filestore.PathResolveReport,
) (*pb.ResolvePathResponse, error) {
	records, err := toPBResolvePathRecords(report.Records)
	if err != nil {
		return nil, err
	}

	manifest, err := toPBResolvePathManifest(report.Manifest)
	if err != nil {
		return nil, err
	}

	response := &pb.ResolvePathResponse{
		Records:        records,
		Manifest:       manifest,
		Kind:           report.Kind,
		Role:           report.Role,
		InputPath:      report.InputPath,
		Path:           report.Path,
		PhysicalPath:   report.PhysicalPath,
		ObjectDigest:   string(report.ObjectDigest),
		LogicalBytes:   report.LogicalBytes,
		AllocatedBytes: report.AllocatedBytes,
		Estimated:      report.Estimated,
		RecordCount:    int32(report.RecordCount),
		RecordsLimit:   int32(report.RecordsLimit),
		Truncated:      report.Truncated,
	}
	if report.SourceObject != nil {
		response.SourceObject = ToPBSourceObjectRef(*report.SourceObject)
	}
	if report.SourceCursor != nil {
		cursor, cursorErr := ToPBSourceCursor(*report.SourceCursor)
		if cursorErr != nil {
			return nil, cursorErr
		}
		response.SourceCursor = cursor
	}
	if report.IngestClaim != nil {
		response.IngestClaim = ToPBSourceIngestClaim(*report.IngestClaim)
	}

	return response, nil
}

func toPBResolvePathRecords(
	records []filestore.PathResolveRecord,
) ([]*pb.ResolvePathRecord, error) {
	out := make([]*pb.ResolvePathRecord, 0, len(records))
	for _, record := range records {
		pbRecord := &pb.ResolvePathRecord{
			ObjectDigest: string(record.ObjectDigest),
			ChildDigest:  string(record.ChildDigest),
			UpdatedAt:    record.UpdatedAt,
		}
		if record.SourceObject != nil {
			pbRecord.SourceObject = ToPBSourceObjectRef(*record.SourceObject)
		}
		if record.Recovery != nil {
			pbRecord.Recovery = &pb.ResolvePathRecovery{
				Digest:             record.Recovery.Digest,
				ObjectId:           record.Recovery.ObjectID,
				IdentityStrategy:   record.Recovery.IdentityStrategy,
				MediaType:          record.Recovery.MediaType,
				UncompressedSize:   record.Recovery.UncompressedSize,
				CompressedSize:     record.Recovery.CompressedSize,
				UncompressedBlake3: record.Recovery.UncompressedBlake3,
				CompressedBlake3:   record.Recovery.CompressedBlake3,
				CreatedAt:          formatTime(record.Recovery.CreatedAt),
			}
		}
		for _, parent := range record.Parents {
			pbRecord.Parents = append(pbRecord.Parents, &pb.ResolvePathParent{
				ParentDigest: string(parent.ParentDigest),
				Role:         parent.Role,
				UpdatedAt:    formatTime(parent.UpdatedAt),
			})
		}
		out = append(out, pbRecord)
	}

	return out, nil
}

func toPBResolvePathManifest(
	manifest *filestore.PathResolveManifest,
) (*pb.ResolvePathManifest, error) {
	if manifest == nil {
		return nil, nil
	}

	provenance, err := ToPBProvenance(manifest.Provenance)
	if err != nil {
		return nil, err
	}

	parts, err := ToPBCompoundParts(manifest.Parts)
	if err != nil {
		return nil, err
	}

	return &pb.ResolvePathManifest{
		Facets:       manifest.Facets,
		Provenance:   provenance,
		Parts:        parts,
		ObjectDigest: string(manifest.ObjectDigest),
		ObjectId:     manifest.ObjectID,
		MediaType:    manifest.MediaType,
		Size:         manifest.Size,
		Compound:     manifest.Compound,
	}, nil
}

func FromPBResolvePath(
	response *pb.ResolvePathResponse,
) (filestore.PathResolveReport, error) {
	records, err := fromPBResolvePathRecords(response.GetRecords())
	if err != nil {
		return filestore.PathResolveReport{}, err
	}

	manifest, err := fromPBResolvePathManifest(response.GetManifest())
	if err != nil {
		return filestore.PathResolveReport{}, err
	}

	report := filestore.PathResolveReport{
		Records:        records,
		Manifest:       manifest,
		Kind:           response.GetKind(),
		Role:           response.GetRole(),
		InputPath:      response.GetInputPath(),
		Path:           response.GetPath(),
		PhysicalPath:   response.GetPhysicalPath(),
		ObjectDigest:   contracts.ObjectDigest(response.GetObjectDigest()),
		LogicalBytes:   response.GetLogicalBytes(),
		AllocatedBytes: response.GetAllocatedBytes(),
		Estimated:      response.GetEstimated(),
		RecordCount:    int(response.GetRecordCount()),
		RecordsLimit:   int(response.GetRecordsLimit()),
		Truncated:      response.GetTruncated(),
	}
	if ref := response.GetSourceObject(); ref != nil {
		sourceObject := FromPBSourceObjectRef(ref)
		report.SourceObject = &sourceObject
	}
	if response.GetSourceCursor() != nil {
		cursor, cursorErr := FromPBSourceCursor(response.GetSourceCursor())
		if cursorErr != nil {
			return filestore.PathResolveReport{}, cursorErr
		}
		report.SourceCursor = &cursor
	}
	if response.GetIngestClaim() != nil {
		claim, claimErr := FromPBSourceIngestClaim(response.GetIngestClaim())
		if claimErr != nil {
			return filestore.PathResolveReport{}, claimErr
		}
		report.IngestClaim = &claim
	}

	return report, nil
}

func fromPBResolvePathRecords(
	records []*pb.ResolvePathRecord,
) ([]filestore.PathResolveRecord, error) {
	out := make([]filestore.PathResolveRecord, 0, len(records))
	for _, record := range records {
		converted := filestore.PathResolveRecord{
			ObjectDigest: contracts.ObjectDigest(record.GetObjectDigest()),
			ChildDigest:  contracts.ObjectDigest(record.GetChildDigest()),
			UpdatedAt:    record.GetUpdatedAt(),
		}
		if ref := record.GetSourceObject(); ref != nil {
			sourceObject := FromPBSourceObjectRef(ref)
			converted.SourceObject = &sourceObject
		}
		if recovery := record.GetRecovery(); recovery != nil {
			createdAt, parseErr := parseTime(recovery.GetCreatedAt())
			if parseErr != nil {
				return nil, fmt.Errorf(
					"parse recovery created_at %q: %w",
					recovery.GetCreatedAt(),
					parseErr,
				)
			}
			converted.Recovery = &filestore.PathResolveRecovery{
				Digest:             recovery.GetDigest(),
				ObjectID:           recovery.GetObjectId(),
				IdentityStrategy:   recovery.GetIdentityStrategy(),
				MediaType:          recovery.GetMediaType(),
				UncompressedSize:   recovery.GetUncompressedSize(),
				CompressedSize:     recovery.GetCompressedSize(),
				UncompressedBlake3: recovery.GetUncompressedBlake3(),
				CompressedBlake3:   recovery.GetCompressedBlake3(),
				CreatedAt:          createdAt,
			}
		}
		for _, parent := range record.GetParents() {
			updatedAt, parseErr := parseTime(parent.GetUpdatedAt())
			if parseErr != nil {
				return nil, fmt.Errorf(
					"parse parent updated_at %q: %w",
					parent.GetUpdatedAt(),
					parseErr,
				)
			}
			converted.Parents = append(converted.Parents, filestore.PathResolveParent{
				ParentDigest: contracts.ObjectDigest(parent.GetParentDigest()),
				Role:         parent.GetRole(),
				UpdatedAt:    updatedAt,
			})
		}
		out = append(out, converted)
	}

	return out, nil
}

func fromPBResolvePathManifest(
	manifest *pb.ResolvePathManifest,
) (*filestore.PathResolveManifest, error) {
	if manifest == nil {
		return nil, nil
	}

	provenance, err := FromPBProvenance(manifest.GetProvenance())
	if err != nil {
		return nil, err
	}

	parts, err := FromPBCompoundParts(manifest.GetParts())
	if err != nil {
		return nil, err
	}

	return &filestore.PathResolveManifest{
		Facets:       manifest.GetFacets(),
		Provenance:   provenance,
		Parts:        parts,
		ObjectDigest: contracts.ObjectDigest(manifest.GetObjectDigest()),
		ObjectID:     manifest.GetObjectId(),
		MediaType:    manifest.GetMediaType(),
		Size:         manifest.GetSize(),
		Compound:     manifest.GetCompound(),
	}, nil
}

func ToPBStructure(structure contracts.Structure) (*pb.Structure, error) {
	roles := make([]*pb.StructureRoleParts, 0, len(structure.PartsByRole))
	for role, parts := range structure.PartsByRole {
		convertedParts := make([]*pb.StructurePart, 0, len(parts))
		for _, part := range parts {
			metadata, err := encodeMap(part.Metadata)
			if err != nil {
				return nil, err
			}

			convertedParts = append(convertedParts, &pb.StructurePart{
				Digest:       string(part.Digest),
				Role:         part.Role,
				Order:        int32(part.Order),
				Required:     part.Required,
				Facets:       append([]string{}, part.Facets...),
				MetadataJson: metadata,
			})
		}

		roles = append(roles, &pb.StructureRoleParts{Role: role, Parts: convertedParts})
	}

	return &pb.Structure{
		SchemaVersion: int32(structure.SchemaVersion),
		Digest:        string(structure.ObjectDigest),
		ObjectId:      structure.ObjectID,
		Facets:        append([]string{}, structure.Facets...),
		PartsByRole:   roles,
	}, nil
}

func FromPBStructure(structure *pb.Structure) (contracts.Structure, error) {
	if structure == nil {
		return contracts.Structure{}, nil
	}

	partsByRole := map[string][]contracts.StructurePart{}

	for _, roleParts := range structure.GetPartsByRole() {
		parts := make([]contracts.StructurePart, 0, len(roleParts.GetParts()))
		for _, part := range roleParts.GetParts() {
			metadata, err := decodeMap(part.GetMetadataJson())
			if err != nil {
				return contracts.Structure{}, err
			}

			parts = append(parts, contracts.StructurePart{
				Digest:   contracts.ObjectDigest(part.GetDigest()),
				Role:     part.GetRole(),
				Order:    int(part.GetOrder()),
				Required: part.GetRequired(),
				Facets:   append([]string{}, part.GetFacets()...),
				Metadata: metadata,
			})
		}

		partsByRole[roleParts.GetRole()] = parts
	}

	return contracts.Structure{
		SchemaVersion: contracts.SchemaVersion(structure.GetSchemaVersion()),
		ObjectDigest:  contracts.ObjectDigest(structure.GetDigest()),
		ObjectID:      structure.GetObjectId(),
		Facets:        append([]string{}, structure.GetFacets()...),
		PartsByRole:   partsByRole,
	}, nil
}

func ToPBSourceCursor(cursor contracts.SourceCursor) (*pb.SourceCursor, error) {
	cursorJSON, err := encodeMap(cursor.Cursor)
	if err != nil {
		return nil, err
	}

	return &pb.SourceCursor{
		SchemaVersion: int32(cursor.SchemaVersion),
		SourceKind:    cursor.SourceKind,
		SourceName:    cursor.SourceName,
		CursorJson:    cursorJSON,
		UpdatedAt:     formatTime(cursor.UpdatedAt),
	}, nil
}

func FromPBSourceCursor(cursor *pb.SourceCursor) (contracts.SourceCursor, error) {
	if cursor == nil {
		return contracts.SourceCursor{}, nil
	}

	cursorMap, err := decodeMap(cursor.GetCursorJson())
	if err != nil {
		return contracts.SourceCursor{}, err
	}

	updatedAt, err := parseTime(cursor.GetUpdatedAt())
	if err != nil {
		return contracts.SourceCursor{}, fmt.Errorf("parse source cursor updated_at: %w", err)
	}

	return contracts.SourceCursor{
		SchemaVersion: contracts.SchemaVersion(cursor.GetSchemaVersion()),
		SourceKind:    cursor.GetSourceKind(),
		SourceName:    cursor.GetSourceName(),
		Cursor:        cursorMap,
		UpdatedAt:     updatedAt,
	}, nil
}

func ToPBAnalyzerSpec(spec contracts.AnalyzerSpec) *pb.AnalyzerSpec {
	return &pb.AnalyzerSpec{
		Name:                  spec.Name,
		Version:               spec.Version,
		MediaTypes:            append([]string{}, spec.MediaTypes...),
		ContentRoles:          append([]string{}, spec.ContentRoles...),
		RequiredInputs:        append([]string{}, spec.RequiredInputs...),
		OutputSections:        append([]string{}, spec.OutputSections...),
		Dependencies:          append([]string{}, spec.Dependencies...),
		Priority:              int32(spec.Priority),
		IdempotencyKeyFormula: spec.IdempotencyFormula,
		Deterministic:         spec.Deterministic,
		WorkerKind:            spec.WorkerKind,
	}
}

func FromPBAnalyzerSpec(spec *pb.AnalyzerSpec) contracts.AnalyzerSpec {
	if spec == nil {
		return contracts.AnalyzerSpec{}
	}

	return contracts.AnalyzerSpec{
		Name:               spec.GetName(),
		Version:            spec.GetVersion(),
		MediaTypes:         append([]string{}, spec.GetMediaTypes()...),
		ContentRoles:       append([]string{}, spec.GetContentRoles()...),
		RequiredInputs:     append([]string{}, spec.GetRequiredInputs()...),
		OutputSections:     append([]string{}, spec.GetOutputSections()...),
		Dependencies:       append([]string{}, spec.GetDependencies()...),
		Priority:           int(spec.GetPriority()),
		IdempotencyFormula: spec.GetIdempotencyKeyFormula(),
		Deterministic:      spec.GetDeterministic(),
		WorkerKind:         spec.GetWorkerKind(),
	}
}

func ToPBAnalyzerJob(job contracts.AnalyzerJob) *pb.AnalyzerJob {
	return &pb.AnalyzerJob{
		SchemaVersion:  int32(job.SchemaVersion),
		JobId:          job.JobID,
		IdempotencyKey: job.IdempotencyKey,
		Analyzer:       ToPBAnalyzerSpec(job.Analyzer),
		ObjectDigest:   string(job.ObjectDigest),
		PriorityClass:  job.PriorityClass,
		RequestedBy:    job.RequestedBy,
		Reason:         job.Reason,
		Attempt:        int32(job.Attempt),
		TraceId:        job.TraceID,
		Deadline:       formatTime(job.Deadline),
		Forced:         job.Forced,
		Priority:       int32(job.Priority),
		CreatedAt:      formatTime(job.CreatedAt),
	}
}

func FromPBAnalyzerJob(job *pb.AnalyzerJob) (contracts.AnalyzerJob, error) {
	if job == nil {
		return contracts.AnalyzerJob{}, nil
	}

	deadline, err := parseTime(job.GetDeadline())
	if err != nil {
		return contracts.AnalyzerJob{}, fmt.Errorf("parse deadline: %w", err)
	}

	createdAt, err := parseTime(job.GetCreatedAt())
	if err != nil {
		return contracts.AnalyzerJob{}, fmt.Errorf("parse created_at: %w", err)
	}

	return contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersion(job.GetSchemaVersion()),
		JobID:          job.GetJobId(),
		IdempotencyKey: job.GetIdempotencyKey(),
		Analyzer:       FromPBAnalyzerSpec(job.GetAnalyzer()),
		ObjectDigest:   contracts.ObjectDigest(job.GetObjectDigest()),
		PriorityClass:  job.GetPriorityClass(),
		RequestedBy:    job.GetRequestedBy(),
		Reason:         job.GetReason(),
		Attempt:        int(job.GetAttempt()),
		TraceID:        job.GetTraceId(),
		Deadline:       deadline,
		Forced:         job.GetForced(),
		Priority:       int(job.GetPriority()),
		CreatedAt:      createdAt,
	}, nil
}

func ToPBTitles(titles []contracts.Title) []*pb.Title {
	out := make([]*pb.Title, 0, len(titles))
	for _, title := range titles {
		out = append(out, &pb.Title{
			Value:      title.Value,
			Source:     title.Source,
			Confidence: title.Confidence,
		})
	}

	return out
}

func FromPBTitles(titles []*pb.Title) []contracts.Title {
	out := make([]contracts.Title, 0, len(titles))
	for _, title := range titles {
		out = append(out, contracts.Title{
			Value:      title.GetValue(),
			Source:     title.GetSource(),
			Confidence: title.GetConfidence(),
		})
	}

	return out
}

func ToPBTimestamps(timestamps contracts.Timestamps) *pb.Timestamps {
	return &pb.Timestamps{
		Created:  formatTime(timestamps.Created),
		Modified: formatTime(timestamps.Modified),
		Observed: formatTime(timestamps.Observed),
	}
}

func FromPBTimestamps(timestamps *pb.Timestamps) contracts.Timestamps {
	if timestamps == nil {
		return contracts.Timestamps{}
	}

	return contracts.Timestamps{
		Created:  mustParseTime(timestamps.GetCreated()),
		Modified: mustParseTime(timestamps.GetModified()),
		Observed: mustParseTime(timestamps.GetObserved()),
	}
}

func ToPBGraphFacts(facts []contracts.GraphFact) ([]*pb.GraphFact, error) {
	out := make([]*pb.GraphFact, 0, len(facts))
	for _, fact := range facts {
		metadata, err := encodeMap(fact.Metadata)
		if err != nil {
			return nil, err
		}

		out = append(out, &pb.GraphFact{
			Subject:      fact.Subject,
			Predicate:    fact.Predicate,
			Object:       fact.Object,
			MetadataJson: metadata,
		})
	}

	return out, nil
}

func FromPBGraphFacts(facts []*pb.GraphFact) ([]contracts.GraphFact, error) {
	out := make([]contracts.GraphFact, 0, len(facts))
	for _, fact := range facts {
		metadata, err := decodeMap(fact.GetMetadataJson())
		if err != nil {
			return nil, err
		}

		out = append(out, contracts.GraphFact{
			Subject:   fact.GetSubject(),
			Predicate: fact.GetPredicate(),
			Object:    fact.GetObject(),
			Metadata:  metadata,
		})
	}

	return out, nil
}

func ToPBEmbeddingRefs(refs []contracts.EmbeddingRef) []*pb.EmbeddingRef {
	out := make([]*pb.EmbeddingRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, &pb.EmbeddingRef{
			Model:        ref.Model,
			ObjectDigest: string(ref.ObjectDigest),
			Dimensions:   int32(ref.Dimensions),
		})
	}

	return out
}

func FromPBEmbeddingRefs(refs []*pb.EmbeddingRef) []contracts.EmbeddingRef {
	out := make([]contracts.EmbeddingRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, contracts.EmbeddingRef{
			Model:        ref.GetModel(),
			ObjectDigest: contracts.ObjectDigest(ref.GetObjectDigest()),
			Dimensions:   int(ref.GetDimensions()),
		})
	}

	return out
}

func encodeMap(value map[string]any) ([]byte, error) {
	if len(value) == 0 {
		return nil, nil
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode dynamic metadata json: %w", err)
	}

	return encoded, nil
}

func decodeMap(value []byte) (map[string]any, error) {
	if len(value) == 0 {
		return nil, nil
	}

	var decoded map[string]any
	err := json.Unmarshal(value, &decoded)
	if err != nil {
		return nil, fmt.Errorf("decode dynamic metadata json: %w", err)
	}

	return decoded, nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339Nano, value)
}

func mustParseTime(value string) time.Time {
	parsed, _ := parseTime(value)

	return parsed
}
