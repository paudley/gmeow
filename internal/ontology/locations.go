// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

// The gmeow location vocabulary (canonical: ~/Active/gmeow-ontology — modules
// places.ttl + contacts.ttl; docs/location-mapping.md). GMEOW is the PRIMARY
// ontology: the importer emits these gmeow: terms first-class; the schema:/vcard:
// equivalents are aligned (owl:equivalentProperty / skos:closeMatch) so reasoners
// still see the standards. A surface gmeow:PostalAddress holds the as-written
// components; a resolved gmeow:Place chain (addressPlace -> containedInPlace*)
// carries coordinates and, later, gazetteer QIDs.
const (
	// Surface postal address (as-written components).
	PostalAddressClass = Gmeow + "PostalAddress"
	PostOfficeBox      = Gmeow + "postOfficeBox"
	ExtendedAddress    = Gmeow + "extendedAddress"
	StreetAddress      = Gmeow + "streetAddress"
	AddressLocality    = Gmeow + "addressLocality"
	AddressRegion      = Gmeow + "addressRegion"
	PostalCode         = Gmeow + "postalCode"
	CountryCode        = Gmeow + "countryCode"
	AddressPlace       = Gmeow + "addressPlace" // PostalAddress -> resolved Place

	// Resolved geographic place + coordinates.
	PlaceClass            = Gmeow + "Place"
	PlaceType             = Gmeow + "placeType"
	PlaceTypePremises     = Gmeow + "placeTypePremises"
	ContainedInPlace      = Gmeow + "containedInPlace"
	HasCoordinates        = Gmeow + "hasCoordinates"
	GeoCoordinatesClass   = Gmeow + "GeoCoordinates"
	Latitude              = Gmeow + "latitude"
	Longitude             = Gmeow + "longitude"
	Elevation             = Gmeow + "elevation"
	Timezone              = Gmeow + "timezone"
	AuthorityLink         = Gmeow + "authorityLink"
	LocatedAt             = Gmeow + "locatedAt"
	HasContactPointPlaces = Gmeow + "hasContactPoint"

	// Digital storage location (the structured form of a Source's location).
	StorageLocationClass       = Gmeow + "StorageLocation"
	StoredIn                   = Gmeow + "storedIn"
	StorageMedium              = Gmeow + "storageMedium"
	StorageMediumLocalFilesyst = Gmeow + "storageMediumLocalFilesystem"
	StorageMediumCloudService  = Gmeow + "storageMediumCloudService"
	StoragePath                = Gmeow + "storagePath"
	StorageService             = Gmeow + "storageService"
	PhysicalPlace              = Gmeow + "physicalPlace"
)
