package org.fiware.mintaka.domain;

import org.fiware.mintaka.persistence.AbstractAttribute;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.ValueType;
import org.fiware.ngsi.model.*;
import org.geojson.LngLatAlt;
import org.mapstruct.Mapper;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Map objects between the internal persistence domain and the api.
 */
@Mapper(componentModel = "jsr330")
public interface AttributePropertyVOMapper {

	// datasetId is included in the primary key and therefore cannot be null. In NGSI-LD, a datasetId is not required, thus orion sets it to "None" in case
	// no datasetId was defined.
	static String DATASET_ID_NONE = "None";

	// value types the denote a {@link GeoPropertyVO}
	Set<ValueType> GEO_TYPES = Set.of(ValueType.GeoLineString, ValueType.GeoMultiLineString, ValueType.GeoMultiPolygon, ValueType.GeoPoint, ValueType.GeoPolygon);

	/**
	 * Check if the given attribute from the persistence layer is a geoProperty
	 *
	 * @param attribute to check if its a geoproperty
	 * @return true if its a geoProperty
	 */
	default boolean isGeoProperty(AbstractAttribute attribute) {
		return GEO_TYPES.contains(attribute.getValueType());
	}

	/**
	 * Check if the given attribute from the persistence layer is a relationship
	 *
	 * @param attribute to check if its a relationship
	 * @return true if its a relationship
	 */
	default boolean isRelationship(AbstractAttribute attribute) {
		return attribute.getValueType() == ValueType.Relationship;
	}

	/**
	 * Map the internal attribute to a relationship
	 *
	 * @param attribute attribute to map
	 * @param createdAt timestamp the relationship was created at
	 * @return the mapped relationship
	 */
	default RelationshipVO attributeToRelationShip(AbstractAttribute attribute, Instant createdAt, boolean modifiedAt) {
		if (!isRelationship(attribute)) {
			throw new IllegalArgumentException("Received attribute is not a relationship");
		}
		RelationshipVO relationshipVO = new RelationshipVO()
				.type(RelationshipVO.Type.RELATIONSHIP)
				.instanceId(URI.create(attribute.getInstanceId()))
				._object(URI.create(attribute.getText()));

		if (modifiedAt) {
			relationshipVO.modifiedAt(attribute.getTs().toInstant(ZoneOffset.UTC));
		}

		if (createdAt != null) {
			relationshipVO.createdAt(createdAt);
		}

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> relationshipVO.observedAt(oa.toInstant(ZoneOffset.UTC)));
		if (attribute instanceof Attribute) {
			Optional.ofNullable(((Attribute) attribute).getDatasetId()).filter(id -> !id.equals(DATASET_ID_NONE)).ifPresent(di -> relationshipVO.datasetId(URI.create(di)));
		}

		return relationshipVO;
	}

	/**
	 * Map the internal attribute to a geoProperty
	 *
	 * @param attribute attribute to map
	 * @param createdAt timestamp the geoProperty was created at
	 * @return the mapped geoProperty
	 */
	default GeoPropertyVO attributeToGeoProperty(AbstractAttribute attribute, Instant createdAt, boolean modifiedAt) {
		if (!isGeoProperty(attribute)) {
			throw new IllegalArgumentException("Received attribute is not a geoproperty.");
		}
		GeoPropertyVO geoPropertyVO = new GeoPropertyVO()
				.instanceId(URI.create(attribute.getInstanceId()))
				.type(GeoPropertyVO.Type.GEOPROPERTY);

		if (attribute.getUnitCode() != null){
			geoPropertyVO.unitCode(attribute.getUnitCode());
		}
		if (modifiedAt) {
			geoPropertyVO.modifiedAt(attribute.getTs().toInstant(ZoneOffset.UTC));
		}

		if (createdAt != null) {
			geoPropertyVO.createdAt(createdAt);
		}

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> geoPropertyVO.observedAt(oa.toInstant(ZoneOffset.UTC)));
		if (attribute instanceof Attribute) {
			Optional.ofNullable(((Attribute) attribute).getDatasetId()).filter(id -> !id.equals(DATASET_ID_NONE)).ifPresent(di -> geoPropertyVO.datasetId(URI.create(di)));
		}
		switch (attribute.getValueType()) {
			case GeoPoint:
				geoPropertyVO
						.value(
								new PointVO()
										.type(PointVO.Type.POINT)
										.coordinates(lgnLatAltToPositionDefinition(attribute.getGeoPoint().getCoordinates())));

				break;
			case GeoPolygon:
				geoPropertyVO.value(
						new PolygonVO()
								.type(PolygonVO.Type.POLYGON)
								.coordinates(
										lgnLatAltListListToPolygonDefinition(attribute.getGeoPolygon().getCoordinates())));
				break;
			case GeoLineString:
				geoPropertyVO.value(
						new LineStringVO()
								.type(LineStringVO.Type.LINESTRING)
								.coordinates(
										lgnLatAltListToLineString(attribute.getGeoLineString().getCoordinates())));
				break;
			case GeoMultiPolygon:
				geoPropertyVO.value(
						new MultiPolygonVO()
								.type(MultiPolygonVO.Type.MULTIPOLYGON)
								.coordinates(
										attribute
												.getGeoMultiPolygon()
												.getCoordinates()
												.stream()
												.map(this::lgnLatAltListListToPolygonDefinition)
												.collect(Collectors.toList())));
				break;
			case GeoMultiLineString:
				geoPropertyVO.value(
						attribute
								.getGeoMultiLineString()
								.getCoordinates()
								.stream()
								.map(this::lgnLatAltListToLineString)
								.collect(Collectors.toList()));
				break;
			default:
				throw new IllegalArgumentException(String.format("Received an attribute with the unsupported type %s.", attribute.getValueType()));
		}
		return geoPropertyVO;
	}

	/**
	 * Map the internal attribute to a property
	 *
	 * @param attribute attribute to map
	 * @param createdAt timestamp the property was created at
	 * @return the mapped property
	 */
	default PropertyVO attributeToPropertyVO(AbstractAttribute attribute, Instant createdAt, boolean modifiedAt) {
		if (isGeoProperty(attribute)) {
			throw new IllegalArgumentException("Received a geoproperty.");
		}
		if (isRelationship(attribute)) {
			throw new IllegalArgumentException("Received a relationship.");
		}
		PropertyVO propertyVO = new PropertyVO()
				.instanceId(URI.create(attribute.getInstanceId()))
				.type(PropertyVO.Type.PROPERTY);

		if (attribute.getUnitCode() != null){
			propertyVO.unitCode(attribute.getUnitCode());
		}
		if (modifiedAt) {
			propertyVO.modifiedAt(attribute.getTs().toInstant(ZoneOffset.UTC));
		}

		if (createdAt != null) {
			propertyVO.createdAt(createdAt);
		}

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> propertyVO.observedAt(oa.toInstant(ZoneOffset.UTC)));
		if (attribute instanceof Attribute) {
			Optional.ofNullable(((Attribute) attribute).getDatasetId()).filter(id -> !id.equals(DATASET_ID_NONE)).ifPresent(di -> propertyVO.datasetId(URI.create(di)));
		}

		switch (attribute.getValueType()) {
			case Number:
				propertyVO.value(attribute.getNumber());
				break;
			case String:
				propertyVO.value(attribute.getText());
				break;
			case Boolean:
				propertyVO.value(attribute.getABoolean());
				break;
			case Compound:
				propertyVO.value(attribute.getCompound());
				break;
			case DateTime:
				propertyVO.value(attribute.getDatetime());
				break;
			case LanguageMap:
				throw new UnsupportedOperationException("Language maps are currently not supported.");
			default:
				throw new IllegalArgumentException(String.format("Received an attribute with the unsupported type %s.", attribute.getValueType()));
		}
		return propertyVO;
	}

	/**
	 * Map a {@link org.geojson.GeoJsonObject} LngLat list to a linearRingDefinition
	 *
	 * @param lngLatAltList geoJson to map
	 * @return the linearRingDefinition
	 */
	default LinearRingDefinitionVO lgnLatAltListToLinearRing(List<LngLatAlt> lngLatAltList) {
		return lngLatAltList.stream()
				.map(this::lgnLatAltToPositionDefinition)
				.collect(
						Collector.of(
								LinearRingDefinitionVO::new,
								LinearRingDefinitionVO::add,
								(l, r) -> {
									l.addAll(r);
									return r;
								}));
	}

	/**
	 * Map a {@link org.geojson.GeoJsonObject} LngLat list to a linearStringDefinition
	 *
	 * @param lngLatAltList geoJson to map
	 * @return the linearStringDefinition
	 */
	default LineStringDefinitionVO lgnLatAltListToLineString(List<LngLatAlt> lngLatAltList) {
		return lngLatAltList.stream()
				.map(this::lgnLatAltToPositionDefinition)
				.collect(
						Collector.of(
								LineStringDefinitionVO::new,
								LineStringDefinitionVO::add,
								(l, r) -> {
									l.addAll(r);
									return r;
								}));
	}

	/**
	 * Map a {@link org.geojson.GeoJsonObject} list of LngLat lists to a polygonDefinition
	 *
	 * @param lngLatAltListList geoJson to map
	 * @return the polygonDefinition
	 */
	default PolygonDefinitionVO lgnLatAltListListToPolygonDefinition(List<List<LngLatAlt>> lngLatAltListList) {
		return lngLatAltListList.stream()
				.map(this::lgnLatAltListToLinearRing)
				.collect(
						Collector.of(
								PolygonDefinitionVO::new,
								PolygonDefinitionVO::add,
								(l, r) -> {
									l.addAll(r);
									return l;
								}));
	}

	/**
	 * Map a {@link org.geojson.GeoJsonObject} lngLatAlt to a positionDefinition
	 *
	 * @param lngLatAlt geoJson to map
	 * @return the positionDefinition
	 */
	default PositionDefinitionVO lgnLatAltToPositionDefinition(LngLatAlt lngLatAlt) {
		PositionDefinitionVO positionDefinitionVO = new PositionDefinitionVO();
		positionDefinitionVO.add(lngLatAlt.getLongitude());
		positionDefinitionVO.add(lngLatAlt.getLatitude());
		if (!Double.isNaN(lngLatAlt.getAltitude())) {
			positionDefinitionVO.add(lngLatAlt.getAltitude());
		}
		return positionDefinitionVO;
	}

}
