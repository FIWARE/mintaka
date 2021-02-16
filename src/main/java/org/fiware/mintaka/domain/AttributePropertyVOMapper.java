package org.fiware.mintaka.domain;

import org.fiware.mintaka.persistence.AbstractAttribute;
import org.fiware.mintaka.persistence.ValueType;
import org.fiware.ngsi.model.*;
import org.geojson.LngLatAlt;
import org.mapstruct.Mapper;

import java.net.URI;
import java.util.Date;
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
	default boolean isRelationShip(AbstractAttribute attribute) {
		return attribute.getValueType() == ValueType.Relationship;
	}

	/**
	 * Map the internal attribute to a relationship
	 * @param attribute attribute to map
	 * @param createdAt timestamp the relationship was created at
	 * @return the mapped relationship
	 */
	default RelationshipVO attributeToRelationShip(AbstractAttribute attribute, Date createdAt) {
		if (!isRelationShip(attribute)) {
			throw new IllegalArgumentException("Received attribute is not a relationship");
		}
		RelationshipVO relationshipVO = new RelationshipVO()
				.instanceId(URI.create(attribute.getInstanceId()))
				.type(RelationshipVO.Type.RELATIONSHIP)
				._object(URI.create(attribute.getText()))
				.createdAt(createdAt)
				.modifiedAt(Date.from(attribute.getTs()));

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> relationshipVO.observedAt(Date.from(oa)));
		Optional.ofNullable(attribute.getDatasetId()).ifPresent(di -> relationshipVO.datasetId(URI.create(di)));

		return relationshipVO;
	}

	/**
	 * Map the internal attribute to a geoProperty
	 * @param attribute attribute to map
	 * @param createdAt timestamp the geoProperty was created at
	 * @return the mapped geoProperty
	 */
	default GeoPropertyVO attributeToGeoProperty(AbstractAttribute attribute, Date createdAt) {
		if (!isGeoProperty(attribute)) {
			throw new IllegalArgumentException("Received attribute is not a geoproperty.");
		}
		GeoPropertyVO geoPropertyVO = new GeoPropertyVO()
				.instanceId(URI.create(attribute.getInstanceId()))
				.type(GeoPropertyVO.Type.GEOPROPERTY)
				.createdAt(createdAt)
				.modifiedAt(Date.from(attribute.getTs()));

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> geoPropertyVO.observedAt(Date.from(oa)));
		Optional.ofNullable(attribute.getDatasetId()).ifPresent(di -> geoPropertyVO.datasetId(URI.create(di)));

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
	 * @param attribute attribute to map
	 * @param createdAt timestamp the property was created at
	 * @return the mapped property
	 */
	default PropertyVO attributeToPropertyVO(AbstractAttribute attribute, Date createdAt) {
		if (isGeoProperty(attribute)) {
			throw new IllegalArgumentException("Received a geoproperty.");
		}
		if (isRelationShip(attribute)) {
			throw new IllegalArgumentException("Received a relationship.");
		}
		PropertyVO propertyVO = new PropertyVO()
				.instanceId(URI.create(attribute.getInstanceId()))
				.type(PropertyVO.Type.PROPERTY)
				.createdAt(createdAt)
				.modifiedAt(Date.from(attribute.getTs()));

		Optional.ofNullable(attribute.getObservedAt()).ifPresent(oa -> propertyVO.observedAt(Date.from(oa)));
		Optional.ofNullable(attribute.getDatasetId()).ifPresent(di -> propertyVO.datasetId(URI.create(di)));

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
				propertyVO.value(Date.from(attribute.getDatetime()));
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
