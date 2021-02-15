package org.fiware.mintaka.domain;

import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.ValueType;
import org.fiware.ngsi.model.*;
import org.geojson.LngLatAlt;
import org.mapstruct.Mapper;

import java.net.URI;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Mapper(componentModel = "jsr330")
public interface AttributePropertyVOMapper {

	Set<ValueType> GEO_TYPES = Set.of(ValueType.GeoLineString, ValueType.GeoMultiLineString, ValueType.GeoMultiPolygon, ValueType.GeoPoint, ValueType.GeoPolygon);

	default boolean isGeoProperty(Attribute attribute) {
		return GEO_TYPES.contains(attribute.getValueType());
	}

	default boolean isRelationShip(Attribute attribute) {
		return attribute.getValueType() == ValueType.Relationship;
	}

	default RelationshipVO attributeToRelationShip(Attribute attribute, Date createdAt) {
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

	default GeoPropertyVO attributeToGeoProperty(Attribute attribute, Date createdAt) {
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

	default PropertyVO attributeToPropertyVO(Attribute attribute, Date createdAt) {
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
