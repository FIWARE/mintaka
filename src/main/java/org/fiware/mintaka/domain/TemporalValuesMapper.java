package org.fiware.mintaka.domain;

import org.fiware.mintaka.exception.JacksonConversionException;
import org.fiware.ngsi.model.EntityTemporalVO;
import org.fiware.ngsi.model.GeoPropertyVO;
import org.fiware.ngsi.model.PropertyVO;
import org.fiware.ngsi.model.RelationshipVO;
import org.mapstruct.Mapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Mapper(componentModel = "jsr330")
public interface TemporalValuesMapper {

	default TemporalValuesEntity entityTemporalToTemporalValuesEntity(EntityTemporalVO entityTemporalVO, TimeStampType timeStampType) {
		TemporalValuesEntity temporalValuesEntity = new TemporalValuesEntity();
		temporalValuesEntity.setAtContext(entityTemporalVO.getAtContext());
		temporalValuesEntity.setId(entityTemporalVO.getId());
		temporalValuesEntity.setType(entityTemporalVO.getType());

		if (entityTemporalVO.getLocation() != null && !entityTemporalVO.getLocation().isEmpty()) {
			temporalValuesEntity.setLocation(geoPropertyToTemporalValuesProperty(entityTemporalVO.getLocation(), timeStampType));
		}
		if (entityTemporalVO.getObservationSpace() != null && !entityTemporalVO.getObservationSpace().isEmpty()) {
			temporalValuesEntity.setObservationSpace(geoPropertyToTemporalValuesProperty(entityTemporalVO.getObservationSpace(), timeStampType));

		}
		if (entityTemporalVO.getOperationSpace() != null && !entityTemporalVO.getOperationSpace().isEmpty()) {
			temporalValuesEntity.setOperationSpace(geoPropertyToTemporalValuesProperty(entityTemporalVO.getOperationSpace(), timeStampType));
		}

		if (entityTemporalVO.getAdditionalProperties() != null && !entityTemporalVO.getAdditionalProperties().isEmpty()) {
			Map<String, AbstractTemporalValue> temporalValueMap = new HashMap<>();
			entityTemporalVO.getAdditionalProperties().entrySet().stream().forEach(entry -> {
				// we expect lists of instances
				if (entry.getValue() instanceof List) {
					getTemporalValueFromInstanceList((List) entry.getValue(), timeStampType).
							ifPresent(abstractTemporalValue -> temporalValueMap.put(entry.getKey(), abstractTemporalValue));
				}
			});
			temporalValuesEntity.setAdditionalProperties(temporalValueMap);
		}
		return temporalValuesEntity;
	}

	private Optional<AbstractTemporalValue> getTemporalValueFromInstanceList(List instancesList, TimeStampType timeStampType) {
		if (instancesList == null || instancesList.isEmpty()) {
			return Optional.empty();
		}
		if (instancesList.get(0) instanceof PropertyVO) {
			return Optional.of(propertyToTemporalValuesProperty((List<PropertyVO>) instancesList, timeStampType));
		}
		if (instancesList.get(0) instanceof GeoPropertyVO) {
			return Optional.of(geoPropertyToTemporalValuesProperty((List<GeoPropertyVO>) instancesList, timeStampType));
		}
		if (instancesList.get(0) instanceof RelationshipVO) {
			return Optional.of(relationshipToTemporalValuesRelationship((List<RelationshipVO>) instancesList, timeStampType));
		}
		throw new JacksonConversionException("Received unknown instances.");
	}

	default TemporalValueProperty propertyToTemporalValuesProperty(List<PropertyVO> propertyVOList, TimeStampType timeStampType) {
		TemporalValueProperty temporalValueProperty = new TemporalValueProperty();
		temporalValueProperty.setValues(
				propertyVOList.stream()
						.map(propertyVO -> List.of(propertyVO.getValue(), getTimeStampFromProperty(propertyVO, timeStampType)))
						.collect(Collectors.toList()));
		return temporalValueProperty;
	}

	default TemporalValueProperty geoPropertyToTemporalValuesProperty(List<GeoPropertyVO> propertyVOList, TimeStampType timeStampType) {
		TemporalValueProperty temporalValueProperty = new TemporalValueProperty();
		temporalValueProperty.setValues(
				propertyVOList.stream()
						.map(propertyVO -> List.of(propertyVO.getValue(), getTimeStampFromGeoProperty(propertyVO, timeStampType)))
						.collect(Collectors.toList()));
		return temporalValueProperty;
	}

	default TemporalValueRelationship relationshipToTemporalValuesRelationship(List<RelationshipVO> propertyVOList, TimeStampType timeStampType) {
		TemporalValueRelationship temporalValueRelationship = new TemporalValueRelationship();
		List<List<Object>> relationShipObjects = propertyVOList
				.stream()
				.map(propertyVO -> List.of(propertyVO.getObject(), getTimeStampFromRelationship(propertyVO, timeStampType)))
				.collect(Collectors.toList());
		temporalValueRelationship.setObjects(relationShipObjects);
		return temporalValueRelationship;
	}


	private Object getTimeStampFromProperty(PropertyVO propertyVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return propertyVO.createdAt();
			case MODIFIED_AT:
				return propertyVO.modifiedAt();
			default:
				return propertyVO.observedAt();
		}
	}

	private Object getTimeStampFromGeoProperty(GeoPropertyVO propertyVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return propertyVO.createdAt();
			case MODIFIED_AT:
				return propertyVO.modifiedAt();
			default:
				return propertyVO.observedAt();
		}
	}

	private Object getTimeStampFromRelationship(RelationshipVO relationshipVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return relationshipVO.createdAt();
			case MODIFIED_AT:
				return relationshipVO.modifiedAt();
			default:
				return relationshipVO.observedAt();
		}
	}
}
