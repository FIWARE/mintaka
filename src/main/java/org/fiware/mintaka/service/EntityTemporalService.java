package org.fiware.mintaka.service;

import io.micronaut.transaction.annotation.ReadOnly;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.domain.*;
import org.fiware.mintaka.persistence.AbstractAttribute;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.EntityRepository;
import org.fiware.mintaka.persistence.NgsiEntity;
import org.fiware.ngsi.model.EntityTemporalVO;
import org.fiware.ngsi.model.GeoPropertyVO;
import org.fiware.ngsi.model.PropertyVO;
import org.fiware.ngsi.model.RelationshipVO;

import javax.inject.Singleton;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class EntityTemporalService {

	private static final String CREATED_AT_PROPERTY = "createdAt";
	private static final String MODIFIED_AT_PROPERTY = "modifiedAt";

	private final EntityRepository entityRepository;
	private final ApiDomainMapper apiDomainMapper;
	private final AttributePropertyVOMapper attributePropertyVOMapper;

	@ReadOnly
	public List<EntityTemporalVO> getEntitiesWithQuery(
			Optional<String> namePattern,
			List<String> expandedTypes,
			List<String> expandedAttributes,
			Optional<String> query,
			Optional<GeoQuery> geoQuery,
			TimeQuery timeQuery,
			Integer lastN,
			boolean sysAttrs,
			boolean temporalRepresentation) {


		return new ArrayList<EntityTemporalVO>(
				entityRepository.findEntityIdsByQuery(namePattern, expandedTypes, timeQuery, geoQuery)
						.stream()
						.map(tempResult -> getNgsiEntitiesWithTimerel(
								tempResult.getEntiyId(),
								new TimeQuery(TimeRelation.BETWEEN, tempResult.getStartTime(), tempResult.getEndTime(), timeQuery.getTimeProperty()),
								expandedAttributes,
								lastN,
								sysAttrs,
								temporalRepresentation))
						.filter(Optional::isPresent)
						.map(Optional::get)
						.collect(Collectors.toMap(EntityTemporalVO::getId, etVO -> etVO, this::mergeEntityTemporals)).values());
	}

	private EntityTemporalVO mergeEntityTemporals(EntityTemporalVO entityTemporalVO1, EntityTemporalVO entityTemporalVO2) {
		entityTemporalVO1.getLocation().addAll(entityTemporalVO2.getLocation());
		entityTemporalVO1.getOperationSpace().addAll(entityTemporalVO2.getOperationSpace());
		entityTemporalVO1.getObservationSpace().addAll(entityTemporalVO2.getObservationSpace());
		entityTemporalVO2.getAdditionalProperties()
				.forEach((key, value) -> entityTemporalVO1.getAdditionalProperties()
						.merge(key, value, (v1, v2) -> ((List) v1).addAll((List) v2)));
		// we use the earliest createdAt, since we assume the entity exists for the whole timeframe. Its undefined by the api when an entity was deleted and recreated inside one timeframe.
		if (entityTemporalVO1.createdAt() != null && entityTemporalVO2.createdAt() != null) {
			entityTemporalVO1.createdAt(entityTemporalVO1.createdAt().isBefore(entityTemporalVO2.createdAt()) ? entityTemporalVO1.createdAt() : entityTemporalVO2.createdAt());
		}
		// we use the latest modifiedAt
		if (entityTemporalVO1.modifiedAt() != null && entityTemporalVO2.modifiedAt() != null) {
			entityTemporalVO1.modifiedAt(entityTemporalVO1.modifiedAt().isBefore(entityTemporalVO2.modifiedAt()) ? entityTemporalVO2.modifiedAt() : entityTemporalVO1.modifiedAt());
		}
		if (!entityTemporalVO1.getType().equals(entityTemporalVO2.getType())) {
			// we let the type untouched, since we do not support delete-recreate inside one timeframe, wich would be the only way to change type
			log.warn("Type of entity {} changed inside the requested timeframe. {} to {}", entityTemporalVO1.getId(), entityTemporalVO1.getType(), entityTemporalVO2.getType());
		}
		return entityTemporalVO1;
	}

	@ReadOnly
	public Optional<EntityTemporalVO> getNgsiEntitiesWithTimerel(
			String entityId,
			TimeQuery timeQuery,
			List<String> attrs,
			Integer lastN,
			boolean sysAttrs,
			boolean temporalRepresentation) {
		List<Attribute> attributes = entityRepository.findAttributeByEntityId(entityId, timeQuery, attrs, lastN);
		List<String> attributesWithSubattributes = attributes.stream()
				.filter(Attribute::getSubProperties)
				.map(Attribute::getInstanceId)
				.collect(Collectors.toList());

		Map<String, List<Instant>> createdAtMap = new HashMap<>();
		Map<String, Object> temporalAttributes = new HashMap<>();


		List<LocalDateTime> attributeTimeStamps = new ArrayList<>();

		boolean includeCreatedAt = sysAttrs || timeQuery.getTimeProperty().equals(CREATED_AT_PROPERTY);
		boolean includeModifiedAt = sysAttrs || timeQuery.getTimeProperty().equals(MODIFIED_AT_PROPERTY);

		attributes.stream()
				.peek(attribute -> attributeTimeStamps.add(attribute.getTs()))
				.map(attribute -> {
					if (includeCreatedAt) {
						return getAttributeEntryWithCreatedAt(createdAtMap, attribute, false, includeModifiedAt);
					} else {
						return attributeToMapEntry(attribute, null, includeModifiedAt);
					}
				})
				.peek(attributeEntry -> {
					if (!temporalRepresentation) {
						addSubAttributesToAttributeInstance(entityId, attributeEntry, lastN, attributesWithSubattributes, includeCreatedAt, includeModifiedAt);
					}
				})
				.forEach(entry -> addEntryToTemporalAttributes(temporalAttributes, entry));

		if (attributeTimeStamps.isEmpty()) {
			return Optional.empty();
		}
		// filter the entity in the timestamp we have attributes for.

		Optional<NgsiEntity> entity = entityRepository.findById(entityId,
				new TimeQuery(
						TimeRelation.BETWEEN,
						attributeTimeStamps.get(0).toInstant(ZoneOffset.UTC),
						attributeTimeStamps.get(attributeTimeStamps.size() - 1).toInstant(ZoneOffset.UTC),
						"ts"));
		// sort them, to get the first and last timestamp
		attributeTimeStamps.sort(Comparator.naturalOrder());
		if (entity.isEmpty()) {
			return Optional.empty();
		}
		NgsiEntity ngsiEntity = entity.get();
		EntityTemporalVO entityTemporalVO = new EntityTemporalVO();
		entityTemporalVO.type(ngsiEntity.getType()).id(URI.create(entityId));

		entityTemporalVO.setAdditionalProperties(temporalAttributes);
		return Optional.ofNullable(entityTemporalVO);

	}


	private void addSubAttributesToAttributeInstance(String entityId, Map.Entry<String, Object> propertyEntry, Integer lastN, List<String> instancesWithSubattributes, boolean createdAt, boolean modifiedAt) {
		if (propertyEntry.getValue() == null) {
			log.debug("The received property entry {} does not have a value.", propertyEntry.getKey());
			return;
		}
		String instanceId = getInstanceIdFromObject(propertyEntry.getValue());
		if (!instancesWithSubattributes.contains(instanceId)) {
			// early exit, since there are not sub attributes
			return;
		}
		Map<String, Object> temporalSubAttributes = new HashMap<>();

		getSubAttributesForInstance(entityId, instanceId, lastN, createdAt, modifiedAt)
				.forEach(entry -> addEntryToTemporalAttributes(temporalSubAttributes, entry));

		if (propertyEntry.getValue() instanceof PropertyVO) {
			PropertyVO propertyVO = (PropertyVO) propertyEntry.getValue();
			if (propertyVO.getAdditionalProperties() == null) {
				propertyVO.setAdditionalProperties(temporalSubAttributes);
			} else {
				propertyVO.getAdditionalProperties().putAll(temporalSubAttributes);
			}
		} else if (propertyEntry.getValue() instanceof GeoPropertyVO) {
			GeoPropertyVO geoPropertyVO = (GeoPropertyVO) propertyEntry.getValue();
			if (geoPropertyVO.getAdditionalProperties() == null) {
				geoPropertyVO.setAdditionalProperties(temporalSubAttributes);
			} else {
				geoPropertyVO.getAdditionalProperties().putAll(temporalSubAttributes);
			}
		} else if (propertyEntry.getValue() instanceof RelationshipVO) {
			RelationshipVO relationshipVO = (RelationshipVO) propertyEntry.getValue();
			if (relationshipVO.getAdditionalProperties() == null) {
				relationshipVO.setAdditionalProperties(temporalSubAttributes);
			} else {
				relationshipVO.getAdditionalProperties().putAll(temporalSubAttributes);
			}
		}
	}

	private String getInstanceIdFromObject(Object attributeObject) {
		if (attributeObject instanceof PropertyVO) {
			return ((PropertyVO) attributeObject).getInstanceId().toString();
		} else if (attributeObject instanceof GeoPropertyVO) {
			return ((GeoPropertyVO) attributeObject).getInstanceId().toString();
		} else if (attributeObject instanceof RelationshipVO) {
			return ((RelationshipVO) attributeObject).getInstanceId().toString();
		}
		return "";
	}

	private List<Map.Entry<String, Object>> getSubAttributesForInstance(String entityId, String attributeId, Integer lastN, boolean createdAt, boolean modifiedAt) {
		if (createdAt) {
			Map<String, List<Instant>> createdAtMap = new HashMap<>();
			return entityRepository.findSubAttributeInstancesForAttributeAndEntity(entityId, attributeId, lastN)
					.stream()
					.map(attribute -> getAttributeEntryWithCreatedAt(createdAtMap, attribute, true, modifiedAt))
					.collect(Collectors.toList());
		}
		return entityRepository.findSubAttributeInstancesForAttributeAndEntity(entityId, attributeId, lastN)
				.stream()
				.map(subAttribute -> attributeToMapEntry(subAttribute, null, modifiedAt))
				.collect(Collectors.toList());
	}


	private void addEntryToTemporalAttributes(Map<String, Object> temporalAttributes, Map.Entry<String, Object> entry) {
		List<Object> attributeList = (List<Object>) temporalAttributes.computeIfAbsent(entry.getKey(), entryKey -> new ArrayList<>());
		attributeList.add(entry.getValue());
		temporalAttributes.put(entry.getKey(), attributeList);
	}

	private Map.Entry<String, Object> getAttributeEntryWithCreatedAt(Map<String, List<Instant>> createdAtMap, AbstractAttribute attribute, boolean isSubAttribute, boolean modifiedAt) {
		if (isSubAttribute) {
			// TODO: go back to old solution when opMode is added to subAttributes table
			return attributeToMapEntry(attribute, attribute.getTs().toInstant(ZoneOffset.UTC), modifiedAt);

		}
		List<Instant> createdAtList = createdAtMap.computeIfAbsent(
				attribute.getId(),
				k -> entityRepository.getCreatedAtForAttribute(attribute.getId(), attribute.getEntityId(), isSubAttribute));
		return attributeToMapEntry(attribute, findClosestBefore(createdAtList, attribute.getTs().toInstant(ZoneOffset.UTC)), modifiedAt);
	}

	// we expect the createdAtList to be sorted ascending. We dont sort here, because we retrieve it already sorted from the db
	private Instant findClosestBefore(List<Instant> createdAtList, Instant attributeTs) {
		Instant createdAt = Instant.ofEpochMilli(0);
		for (Instant currentDate : createdAtList) {
			// we check if NOT after, because we want "before or equal
			if (!currentDate.isAfter(attributeTs)) {
				createdAt = currentDate;
			} else {
				break;
			}
		}
		return createdAt;
	}

	private Map.Entry<String, Object> attributeToMapEntry(AbstractAttribute attribute, Instant createdAt, boolean modifiedAt) {

		if (attributePropertyVOMapper.isRelationShip(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToRelationShip(attribute, createdAt, modifiedAt));
		}
		if (attributePropertyVOMapper.isGeoProperty(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToGeoProperty(attribute, createdAt, modifiedAt));
		}
		return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToPropertyVO(attribute, createdAt, modifiedAt));
	}


}
