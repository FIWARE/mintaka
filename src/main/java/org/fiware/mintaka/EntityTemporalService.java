package org.fiware.mintaka;

import com.sun.jdi.request.StepRequest;
import io.micronaut.transaction.annotation.ReadOnly;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.domain.ApiDomainMapper;
import org.fiware.mintaka.domain.AttributePropertyVOMapper;
import org.fiware.mintaka.persistence.AbstractAttribute;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.EntityRepository;
import org.fiware.mintaka.persistence.NgsiEntity;
import org.fiware.ngsi.model.*;

import javax.inject.Singleton;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class EntityTemporalService {

	private static final String TEMPORAL_VALUES_OPTION = "temporalValues";

	private final EntityRepository entityRepository;
	private final ApiDomainMapper apiDomainMapper;
	private final AttributePropertyVOMapper attributePropertyVOMapper;

	@ReadOnly
	public EntityTemporalVO getNgsiEntitiesWithTimerel(String entityId, String timeProperty, TimerelVO timeRel, Instant timeAt, Instant endTime, List<String> attrs, Integer lastN) {
		Optional<NgsiEntity> entity = entityRepository.findById(entityId);
		if (entity.isEmpty()) {
			return null;
		}
		NgsiEntity ngsiEntity = entity.get();
		List<Attribute> attributes = entityRepository.findAttributeByEntityId(entityId, apiDomainMapper.timeRelVoToTimeRelation(timeRel), timeAt, endTime, attrs, lastN, timeProperty);
		List<String> attributesWithSubattributes = attributes.stream()
				.filter(Attribute::getSubProperties)
				.map(Attribute::getInstanceId)
				.collect(Collectors.toList());

		Map<String, List<Instant>> createdAtMap = new HashMap<>();
		Map<String, Object> temporalAttributes = new HashMap<>();

		EntityTemporalVO entityTemporalVO = new EntityTemporalVO();
		entityTemporalVO.type(ngsiEntity.getType()).id(URI.create(entityId));
		attributes.stream()
				.map(attribute -> getAttributeEntryWithCreatedAt(createdAtMap, attribute, false))
				.peek(attributeEntry -> addSubAttributesToAttributeInstance(entityId, attributeEntry, lastN, attributesWithSubattributes))
				.forEach(entry -> addEntryToTemporalAttributes(temporalAttributes, entry));

		entityTemporalVO.setAdditionalProperties(temporalAttributes);
		return entityTemporalVO;

	}

	private void addSubAttributesToAttributeInstance(String entityId, Map.Entry<String, Object> propertyEntry, int lastN, List<String> instancesWithSubattributes) {
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

		getSubAttributesForInstance(entityId, instanceId, lastN)
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

	private List<Map.Entry<String, Object>> getSubAttributesForInstance(String entityId, String attributeId, int lastN) {

		Map<String, List<Instant>> createdAtMap = new HashMap<>();
		return entityRepository.findSubAttributeInstancesForAttributeAndEntity(entityId, attributeId, lastN)
				.stream()
				.map(attribute -> getAttributeEntryWithCreatedAt(createdAtMap, attribute, true))
				.collect(Collectors.toList());
	}


	private void addEntryToTemporalAttributes(Map<String, Object> temporalAttributes, Map.Entry<String, Object> entry) {
		List<Object> attributeList = (List<Object>) temporalAttributes.computeIfAbsent(entry.getKey(), entryKey -> new ArrayList<>());
		attributeList.add(entry.getValue());
		temporalAttributes.put(entry.getKey(), attributeList);
	}

	private Map.Entry<String, Object> getAttributeEntryWithCreatedAt(Map<String, List<Instant>> createdAtMap, AbstractAttribute attribute, boolean isSubAttribute) {
		if (isSubAttribute) {
			// TODO: go back to old solution when opmode is added to subAttributes table
			return attributeToMapEntry(attribute, attribute.getTs());

		}
		List<Instant> createdAtList = createdAtMap.computeIfAbsent(
				attribute.getId(),
				k -> entityRepository.getCreatedAtForAttribute(attribute.getId(), attribute.getEntityId(), isSubAttribute));
		return attributeToMapEntry(attribute, findClosestBefore(createdAtList, attribute.getTs()));
	}


	// we expect the createdAtList to be sorted ascending. We dont sort here, because we retrieve it already sorted from the db
	private Instant findClosestBefore(List<Instant> createdAtList, Instant attributeTs) {
		Instant createdAt = Instant.ofEpochMilli(0);
		for (Instant currentInstant : createdAtList) {
			// we check if NOT after, because we want "before or equal
			if (!currentInstant.isAfter(attributeTs)) {
				createdAt = currentInstant;
			} else {
				break;
			}
		}
		return createdAt;
	}

	private Map.Entry<String, Object> attributeToMapEntry(AbstractAttribute attribute, Instant createdAt) {
		Date createdAtDate = Date.from(createdAt);
		if (attributePropertyVOMapper.isRelationShip(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToRelationShip(attribute, createdAtDate));
		}
		if (attributePropertyVOMapper.isGeoProperty(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToGeoProperty(attribute, createdAtDate));
		}
		return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToPropertyVO(attribute, createdAtDate));
	}

}
