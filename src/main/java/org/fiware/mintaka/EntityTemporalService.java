package org.fiware.mintaka;

import io.micronaut.transaction.annotation.ReadOnly;
import lombok.RequiredArgsConstructor;
import org.fiware.mintaka.domain.ApiDomainMapper;
import org.fiware.mintaka.domain.AttributePropertyVOMapper;
import org.fiware.mintaka.domain.TimeRelation;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.EntityRepository;
import org.fiware.mintaka.persistence.NgsiEntity;
import org.fiware.ngsi.model.EntityTemporalVO;
import org.fiware.ngsi.model.TimerelVO;

import javax.inject.Singleton;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
@RequiredArgsConstructor
public class EntityTemporalService {

	private static final String TEMPORAL_VALUES_OPTION = "temporalValues";

	private final EntityRepository entityRepository;
	private final ApiDomainMapper apiDomainMapper;
	private final AttributePropertyVOMapper attributePropertyVOMapper;

	@ReadOnly
	public EntityTemporalVO getNgsiEntitiesWithTimerel(String entityId, String timeProperty, TimerelVO timeRel, Instant timeAt, Instant endTime, List<String> attrs, Integer lastN) {
		Optional<NgsiEntity> entity = entityRepository.findById(entityId).stream().findFirst();
		if (entity.isEmpty()) {
			return null;
		}
		NgsiEntity ngsiEntity = entity.get();
		List<Attribute> attributes = entityRepository.findAttributeByEntityId(entityId, apiDomainMapper.timeRelVoToTimeRelation(timeRel), timeAt, endTime, attrs, lastN, timeProperty);
		//TODO: retrieve subattributes
		List<Attribute> attributesWithSubattributes = attributes.stream().filter(Attribute::getSubProperties).collect(Collectors.toList());


		Map<String, List<Instant>> createdAtMap = new HashMap<>();
		Map<String, Object> temporalAttributes = new HashMap<>();

		EntityTemporalVO entityTemporalVO = new EntityTemporalVO();
		entityTemporalVO.type(ngsiEntity.getType()).id(URI.create(entityId));
		attributes.stream()
				.map(attribute -> getAttributeEntryWithCreatedAt(createdAtMap, attribute))
				.forEach(entry -> addEntryToTemporalAttributes(temporalAttributes, entry));


		entityTemporalVO.setAdditionalProperties(temporalAttributes);
		return entityTemporalVO;

	}

	private void addEntryToTemporalAttributes(Map<String, Object> temporalAttributes, Map.Entry<String, Object> entry) {
		List<Object> attributeList = (List<Object>) temporalAttributes.computeIfAbsent(entry.getKey(), entryKey -> new ArrayList<>());
		attributeList.add(entry.getValue());
		temporalAttributes.put(entry.getKey(), attributeList);
	}

	private Map.Entry<String, Object> getAttributeEntryWithCreatedAt(Map<String, List<Instant>> createdAtMap, Attribute attribute) {
		List<Instant> createdAtList = createdAtMap.computeIfAbsent(
				attribute.getId(),
				k -> entityRepository.getCreatedAtForAttribute(attribute.getId(), attribute.getEntityId()));
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

	private Map.Entry<String, Object> attributeToMapEntry(Attribute attribute, Instant createdAt) {
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
