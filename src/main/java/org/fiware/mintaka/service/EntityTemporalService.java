package org.fiware.mintaka.service;

import io.micronaut.transaction.annotation.ReadOnly;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.domain.AttributePropertyVOMapper;
import org.fiware.mintaka.domain.EntityIdTempResults;
import org.fiware.mintaka.domain.PaginationInformation;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.fiware.mintaka.domain.query.geo.GeoQuery;
import org.fiware.mintaka.domain.query.ngsi.QueryTerm;
import org.fiware.mintaka.persistence.AbstractAttribute;
import org.fiware.mintaka.persistence.Attribute;
import org.fiware.mintaka.persistence.EntityRepository;
import org.fiware.mintaka.persistence.LimitableResult;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class EntityTemporalService {

	private static final String CREATED_AT_PROPERTY = "createdAt";
	private static final String MODIFIED_AT_PROPERTY = "modifiedAt";

	private final EntityRepository entityRepository;
	private final AttributePropertyVOMapper attributePropertyVOMapper;

	@ReadOnly
	public LimitableResult<List<EntityTemporalVO>> getEntitiesWithQuery(
			Optional<List<String>> ids,
			Optional<String> namePattern,
			List<String> expandedTypes,
			List<String> expandedAttributes,
			Optional<QueryTerm> query,
			Optional<GeoQuery> geoQuery,
			TimeQuery timeQuery,
			Integer lastN,
			boolean sysAttrs,
			boolean temporalRepresentation,
			int pageSize,
			Optional<String> anchor) {

		AtomicBoolean isLimited = new AtomicBoolean(false);

		List<EntityIdTempResults> entityIdTempResultsList = entityRepository.findEntityIdsAndTimeframesByQuery(ids, namePattern, expandedTypes, timeQuery, geoQuery, query, pageSize, anchor);

		Map<String, List<EntityIdTempResults>> tempResultsMap = new HashMap<>();

		entityIdTempResultsList.stream().forEach(tempResult -> {
			if (tempResultsMap.containsKey(tempResult.getEntityId())) {
				tempResultsMap.get(tempResult.getEntityId()).add(tempResult);
			} else {
				List<EntityIdTempResults> tempResults = new ArrayList<>();
				tempResults.add(tempResult);
				tempResultsMap.put(tempResult.getEntityId(), tempResults);
			}
		});

		int limitPerEntity = entityRepository.getLimit(tempResultsMap.keySet().size(), expandedAttributes.size(), lastN);

		tempResultsMap.entrySet().forEach(entry -> {
			if (lastN == null || lastN > 0) {
				entry.getValue().sort(Comparator.comparing(EntityIdTempResults::getStartTime).reversed());
			} else {
				entry.getValue().sort(Comparator.comparing(EntityIdTempResults::getStartTime));
			}
		});
		boolean backwards = lastN != null;
		List<EntityTemporalVO> entityTemporalVOS = new ArrayList<>(tempResultsMap.values().stream()
				.map(tempResults -> getLimitableEntityTemporal(tempResults,
						timeQuery.getTimeProperty(),
						expandedAttributes,
						sysAttrs,
						temporalRepresentation,
						limitPerEntity,
						backwards))
				.filter(Optional::isPresent)
				.flatMap(optionalResult -> {
					LimitableResult<List<EntityTemporalVO>> limitableResult = optionalResult.get();
					if (limitableResult.isLimited()) {
						isLimited.set(true);
					}
					return limitableResult.getResult().stream();
				})
				.collect(Collectors.toMap(EntityTemporalVO::getId, etVO -> etVO, this::mergeEntityTemporals)).values());

		return new LimitableResult<>(entityTemporalVOS, isLimited.get());
	}

	/**
	 * Count the number of entities that match the current query.
	 */
	@ReadOnly
	public Number countMatchingEntities(
			Optional<List<String>> ids,
			Optional<String> namePattern,
			List<String> expandedTypes,
			Optional<QueryTerm> query,
			Optional<GeoQuery> geoQuery,
			TimeQuery timeQuery) {
		return entityRepository.getCount(ids, namePattern, expandedTypes, timeQuery, geoQuery, query);
	}

	@ReadOnly
	public PaginationInformation getPaginationInfo(
			Optional<List<String>> ids,
			Optional<String> namePattern,
			List<String> expandedTypes,
			Optional<QueryTerm> query,
			Optional<GeoQuery> geoQuery,
			TimeQuery timeQuery,
			int pageSize,
			Optional<String> anchor) {

		return entityRepository.getPaginationInfo(ids, namePattern, expandedTypes, timeQuery, geoQuery, query, pageSize, anchor);
	}

	private int maxNumberOfInstances(EntityTemporalVO entityTemporalVO) {
		int size = getSizeFromNullableList(entityTemporalVO.getLocation());
		if (size > getSizeFromNullableList(entityTemporalVO.getOperationSpace())) {
			size = getSizeFromNullableList(entityTemporalVO.getOperationSpace());
		}
		if (size > getSizeFromNullableList(entityTemporalVO.getObservationSpace())) {
			size = getSizeFromNullableList(entityTemporalVO.getObservationSpace());
		}
		for (Object additionalProperty : entityTemporalVO.getAdditionalProperties().values()) {
			if ((additionalProperty instanceof List) && ((List) additionalProperty).size() > size) {
				size = ((List) additionalProperty).size();
			}
		}
		return size;
	}

	private int getSizeFromNullableList(List nullableList) {
		return Optional.ofNullable(nullableList).map(List::size).orElse(0);
	}


	private Optional<LimitableResult<List<EntityTemporalVO>>> getLimitableEntityTemporal(
			List<EntityIdTempResults> tempResults,
			String timeProperty,
			List<String> expandedAttributes,
			boolean sysAttrs,
			boolean temporalRepresentation,
			int limitPerEntity,
			boolean backwards) {
		boolean limited = false;
		List<EntityTemporalVO> entityTemporalVOS = new ArrayList<>();

		for (EntityIdTempResults result : tempResults) {
			Optional<LimitableResult<EntityTemporalVO>> optionalLimitableResult = getNgsiEntitiesWithTimerel(result.getEntityId(),
					new TimeQuery(TimeRelation.BETWEEN, result.getStartTime(), result.getEndTime(), timeProperty, true),
					expandedAttributes,
					sysAttrs,
					temporalRepresentation,
					limitPerEntity,
					backwards);
			if (optionalLimitableResult.isPresent()) {
				LimitableResult<EntityTemporalVO> limitableResult = optionalLimitableResult.get();
				entityTemporalVOS.add(optionalLimitableResult.get().getResult());
				limited = limitableResult.isLimited();
				limitPerEntity -= maxNumberOfInstances(limitableResult.getResult());

			}
			if (limitPerEntity < 1) {
				break;
			}
		}

		if (entityTemporalVOS.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new LimitableResult<>(entityTemporalVOS, limited));

	}

	private EntityTemporalVO mergeEntityTemporals(EntityTemporalVO entityTemporalVO1, EntityTemporalVO entityTemporalVO2) {
		Optional.ofNullable(entityTemporalVO1.getLocation())
				.ifPresentOrElse(location -> location.addAll(entityTemporalVO2.getLocation()), () -> entityTemporalVO1.setLocation(entityTemporalVO2.getLocation()));
		Optional.ofNullable(entityTemporalVO1.getOperationSpace())
				.ifPresentOrElse(operationSpace -> operationSpace.addAll(entityTemporalVO2.getOperationSpace()), () -> entityTemporalVO1.setOperationSpace(entityTemporalVO2.getOperationSpace()));
		Optional.ofNullable(entityTemporalVO1.getObservationSpace())
				.ifPresentOrElse(observationSpace -> observationSpace.addAll(entityTemporalVO2.getObservationSpace()), () -> entityTemporalVO1.setObservationSpace(entityTemporalVO2.getObservationSpace()));

		if (entityTemporalVO1.getAdditionalProperties() == null) {
			entityTemporalVO1.setAdditionalProperties(entityTemporalVO2.getAdditionalProperties());
		} else {
			entityTemporalVO2.getAdditionalProperties()
					.forEach((key, value) -> entityTemporalVO1.getAdditionalProperties()
							.merge(key, value, (v1, v2) -> {
								((List) v1).addAll((List) v2);
								return v1;
							}));
		}
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

	private Optional<LimitableResult<EntityTemporalVO>> getNgsiEntitiesWithTimerel(
			String entityId,
			TimeQuery timeQuery,
			List<String> attrs,
			boolean sysAttrs,
			boolean temporalRepresentation,
			Integer limit,
			boolean backwards) {

		LimitableResult<List<Attribute>> limitableAttributesList = entityRepository.findAttributeByEntityId(entityId, timeQuery, attrs, limit, backwards);
		List<Attribute> attributes = limitableAttributesList.getResult();
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
				.map(attribute -> {
					attributeTimeStamps.add(attribute.getTs());
					if (includeCreatedAt) {
						return getAttributeEntryWithCreatedAt(createdAtMap, attribute, false, includeModifiedAt);
					} else {
						return attributeToMapEntry(attribute, null, includeModifiedAt);
					}
				})
				.forEach(entry -> {
					if (!temporalRepresentation) {
						addSubAttributesToAttributeInstance(entityId, entry, attributesWithSubattributes, includeCreatedAt, includeModifiedAt, limit, backwards);
					}
					addEntryToTemporalAttributes(temporalAttributes, entry);
				});

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
		return Optional.of(new LimitableResult<EntityTemporalVO>(entityTemporalVO, limitableAttributesList.isLimited()));
	}

	@ReadOnly
	public Optional<LimitableResult<EntityTemporalVO>> getNgsiEntitiesWithTimerel(
			String entityId,
			TimeQuery timeQuery,
			List<String> attrs,
			Integer lastN,
			boolean sysAttrs,
			boolean temporalRepresentation) {
		return getNgsiEntitiesWithTimerel(entityId, timeQuery, attrs, sysAttrs, temporalRepresentation, entityRepository.getLimit(1, attrs.size(), lastN), lastN != null);
	}


	private void addSubAttributesToAttributeInstance(String entityId, Map.Entry<String, Object> propertyEntry, List<String> instancesWithSubattributes, boolean createdAt, boolean modifiedAt, int limit, boolean backwards) {
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

		getSubAttributesForInstance(entityId, instanceId, createdAt, modifiedAt, limit, backwards)
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

	private List<Map.Entry<String, Object>> getSubAttributesForInstance(String entityId, String attributeId, boolean createdAt, boolean modifiedAt, int limit, boolean backwards) {
		if (createdAt) {
			Map<String, List<Instant>> createdAtMap = new HashMap<>();
			return entityRepository.findSubAttributeInstancesForAttributeAndEntity(entityId, attributeId, limit, backwards)
					.stream()
					.map(attribute -> getAttributeEntryWithCreatedAt(createdAtMap, attribute, true, modifiedAt))
					.collect(Collectors.toList());
		}
		return entityRepository.findSubAttributeInstancesForAttributeAndEntity(entityId, attributeId, limit, backwards)
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

		if (attributePropertyVOMapper.isRelationship(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToRelationShip(attribute, createdAt, modifiedAt));
		}
		if (attributePropertyVOMapper.isGeoProperty(attribute)) {
			return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToGeoProperty(attribute, createdAt, modifiedAt));
		}
		return Map.entry(attribute.getId(), attributePropertyVOMapper.attributeToPropertyVO(attribute, createdAt, modifiedAt));
	}


}
