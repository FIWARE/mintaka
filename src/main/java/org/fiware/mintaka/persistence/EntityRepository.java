package org.fiware.mintaka.persistence;

import io.micronaut.data.model.query.QueryModel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.domain.TimeRelation;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;

import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.time.*;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Repository implementation for retrieving temporal entity representations from the database
 */
@Singleton
@RequiredArgsConstructor
public class EntityRepository {

	private final EntityManager entityManager;

	/**
	 * Retrieve the entity from the db.
	 *
	 * @param entityId id of the entity to retrieve
	 * @return optional entity
	 */
	public Optional<NgsiEntity> findById(String entityId, TimeRelation timeRelation, LocalDateTime timeAt, LocalDateTime endTime) {
		String timeQueryPart = getTimeQueryPart(Optional.ofNullable(timeRelation).orElse(TimeRelation.BETWEEN),
				Optional.ofNullable(timeAt).map(tA -> tA.toInstant(ZoneOffset.UTC)).orElse(null),
				Optional.ofNullable(endTime).map(tE -> tE.toInstant(ZoneOffset.UTC)).orElse(null),
				"ts", "entity");
		String wherePart = "entity.id=:id";
		if (!timeQueryPart.isEmpty()) {
			// give me the last non-deleted instance inside the timeframe or the last instance(including deleted) before the frame.
			wherePart = "(entity.id=:id " + timeQueryPart +
					" and entity.opMode!='" + OpMode.Delete.name() + "')" +
					String.format(" or (entity.id=:id and entity.ts <='%s')", timeAt);
		}

		TypedQuery<NgsiEntity> getNgsiEntitiesQuery = entityManager.createQuery(
				"Select entity from NgsiEntity entity where " +
						wherePart +
						" order by entity.ts desc", NgsiEntity.class);
		getNgsiEntitiesQuery.setParameter("id", entityId);
		getNgsiEntitiesQuery.setMaxResults(1);
		List<NgsiEntity> ngsiEntityList = getNgsiEntitiesQuery.getResultList();
		// only return the entity if its not deleted.
		return ngsiEntityList.stream().findFirst().filter(ngsiEntity -> ngsiEntity.getOpMode() != OpMode.Delete);
	}

	/**
	 * Find all attributes of an entity in the define timeframe
	 *
	 * @param entityId     id to get attributes for
	 * @param timeRelation relation to be used in time query
	 * @param timeAt       reference timestamp
	 * @param endTime      endTime in case relation "between" is usd
	 * @param attributes   the attributes to be included, if null or empty return all
	 * @param lastN        number of instances to be retrieved, will retrieve the last instances
	 * @param timeField    timeproperty to be used for the timerelations
	 * @return list of attribute instances
	 */
	public List<Attribute> findAttributeByEntityId(String entityId, TimeRelation timeRelation, Instant timeAt, Instant endTime, List<String> attributes, Integer lastN, String timeField) {

		String timeQueryPart = getTimeQueryPart(timeRelation, timeAt, endTime, timeField);
		if (attributes == null || attributes.isEmpty()) {
			attributes = findAllAttributesForEntity(entityId, timeQueryPart, timeField);
		}

		return attributes.stream()
				.flatMap(attributeId -> findAttributeInstancesForEntity(entityId, attributeId, timeQueryPart, lastN).stream())
				.collect(Collectors.toList());
	}

	/**
	 * Return all sub attribute instnaces for the given attribute instance
	 *
	 * @param entityId            entity the attributes and subattributes are connected to
	 * @param attributeInstanceId id of the concrete attribute
	 * @param lastN               number of instances to be retrieved, will retrieve the last instances
	 * @return list of subattribute instances
	 */
	public List<SubAttribute> findSubAttributeInstancesForAttributeAndEntity(String entityId, String attributeInstanceId, Integer lastN) {
		TypedQuery<SubAttribute> getSubAttributeInstancesQuery =
				entityManager.createQuery(
						"Select subAttribute " +
								"from SubAttribute subAttribute " +
								"where subAttribute.entityId=:entityId " +
								"and subAttribute.attributeInstanceId=:attributeInstanceId " +
								// TODO: add back when opMode is added to subAttributes table
//								"and subAttribute.opMode!='" + OpMode.Delete.name() + "' " +
								"order by subAttribute.ts desc", SubAttribute.class);
		getSubAttributeInstancesQuery.setParameter("entityId", entityId);
		getSubAttributeInstancesQuery.setParameter("attributeInstanceId", attributeInstanceId);
		if (lastN != null && lastN > 0) {
			getSubAttributeInstancesQuery.setMaxResults(lastN);
		}
		return getSubAttributeInstancesQuery.getResultList();
	}

	/**
	 * Find instances of a concrete attribute for an entity.
	 *
	 * @param entityId      the entity to retrieve the entities for
	 * @param attributeId   id of the attribute to retrieve the instances for
	 * @param timeQueryPart the part of the sql query that denotes the timeframe
	 * @param lastN         number of instances to be retrieved, will retrieve the last instances
	 * @return list of attribute instances
	 */
	private List<Attribute> findAttributeInstancesForEntity(String entityId, String attributeId, String timeQueryPart, Integer lastN) {
		TypedQuery<Attribute> getAttributeInstancesQuery =
				entityManager.createQuery(
						"Select attribute " +
								"from Attribute attribute " +
								"where attribute.entityId=:entityId " +
								"and attribute.id=:attributeId " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								timeQueryPart +
								"order by attribute.ts desc", Attribute.class);
		getAttributeInstancesQuery.setParameter("entityId", entityId);
		getAttributeInstancesQuery.setParameter("attributeId", attributeId);
		if (lastN != null && lastN > 0) {
			getAttributeInstancesQuery.setMaxResults(lastN);
		}
		return getAttributeInstancesQuery.getResultList();
	}

	/**
	 * Find all id's of attributes that exist for the entity in the given timeframe
	 *
	 * @param entityId      id of the entity to get the attributes for
	 * @param timeQueryPart the part of the sql query that denotes the timeframe
	 * @param timeField     field to be used in the timequery
	 * @return list of attribute ID's
	 */
	private List<String> findAllAttributesForEntity(String entityId, String timeQueryPart, String timeField) {
		TypedQuery<String> getAttributeIdsQuery =
				entityManager.createQuery(
						"Select distinct attribute.id " +
								"from Attribute attribute " +
								"where attribute.entityId=:entityId " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								timeQueryPart, String.class);
		getAttributeIdsQuery.setParameter("entityId", entityId);
		return getAttributeIdsQuery.getResultList();
	}

	private String getTimeQueryPart(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeField, String entity) {
		InvalidTimeRelationException invalidTimeRelationException = new InvalidTimeRelationException("Received an invalid time relation.");
		if (entity == null) {
			entity = "attribute";
		}

		if (timeRelation == null && timeAt == null && endTime == null) {
			return "";
		}
		String timeProperty = "";
		if (timeField.equals("observedAt")) {
			timeProperty += String.format("%s.observedAt", entity);
		} else if (timeField.equals("createdAt")) {
			timeProperty += String.format(" %s.opMode='", entity) + OpMode.Create.name() + String.format("' and %s.ts", entity);
		} else if (timeField.equals("modifiedAt")) {
			timeProperty += String.format(" %s.opMode!='", entity) + OpMode.Create.name() + String.format("' and %s.ts", entity);
		} else if (timeField.equals("ts")) {
			timeProperty += String.format("%s.ts", entity);
		} else {
			throw new PersistenceRetrievalException(String.format("Querying by %s is currently not supported.", timeField));
		}

		LocalDateTime timeAtLDT = LocalDateTime.ofInstant(timeAt, ZoneOffset.UTC);

		switch (timeRelation) {
			case BETWEEN:
				return String.format("and %s > '%s' and  %s < '%s' ", timeProperty, timeAtLDT, timeProperty, LocalDateTime.ofInstant(endTime, ZoneOffset.UTC));
			case BEFORE:
				return String.format("and %s < '%s' ", timeProperty, timeAtLDT);
			case AFTER:
				return String.format("and %s > '%s' ", timeProperty, timeAtLDT);
			default:
				throw invalidTimeRelationException;
		}
	}


	/**
	 * Translate the time relation into a part of an sql query
	 *
	 * @param timeRelation relation to be used in time query
	 * @param timeAt       reference timestamp
	 * @param endTime      endTime in case relation "between" is usd
	 * @return the time related part of the sql query
	 */
	private String getTimeQueryPart(TimeRelation timeRelation, Instant timeAt, Instant endTime, String timeField) {
		return getTimeQueryPart(timeRelation, timeAt, endTime, timeField, null);
	}

	/**
	 * Get the list of timestamps that have opMode "create" for the given attribute.
	 *
	 * @param attributeId    id of the attribute to find the timestamps for
	 * @param entityId       id of the entity that are connected with the attribute
	 * @param isSubAttribute is the requestend attribute an attribute or a subattribute
	 * @return the list of timestamps
	 */
	public List<Instant> getCreatedAtForAttribute(String attributeId, String entityId, boolean isSubAttribute) {
		String tableName = isSubAttribute ? "SubAttribute" : "Attribute";

		TypedQuery<LocalDateTime> instantTypedQuery = entityManager.
				createQuery(
						"select attribute.ts " +
								"from " + tableName + " attribute " +
								"where attribute.entityId=:entityId and attribute.id=:attributeId and attribute.opMode='" + OpMode.Create.name() + "'" +
								" order by attribute.ts asc", LocalDateTime.class);
		instantTypedQuery.setParameter("entityId", entityId);
		instantTypedQuery.setParameter("attributeId", attributeId);

		return instantTypedQuery.getResultList().stream().map(localDateTime -> localDateTime.toInstant(ZoneOffset.UTC)).collect(Collectors.toList());
	}

}
