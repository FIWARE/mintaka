package org.fiware.mintaka.persistence;

import io.micronaut.data.model.query.QueryModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.domain.EntityIdTempResults;
import org.fiware.mintaka.domain.GeoQuery;
import org.fiware.mintaka.domain.TimeQuery;
import org.fiware.mintaka.domain.TimeRelation;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;

import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.net.URI;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Repository implementation for retrieving temporal entity representations from the database
 */
@Slf4j
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
	public Optional<NgsiEntity> findById(String entityId, TimeQuery timeQuery) {
		String timeQueryPart = timeQuery.getSqlRepresentation("entity");
		String wherePart = "entity.id=:id";
		if (!timeQueryPart.isEmpty()) {
			// give me the last non-deleted instance inside the timeframe or the last instance(including deleted) before the frame.
			wherePart = "(entity.id=:id " + timeQueryPart +
					" and entity.opMode!='" + OpMode.Delete.name() + "')" +
					String.format(" or (entity.id=:id and entity.ts <='%s')", timeQuery.getTimeAt());
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
	 * @param entityId   id to get attributes for
	 * @param timeQuery  time related query
	 * @param attributes the attributes to be included, if null or empty return all
	 * @param lastN      number of instances to be retrieved, will retrieve the last instances
	 * @return list of attribute instances
	 */
	public List<Attribute> findAttributeByEntityId(String entityId, TimeQuery timeQuery, List<String> attributes, Integer lastN) {

		String timeQueryPart = timeQuery.getSqlRepresentation();
		if (attributes == null || attributes.isEmpty()) {
			attributes = findAllAttributesForEntity(entityId, timeQueryPart);
		}

		// we need to do single queries in order to fullfil the "lastN" parameter.
		return attributes.stream()
				.flatMap(attributeId -> findAttributeInstancesForEntity(entityId, attributeId, timeQueryPart, lastN).stream())
				.collect(Collectors.toList());
	}

	public List<EntityIdTempResults> findEntityIdsByQuery(Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery) {
		String timeQueryPart = timeQuery.getSqlRepresentation();

		// The query:
		// find all entityIds that match the pattern, type and are not deleted
		// then get all attributes with the geoProperties attribute id and the result of its geoquery in the requested timeframe
		// then sort by entity-id and timestamp and combine consecutive rows with distinct query results into single rows

		String idSubSelect = "SELECT DISTINCT entity.id FROM entities entity WHERE entity.opMode != '" + OpMode.Delete.name() + "' ";
		if (!types.isEmpty()) {

			idSubSelect += " AND entity.type in (" + types.stream().map(type -> String.format("'%s'", type)).collect(Collectors.joining(",")) + ")";
		}

		if (idPattern.isPresent()) {
			idSubSelect += " AND entity.id ~ '" + idPattern.get() + "' ";
		}

		log.debug("Subselect: {}", idSubSelect);

		String innerJoin = "SELECT ";
		if (geoQuery.isPresent()) {
			innerJoin += geoQuery.get().getSqlRepresentation() + " as geoResult";
		}
		//TODO: at query result
		//if(query.isPresent()){
		//innerJoin += query.get().getSqlRepresentation() + "as queryResult";
		//}
		innerJoin += ",attribute." + timeQuery.getTimeProperty() + ",attribute.instanceId,attribute.entityId " +
				"FROM attributes attribute WHERE attribute.entityId in (" + idSubSelect + ") ";
		if (geoQuery.isPresent()) {
			innerJoin += "and attribute.id='" + geoQuery.get().getGeoProperty() + "' ";
		}

		innerJoin += timeQueryPart + ") " +
				"sub USING (instanceId)";
		log.debug("InnerJoin: {}", innerJoin);

		String outerJoin = "SELECT attribute2.instanceId, sub.geoResult as geoResult, LAG(sub.geoResult, 1) OVER (ORDER BY sub.entityId,sub." + timeQuery.getTimeProperty() + ") as lag FROM attributes attribute2 " +
				"JOIN (" + innerJoin;

		String finalJoin = "SELECT sub_lag.geoResult, attributeOut." + timeQuery.getTimeProperty() + ", attributeOut.entityId, ROW_NUMBER() OVER(order by attributeOut.entityId, attributeOut." + timeQuery.getTimeProperty() + ") as rn, attributeOut.instanceId FROM attributes attributeOut " +
				"JOIN(" + outerJoin + ") sub_lag USING (instanceId) WHERE lag!=geoResult";
		log.debug("Final join: {}", finalJoin);

		String fullSelect = "SELECT finalJoin.geoResult, finalJoin.entityId as entity_id, LAG(finalJoin." + timeQuery.getTimeProperty() + ") OVER (ORDER BY finalJoin.rn) as start, finalJoin." + timeQuery.getTimeProperty() + " as end FROM attributes a "
				+ "JOIN(" + finalJoin + ") finalJoin USING (instanceId)";

		Query getIdsAndTimeRangeQuery = entityManager.createNativeQuery(fullSelect);

		List<Object[]> queryResultList = getIdsAndTimeRangeQuery.getResultList();

		return queryResultList.stream()
				.map(queryResult -> Arrays.asList(queryResult))
				.filter(queryResult -> queryResult.size() == 4)
				//false denotes the line combining the first true-timestamp and the first false-timestamp, e.g. the rows that contain our timeframes.
				.filter(queryResult -> !(Boolean) queryResult.get(0))
				.map(queryResult -> mapQueryResultToPojo(queryResult))
				.collect(Collectors.toList());
	}

	private EntityIdTempResults mapQueryResultToPojo(List<Object> queryResult) {
		if (!(queryResult.get(1) instanceof String)) {
			throw new PersistenceRetrievalException(String.format("The query-result contains a non-string id: %s", queryResult.get(1)));
		}
		if (!(queryResult.get(2) instanceof Timestamp) || !(queryResult.get(3) instanceof Timestamp)) {
			throw new PersistenceRetrievalException(String.format("The query-result contains a non-timestamp time. Start: %s, End: %s", queryResult.get(2), queryResult.get(3)));
		}
		Instant startTime = ((Timestamp) queryResult.get(2)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		Instant endTime = ((Timestamp) queryResult.get(3)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		return new EntityIdTempResults((String) queryResult.get(1), startTime, endTime);
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

	public List<String> findAttributesByEntityIds(List<String> entityIds, String timeQueryPart) {
		TypedQuery<String> getAttributeIdsQuery =
				entityManager.createQuery(
						"Select distinct attribute.id " +
								"from Attribute attribute " +
								"where attribute.entityId in :entityIds " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								timeQueryPart, String.class);
		getAttributeIdsQuery.setParameter("entityIds", entityIds);
		return getAttributeIdsQuery.getResultList();
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

	private List<Attribute> findAttributeInstancesForEntities(List<String> entityIds, String attributeId, String timeQueryPart, Integer lastN) {
		TypedQuery<Attribute> getAttributeInstancesQuery =
				entityManager.createQuery(
						"Select attribute " +
								"from Attribute attribute " +
								"where attribute.entityId in :entityIds " +
								"and attribute.id=:attributeId " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								timeQueryPart +
								"order by attribute.ts desc", Attribute.class);
		getAttributeInstancesQuery.setParameter("entityIds", entityIds);
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
	 * @return list of attribute ID's
	 */
	private List<String> findAllAttributesForEntity(String entityId, String timeQueryPart) {
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

	private List<String> findAllAttributesForEntities(List<String> entityIds, String timeQueryPart, GeoQuery geoQuery) {
		TypedQuery<String> getAttributeIdsQuery =
				entityManager.createQuery(
						"Select distinct attribute.id " +
								"from Attribute attribute " +
								"where attribute.entityId in :entityIds " +
								"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
								timeQueryPart, String.class);
		getAttributeIdsQuery.setParameter("entityIds", entityIds);
		return getAttributeIdsQuery.getResultList();
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
