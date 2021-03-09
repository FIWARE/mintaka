package org.fiware.mintaka.persistence;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.domain.EntityIdTempResults;
import org.fiware.mintaka.domain.query.geo.GeoQuery;
import org.fiware.mintaka.domain.query.ngsi.*;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;

import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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

	private static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.ss");

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

	/**
	 * Query for timeframe and entityIds where all query elements are fulfilled.
	 *
	 * @param idPattern pattern to check ids
	 * @param types     types to include
	 * @param timeQuery timeframe definition
	 * @param geoQuery  geo related query
	 * @param queryTerm ngsi query
	 * @return the list of entityIds and there timeframes
	 */
	public List<EntityIdTempResults> findEntityIdsAndTimeframesByQuery(Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm) {
		Optional<String> querySelectString = queryTerm.map(query -> createSelectionCriteriaFromQueryTerm(idPattern, types, timeQuery, query));
		Optional<String> geoSelectString = Optional.empty();
		if (geoQuery.isPresent()) {
			geoSelectString = Optional.of((createSelectionCriteria(idPattern, types, timeQuery, geoQuery, Optional.empty())));
		}

		String finalSelect = "";
		if (querySelectString.isPresent() && geoSelectString.isPresent()) {
			// query for geography AND ngsi-attributes
			finalSelect = selectAndTerm(querySelectString.get(), geoSelectString.get());
		} else if (querySelectString.isPresent()) {
			// query only with ngsi query
			finalSelect = querySelectString.get();
		} else if (geoSelectString.isPresent()) {
			// query only with geography
			finalSelect = geoSelectString.get();
		} else {
			// query only with basic parameters(id, type, time)
			finalSelect = createSelectionCriteria(idPattern, types, timeQuery, Optional.empty(), Optional.empty());
		}
		log.debug("Final select:  {}", finalSelect);
		Query getIdsAndTimeRangeQuery = entityManager.createNativeQuery(finalSelect);

		List<Object[]> queryResultList = getIdsAndTimeRangeQuery.getResultList();

		return queryResultList.stream()
				.map(Arrays::asList)
				.filter(queryResult -> queryResult.size() == 4)
				.filter(queryResult -> (Boolean) queryResult.get(0))
				.map(this::mapQueryResultToPojo)
				.collect(Collectors.toList());
	}

	/**
	 * Map a query result of form result::boolean-entityId::string-startTime::timestamp-endTime::timestamp to a temporary result object.
	 */
	private EntityIdTempResults mapQueryResultToPojo(List<Object> queryResult) {
		if (!(queryResult.get(1) instanceof String)) {
			throw new PersistenceRetrievalException(String.format("The query-result contains a non-string id: %s", queryResult.get(0)));
		}
		if (!(queryResult.get(2) instanceof Timestamp) || !(queryResult.get(3) instanceof Timestamp)) {
			throw new PersistenceRetrievalException(String.format("The query-result contains a non-timestamp time. Start: %s, End: %s", queryResult.get(2), queryResult.get(3)));
		}
		Instant startTime = ((Timestamp) queryResult.get(2)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		Instant endTime = ((Timestamp) queryResult.get(3)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		return new EntityIdTempResults((String) queryResult.get(1), startTime, endTime);
	}

	/**
	 * Return all sub attribute instances for the given attribute instance
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

	/**
	 * Create the sql selection criteria, based on the QueryTerm, while including all additional parameters. Geoqueries are handle as a seperate
	 * query term.
	 */
	private String createSelectionCriteriaFromQueryTerm(Optional<String> idPattern, List<String> types, TimeQuery timeQuery, QueryTerm queryTerm) {
		log.debug("Build from term {}.", queryTerm);
		if (queryTerm instanceof LogicalTerm) {
			Optional<String> tableA = Optional.empty();
			Optional<LogicalOperator> operator = Optional.empty();
			LogicalTerm logicalTerm = (LogicalTerm) queryTerm;
			for (QueryTerm subTerm : logicalTerm.getSubTerms()) {
				log.debug("Subterm: {}", subTerm);
				// if tableA and operator are present, the current term is the second part of a logical query -> combine according to the operator
				if (tableA.isPresent() && operator.isPresent()) {
					switch (operator.get()) {
						case OR:
							tableA = Optional.of(selectOrTerm(tableA.get(), createSelectionCriteriaFromQueryTerm(idPattern, types, timeQuery, subTerm)));
							operator = Optional.empty();
							continue;
						case AND:
							tableA = Optional.of(selectAndTerm(tableA.get(), createSelectionCriteriaFromQueryTerm(idPattern, types, timeQuery, subTerm)));
							operator = Optional.empty();
							continue;
						default:
							throw new IllegalArgumentException(String.format("Cannot build criteria for operator %s.", operator.get()));
					}
				}
				// if tabelA is empty, set the current comparison as tableA
				if (subTerm instanceof ComparisonTerm && tableA.isEmpty()) {
					tableA = Optional.of(createSelectionCriteria(idPattern, types, timeQuery, Optional.empty(), Optional.of((ComparisonTerm) subTerm)));
					continue;
				}
				// set the operator to be used for the next logical connection
				if (subTerm instanceof LogicalConnectionTerm) {
					operator = Optional.of(((LogicalConnectionTerm) subTerm).getOperator());
					continue;
				}
				// if the first term is a logical term, evaluate it and set as tableA
				if (subTerm instanceof LogicalTerm && tableA.isEmpty()) {
					tableA = Optional.of(createSelectionCriteriaFromQueryTerm(idPattern, types, timeQuery, subTerm));
					continue;
				}
			}
			// return the subselect to get the queried table
			return tableA.get();
		} else if (queryTerm instanceof ComparisonTerm) {
			return createSelectionCriteria(idPattern, types, timeQuery, Optional.empty(), Optional.of(((ComparisonTerm) queryTerm)));
		}
		throw new IllegalArgumentException(String.format("Cannot build criteria from given term: %s", queryTerm));
	}

	/**
	 * Create the full sql selection criteria.
	 */
	private String createSelectionCriteria(Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<ComparisonTerm> comparisonTerm) {
		String timeQueryPart = timeQuery.getSqlRepresentation();

		// The query:
		// find all entityIds that match the pattern, type and are not deleted
		// then get all attributes according to geo and comparison query
		// then sort by entity-id and timestamp and combine consecutive rows with distinct query results into single rows

		String idSubSelect = "(SELECT DISTINCT entity.id FROM entities entity WHERE entity.opMode != '" + OpMode.Delete.name() + "' ";
		if (!types.isEmpty()) {
			idSubSelect += " AND entity.type in (" + types.stream().map(type -> String.format("'%s'", type)).collect(Collectors.joining(",")) + ")";
		}

		if (idPattern.isPresent()) {
			idSubSelect += " AND entity.id ~ '" + idPattern.get() + "' ";
		}
		idSubSelect += ")";
		log.debug("Subselect: {}", idSubSelect);


		String selectTempTable = "(SELECT ";
		if (geoQuery.isPresent()) {
			selectTempTable += geoQuery.get().toSQLQuery() + " as result";
		} else if (comparisonTerm.isPresent()) {
			selectTempTable += comparisonTerm.get().toSQLQuery() + " as result";
		} else {
			selectTempTable += " true as result";
		}
		//TODO: at query result
		//if(query.isPresent()){
		//innerJoin += query.get().getSqlRepresentation() + "as queryResult";
		//}
		selectTempTable += ",attribute." + timeQuery.getTimeProperty() + " as time, attribute.entityId as entityId " +
				"FROM attributes attribute WHERE attribute.entityId in (" + idSubSelect + ") ";
		if (comparisonTerm.isPresent()) {
			selectTempTable += "and attribute.id='" + comparisonTerm.get().getAttributePath() + "' ";
		}
		if (geoQuery.isPresent()) {
			selectTempTable += "and attribute.id='" + geoQuery.get().getGeoProperty() + "' ";
		}

		LocalDateTime ldtTimeAt = LocalDateTime.ofInstant(timeQuery.getTimeAt(), ZoneOffset.UTC);

		String timeEnd;
		String timeAt;
		switch (timeQuery.getTimeRelation()) {
			case AFTER:
				timeEnd = "now()";
				timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
				break;
			case BEFORE:
				timeEnd = ldtTimeAt.minus(1, ChronoUnit.MILLIS).format(LOCAL_DATE_TIME_FORMATTER);
				timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
				break;
			case BETWEEN:
				timeEnd = LocalDateTime.ofInstant(timeQuery.getEndTime(), ZoneOffset.UTC).minus(1, ChronoUnit.MILLIS).format(LOCAL_DATE_TIME_FORMATTER);
				timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
				break;
			default:
				throw new InvalidTimeRelationException(String.format("Requested timerelation was not valid: %s", timeQuery));
		}
		// select the last known value before time at, in order to get the attribute state at that time
		String selectLastBefore = "(SELECT last(lastBefore.result,lastBefore.time) as result, '" + timeAt + "' as time, lastBefore.entityId as entityId FROM" + selectTempTable + ") as lastBefore WHERE time<='" + timeAt + "' GROUP BY lastBefore.entityId)";
		String selectLastIn = "(SELECT last(lastIn.result,lastIn.time) as result, '" + timeEnd + "' as time, lastIn.entityId as entityId FROM" + selectTempTable + ") as lastIn WHERE time<='" + timeEnd + "' GROUP BY lastIn.entityId)";

		selectTempTable += timeQueryPart + " order by attribute.entityId, attribute." + timeQuery.getTimeProperty();
		log.debug("Select temp table: {}", selectTempTable);

		// a usual result set will look similar to:
		// id1, false, t0 - set1
		// id1, true, t1 - set2
		// id1, true, t2 - set2
		// id1, false, t3 - set3
		// id1, false, t4 - set3
		// id2, true, t0 - set4
		// id2, true, t1 - set4
		// id2, false, t2 - set5
		// id2, false, t3 - set5
		// the changed order assures that the entities of the same set are moved by the same distance and therefore get the same setId.
		String tempWithSetId = "SELECT *, (ROW_NUMBER() OVER (ORDER BY temp.entityId, temp.time)) - (ROW_NUMBER() OVER (ORDER BY temp.entityId, temp.result, temp.time)) as setId FROM  (" + selectTempTable + ") UNION " + selectLastBefore + " UNION " + selectLastIn + ") as temp";

		String selectOnTemp = "SELECT t1.result, t1.entityId,MIN(t1.time) AS startTime, MAX(t1.time) AS endTime FROM (" + tempWithSetId + ") AS t1 WHERE result=true GROUP BY t1.result,t1.entityId,t1.setId";

		log.debug("Final query: {}", selectOnTemp);
		return selectOnTemp;
	}

	/**
	 * Combine two subtables with AND-logic, e.g. filter out all none overlapping entries and merge the overlapping to there intersection
	 */
	private String selectAndTerm(String tableA, String tableB) {
		String selectAnd = "SELECT a.result as result, a.entityId as entityId, GREATEST(a.startTime,b.startTime) as startTime, LEAST(a.endTime, b.endTime) as endTime FROM (" + tableA + ") as a, (" + tableB + ") as b WHERE " +
				"a.entityId=b.entityId " +
				"AND ((a.startTime between b.startTime and b.endTime) " +
				"OR (a.endTime between b.startTime and b.endTime) " +
				"OR (b.startTime between a.startTime and a.endTime) " +
				"OR (b.endTime between a.startTime and a.endTime)) ";
		log.debug("Select and: {}", selectAnd);
		return selectAnd;
	}

	/**
	 * Combine two subtables with OR-logic, e.g. combine all overlapping entires to there union and add all non-overlapping timerframes.
	 */
	private String selectOrTerm(String tableA, String tableB) {
		// select all overlapping frames an merge them
		String selectOverlapping = "SELECT a.result as result, a.entityId as entityId, LEAST(a.startTime,b.startTime) as startTime, GREATEST(a.endTime, b.endTime) as endTime FROM (" + tableA + ") as a, (" + tableB + ") as b WHERE " +
				"a.entityId=b.entityId " +
				"AND ((a.startTime between b.startTime and b.endTime) " +
				"OR (a.endTime between b.startTime and b.endTime) " +
				"OR (b.startTime between a.startTime and a.endTime) " +
				"OR (b.endTime between a.startTime and a.endTime)) ";

		String selectNonOverlappingA = "SELECT a.result, a.entityId, a.startTime, a.endTime  FROM (" + tableA + ") as a, (" + tableB + ") as b WHERE " +
				"a.entityId=b.entityId " +
				"AND NOT ((a.startTime between b.startTime and b.endTime) " +
				"OR (a.endTime between b.startTime and b.endTime) " +
				"OR (b.startTime between a.startTime and a.endTime) " +
				"OR (b.endTime between a.startTime and a.endTime)) ";

		String selectNonOverlappingB = "SELECT b.result, b.entityId, b.startTime, b.endTime  FROM (" + tableA + ") as a, (" + tableB + ") as b WHERE " +
				"a.entityId=b.entityId " +
				"AND NOT ((a.startTime between b.startTime and b.endTime) " +
				"OR (a.endTime between b.startTime and b.endTime) " +
				"OR (b.startTime between a.startTime and a.endTime) " +
				"OR (b.endTime between a.startTime and a.endTime)) ";

		String selectBoth = "(" + selectOverlapping + ") UNION (" + selectNonOverlappingA + ") UNION (" + selectNonOverlappingB + ")";
		log.debug("Select and: {}", selectBoth);
		return selectBoth;
	}

}
