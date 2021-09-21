package org.fiware.mintaka.persistence;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.domain.EntityIdTempResults;
import org.fiware.mintaka.domain.PaginationInformation;
import org.fiware.mintaka.domain.query.geo.GeoQuery;
import org.fiware.mintaka.domain.query.ngsi.*;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;

import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.fiware.mintaka.domain.query.ngsi.ComparisonTerm.YEAR_MONTH_DAY_FORMAT;

/**
 * Repository implementation for retrieving temporal entity representations from the timescale database
 */
@Slf4j
@Singleton
@RequiredArgsConstructor
public class TimescaleBackedEntityRepository implements EntityRepository {

	private static final int TOTAL_MAX_NUMBER_OF_INSTANCES = 1000;
	private static final int DEFAULT_NUM_ATTRIBUTES = 10;
	public static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(YEAR_MONTH_DAY_FORMAT + " hh:mm:ss.ss");
	public static final int EXPECTED_RESULT_SIZE = 4;

	private final EntityManager entityManager;

	@Override
	public Optional<NgsiEntity> findById(String entityId, TimeQuery timeQuery, List<String> aggregationMethods, Optional<String> aggregationPeriod) {
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

		if (!aggregationMethods.isEmpty()) {
			// TODO: use aggregation period
			String aggregatedSelect = "SELECT time_bucket('5 minutes', ts) as time_bucket, "
					+ aggregationMethods.stream().map(agMethod -> mapAggregationMethodToSql(agMethod, "*")).collect(Collectors.joining(", ")) +
					" from entities where  " + wherePart + " group by time_bucket order by time_bucket desc";
			Query aggregatedQuery = entityManager.createNativeQuery(aggregatedSelect);
			aggregatedQuery.setParameter("id", entityId);
			List aggregationResult = aggregatedQuery.getResultList();
		}


		List<NgsiEntity> ngsiEntityList = getNgsiEntitiesQuery.getResultList();
		// only return the entity if its not deleted.
		return ngsiEntityList.stream().findFirst().filter(ngsiEntity -> ngsiEntity.getOpMode() != OpMode.Delete);
	}

	@Override
	public LimitableResult<List<Attribute>> findAttributeByEntityId(String entityId, TimeQuery timeQuery, List<String> attributes, Integer limit, boolean backwards, List<String> aggregationMethods, Optional<String> aggregationPeriod) {
		AtomicBoolean isLimited = new AtomicBoolean(false);
		String timeQueryPart = timeQuery.getSqlRepresentation();
		if (attributes == null || attributes.isEmpty()) {
			attributes = findAllAttributesForEntity(entityId, timeQueryPart);
		}
		// if there still is nothing, return
		if (attributes.isEmpty()) {
			return new LimitableResult<>(List.of(), false);
		}

		// we need to do single queries in order to fullfil the "lastN" parameter.
		List<Attribute> attributeInstance = attributes.stream()
				.flatMap(attributeId -> {
					List<Attribute> instances = findAttributeInstancesForEntity(entityId, attributeId, timeQueryPart, timeQuery.getDBTimeField(), backwards, limit, aggregationMethods, aggregationPeriod);
					if (instances.size() == limit) {
						// Resultset was most probably limited. In an edge case the not limited number of attributes will exactly match the number of retrieved
						// instances. Since it would be expensive to differ that case(e.g. an additional query would be required), we accept that.
						isLimited.set(true);
					}
					return instances.stream();
				})
				.collect(Collectors.toList());
		return new LimitableResult<>(attributeInstance, isLimited.get());
	}

	private boolean isBackwards(Integer lastN) {
		return lastN != null && lastN >= 0;
	}

	@Override
	public int getLimit(int entitiesNumber, int attributesNumber, Integer lastN) {
		if (entitiesNumber == 0) {
			// nothing to apply here.
			return entitiesNumber;
		}
		if (attributesNumber == 0) {
			// means we dont know the actual number, so we assume one
			attributesNumber = DEFAULT_NUM_ATTRIBUTES;
		}

		if (lastN != null) {
			// in case of lastN, we can make an assumption
			int expectedInstances = lastN * attributesNumber * entitiesNumber;
			if (expectedInstances > TOTAL_MAX_NUMBER_OF_INSTANCES) {
				// implicit cast to int will cut everything after the comma -> will keep total number below max
				return TOTAL_MAX_NUMBER_OF_INSTANCES / (attributesNumber * entitiesNumber);
			} else {
				return lastN;
			}
		}
		if (attributesNumber * entitiesNumber < TOTAL_MAX_NUMBER_OF_INSTANCES) {
			// implicit cast to int will cut everything after the comma -> will keep total number below max
			return TOTAL_MAX_NUMBER_OF_INSTANCES / (attributesNumber * entitiesNumber);
		}
		// we have at least {@link TOTAL_MAX_NUMBER_OF_INSTANCES} attributes, only return one instance per attribute
		return 1;
	}

	private String mapAggregationMethodToSql(String aggregationMethod, String attributeToAggregate) {
		switch (aggregationMethod) {
			case "totalCount":
				return String.format("count(%s)", attributeToAggregate);
			case "distinctCount":
				return String.format("count(distinct %s)", attributeToAggregate);
			case "sum":
				return String.format("sum(%s)", attributeToAggregate);
			case "avg":
				//TODO: check with etsi in regards of  a timeweighted average
				return String.format("avg(%s)", attributeToAggregate);
			case "min":
				return String.format("min(%s)", attributeToAggregate);
			case "max":
				return String.format("max(%s)", attributeToAggregate);
			case "stddev":
			case "sumsq":
			default:
				throw new IllegalArgumentException(String.format("%s is not a supported aggregation method.", aggregationMethod));
		}
	}

	@Override
	public List<EntityIdTempResults> findEntityIdsAndTimeframesByQuery(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm, int pageSize, Optional<String> anchor) {
		String finalSelect = getQuerySelect(idList, idPattern, types, timeQuery, geoQuery, queryTerm);

		String limitedSelect = "SELECT  * FROM (" + finalSelect + ") as final WHERE entityId IN (SELECT DISTINCT entityID FROM (" + finalSelect + ") as fs";
		if (anchor.isPresent()) {
			limitedSelect += " WHERE entityID>='" + anchor.get() + "' ORDER BY entityID LIMIT " + pageSize + ")";
		} else {
			limitedSelect += " ORDER BY entityID LIMIT " + pageSize + ")";
		}
		Query getIdsAndTimeRangeQuery = entityManager.createNativeQuery(limitedSelect);
		log.debug("Final select:  {}", limitedSelect);


		List<Object[]> queryResultList = getIdsAndTimeRangeQuery.getResultList();

		return queryResultList.stream()
				.map(Arrays::asList)
				.filter(queryResult -> queryResult.size() == EXPECTED_RESULT_SIZE)
				.filter(queryResult -> (Boolean) queryResult.get(0))
				.map(this::mapQueryResultToPojo)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}

	@Override
	public Number getCount(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm) {

		String finalSelect = getQuerySelect(idList, idPattern, types, timeQuery, geoQuery, queryTerm);

		String countSelect = "SELECT COUNT(DISTINCT entityID) FROM (" + finalSelect + ") as fs";
		Query getCount = entityManager.createNativeQuery(countSelect);
		// if no count is returned, the select did not find anything -> return a count of 0
		return Optional.ofNullable(getCount.getResultList().get(0)).map(res -> (Number) res).orElse(0);
	}

	@Override
	public PaginationInformation getPaginationInfo(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm, int pageSize, Optional<String> anchor) {

		String finalSelect = getQuerySelect(idList, idPattern, types, timeQuery, geoQuery, queryTerm);
		String selectDistinctIds = "SELECT DISTINCT entityID as entityID, ROW_NUMBER() OVER (ORDER BY entityID) as row_number  FROM (" + finalSelect + ") as fs ORDER BY entityID";
		String selectCurrentRow = anchor.map(a -> "SELECT row_number FROM (" + selectDistinctIds + ") as distinctTable WHERE entityId='" + a + "'").orElse("1");
		String nextSelect = "SELECT entityID FROM (" + selectDistinctIds + ") as distinctTable WHERE row_number=((" + selectCurrentRow + ")+" + pageSize + ")";
		String previousSelect = "SELECT entityID FROM (" + selectDistinctIds + ") as distinctTable WHERE row_number=((" + selectCurrentRow + ")-" + pageSize + ")";

		log.debug("Next id: {}", nextSelect);
		log.debug("Previous id: {}", previousSelect);

		Query nextQuery = entityManager.createNativeQuery(nextSelect);
		Query previousQuery = entityManager.createNativeQuery(previousSelect);

		return new PaginationInformation(
				pageSize,
				getSingleQueryResult(previousQuery).filter(pio -> pio instanceof String).map(pio -> (String) pio),
				getSingleQueryResult(nextQuery).filter(nio -> nio instanceof String).map(nio -> (String) nio));

	}

	private Optional<Object> getSingleQueryResult(Query query) {
		try {
			return Optional.ofNullable(query.getSingleResult());
		} catch (NoResultException e) {
			return Optional.empty();
		} catch (RuntimeException e) {
			throw new PersistenceRetrievalException(String.format("Was not able to get single result from query: %s", query), e);
		}
	}

	private String getQuerySelect(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm) {
		Optional<String> querySelectString = queryTerm.map(query -> createSelectionCriteriaFromQueryTerm(idList, idPattern, types, timeQuery, query));
		Optional<String> geoSelectString = Optional.empty();
		if (geoQuery.isPresent()) {
			geoSelectString = Optional.of((createSelectionCriteria(idList, idPattern, types, timeQuery, geoQuery, Optional.empty())));
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
			finalSelect = createSelectionCriteria(idList, idPattern, types, timeQuery, Optional.empty(), Optional.empty());
		}
		return finalSelect;
	}

	/**
	 * Map a query result of form result::boolean-entityId::string-startTime::timestamp-endTime::timestamp to a temporary result object.
	 */
	private Optional<EntityIdTempResults> mapQueryResultToPojo(List<Object> queryResult) {
		if (!(queryResult.get(1) instanceof String)) {
			throw new PersistenceRetrievalException(String.format("The query-result contains a non-string id: %s", queryResult.get(0)));
		}
		if (!(queryResult.get(2) instanceof Timestamp) || !(queryResult.get(3) instanceof Timestamp)) {
			log.info("The query-result contains a non-timestamp time. Start: {}, End: {}", queryResult.get(2), queryResult.get(3));
			return Optional.empty();
		}
		Instant startTime = ((Timestamp) queryResult.get(2)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		Instant endTime = ((Timestamp) queryResult.get(3)).toLocalDateTime().toInstant(ZoneOffset.UTC);
		return Optional.of(new EntityIdTempResults((String) queryResult.get(1), startTime, endTime));
	}

	@Override
	public List<SubAttribute> findSubAttributeInstancesForAttributeAndEntity(String entityId, String attributeInstanceId, int limit, boolean backwards) {
		String query = "Select subAttribute " +
				"from SubAttribute subAttribute " +
				"where subAttribute.entityId=:entityId " +
				"and subAttribute.attributeInstanceId=:attributeInstanceId " +
				// TODO: add back when opMode is added to subAttributes table
//								"and subAttribute.opMode!='" + OpMode.Delete.name() + "' " +
				"order by subAttribute.ts ";
		if (backwards) {
			query += "desc";
		} else {
			query += "asc";
		}

		TypedQuery<SubAttribute> getSubAttributeInstancesQuery =
				entityManager.createQuery(query, SubAttribute.class);
		getSubAttributeInstancesQuery.setParameter("entityId", entityId);
		getSubAttributeInstancesQuery.setParameter("attributeInstanceId", attributeInstanceId);
		getSubAttributeInstancesQuery.setMaxResults(limit);

		return getSubAttributeInstancesQuery.getResultList();
	}

	@Override
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
	 * @param backwards     if the instances should be retrieved starting with the newest
	 * @return list of attribute instances
	 */
	private List<Attribute> findAttributeInstancesForEntity(String entityId, String attributeId, String timeQueryPart, String timeProperty, boolean backwards, Integer limit, List<String> aggregationMethods, Optional<String> aggregationPeriod) {
		if (!aggregationMethods.isEmpty()) {
			String aggregatedQuery = String.format("Select time_bucket('5 minutes', attribute.%s) as time_bucket, avg(number) " +
					"from attributes as attribute " +
					"where attribute.entityId=:entityId " +
					"and attribute.id=:attributeId " +
					"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
					timeQueryPart +
					"group by time_bucket order by time_bucket", timeProperty, timeProperty);
			Query aggregationQuery = entityManager.createNativeQuery(aggregatedQuery);
			aggregationQuery.setParameter("entityId", entityId);
			aggregationQuery.setParameter("attributeId", attributeId);
			List aggregationResult = aggregationQuery.getResultList();
		}

		String selectionQuery = "Select attribute " +
				"from Attribute attribute " +
				"where attribute.entityId=:entityId " +
				"and attribute.id=:attributeId " +
				"and attribute.opMode!='" + OpMode.Delete.name() + "' " +
				timeQueryPart;
		if (backwards) {
			// retrieve backwards
			selectionQuery += String.format("order by attribute.%s desc", timeProperty);
		} else {
			// retrieve forward
			selectionQuery += String.format("order by attribute.%s asc", timeProperty);
		}

		TypedQuery<Attribute> getAttributeInstancesQuery = entityManager.createQuery(selectionQuery, Attribute.class);
		getAttributeInstancesQuery.setParameter("entityId", entityId);
		getAttributeInstancesQuery.setParameter("attributeId", attributeId);
		getAttributeInstancesQuery.setMaxResults(limit);
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

	@Override
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
	private String createSelectionCriteriaFromQueryTerm(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, QueryTerm queryTerm) {
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
							tableA = Optional.of(selectOrTerm(tableA.get(), createSelectionCriteriaFromQueryTerm(idList, idPattern, types, timeQuery, subTerm)));
							operator = Optional.empty();
							continue;
						case AND:
							tableA = Optional.of(selectAndTerm(tableA.get(), createSelectionCriteriaFromQueryTerm(idList, idPattern, types, timeQuery, subTerm)));
							operator = Optional.empty();
							continue;
						default:
							throw new IllegalArgumentException(String.format("Cannot build criteria for operator %s.", operator.get()));
					}
				}
				// if tabelA is empty, set the current comparison as tableA
				if (subTerm instanceof ComparisonTerm && tableA.isEmpty()) {
					tableA = Optional.of(createSelectionCriteria(idList, idPattern, types, timeQuery, Optional.empty(), Optional.of((ComparisonTerm) subTerm)));
					continue;
				}
				// set the operator to be used for the next logical connection
				if (subTerm instanceof LogicalConnectionTerm) {
					operator = Optional.of(((LogicalConnectionTerm) subTerm).getOperator());
					continue;
				}
				// if the first term is a logical term, evaluate it and set as tableA
				if (subTerm instanceof LogicalTerm && tableA.isEmpty()) {
					tableA = Optional.of(createSelectionCriteriaFromQueryTerm(idList, idPattern, types, timeQuery, subTerm));
					continue;
				}
			}
			// return the subselect to get the queried table
			if (tableA.isPresent()) {
				return tableA.get();
			}
		} else if (queryTerm instanceof ComparisonTerm) {
			return createSelectionCriteria(idList, idPattern, types, timeQuery, Optional.empty(), Optional.of(((ComparisonTerm) queryTerm)));
		}
		throw new IllegalArgumentException(String.format("Cannot build criteria from given term: %s", queryTerm));
	}

	/**
	 * Create the full sql selection criteria.
	 */
	private String createSelectionCriteria(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<ComparisonTerm> comparisonTerm) {
		String timeQueryPart = timeQuery.getSqlRepresentation();

		// The query:
		// find all entityIds that match the pattern, type and are not deleted
		// then get all attributes according to geo and comparison query
		// then sort by entity-id and timestamp and combine consecutive rows with distinct query results into single rows

		String idSubSelect = "(SELECT DISTINCT entity.id FROM entities entity WHERE entity.opMode != '" + OpMode.Delete.name() + "' ";
		if (!types.isEmpty()) {
			idSubSelect += " AND entity.type in (" + types.stream().map(type -> String.format("'%s'", type)).collect(Collectors.joining(",")) + ")";
		}

		if (idList.isPresent()) {
			idSubSelect += " AND entity.id IN (" + idList.get().stream().map(idString -> String.format("'%s'", idString)).collect(Collectors.joining(",")) + ")";
		} else if (idPattern.isPresent()) {
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

		selectTempTable += ",attribute." + timeQuery.getDBTimeField() + " as time, attribute.entityId as entityId " +
				"FROM attributes attribute WHERE attribute.entityId in (" + idSubSelect + ") ";
		if (comparisonTerm.isPresent()) {
			selectTempTable += "and attribute.id='" + comparisonTerm.get().getAttributePath() + "' ";
		}
		if (geoQuery.isPresent()) {
			selectTempTable += "and attribute.id='" + geoQuery.get().getGeoProperty() + "' ";
		}


		String selectLastBefore;
		String selectLastIn;
		String tempWithSetId;
		selectTempTable += timeQueryPart + " order by attribute.entityId, attribute." + timeQuery.getDBTimeField();
		if (timeQuery.getTimeAt() != null) {
			LocalDateTime ldtTimeAt = LocalDateTime.ofInstant(timeQuery.getTimeAt(), ZoneOffset.UTC);

			String timeEnd;
			String timeAt;
			switch (timeQuery.getTimeRelation()) {
				case AFTER:
					timeEnd = "now()";
					timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
					break;
				case BEFORE:
					timeEnd = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
					timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
					break;
				case BETWEEN:
					timeEnd = LocalDateTime.ofInstant(timeQuery.getEndTime(), ZoneOffset.UTC).format(LOCAL_DATE_TIME_FORMATTER);
					timeAt = ldtTimeAt.format(LOCAL_DATE_TIME_FORMATTER);
					break;
				default:
					throw new InvalidTimeRelationException(String.format("Requested timerelation was not valid: %s", timeQuery));
			}
			// select the last known value before time at, in order to get the attribute state at that time
			selectLastBefore = "(SELECT last(lastBefore.result,lastBefore.time) as result, '" + timeAt + "' as time, lastBefore.entityId as entityId FROM" + selectTempTable + ") as lastBefore WHERE time<'" + timeAt + "' GROUP BY lastBefore.entityId)";
			selectLastIn = "(SELECT last(lastIn.result,lastIn.time) as result, '" + timeEnd + "' as time, lastIn.entityId as entityId FROM" + selectTempTable + ") as lastIn WHERE time<'" + timeEnd + "' GROUP BY lastIn.entityId)";
			tempWithSetId = "SELECT *, (ROW_NUMBER() OVER (ORDER BY tempTable.entityId, tempTable.time)) - (ROW_NUMBER() OVER (ORDER BY tempTable.entityId, tempTable.result, tempTable.time)) as setId FROM  (" + selectTempTable + ") UNION " + selectLastBefore + " UNION " + selectLastIn + ") as tempTable";
		} else {
			tempWithSetId = "SELECT *, (ROW_NUMBER() OVER (ORDER BY tempTable.entityId, tempTable.time)) - (ROW_NUMBER() OVER (ORDER BY tempTable.entityId, tempTable.result, tempTable.time)) as setId FROM  " + selectTempTable + ") as tempTable";
		}

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
