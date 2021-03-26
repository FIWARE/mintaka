package org.fiware.mintaka.persistence;

import org.fiware.mintaka.domain.EntityIdTempResults;
import org.fiware.mintaka.domain.PaginationInformation;
import org.fiware.mintaka.domain.query.geo.GeoQuery;
import org.fiware.mintaka.domain.query.ngsi.QueryTerm;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Interface for entity retrieval
 */
public interface EntityRepository {

	/**
	 * Retrieve the entity from the db.
	 *
	 * @param entityId id of the entity to retrieve
	 * @return optional entity
	 */
	Optional<NgsiEntity> findById(String entityId, TimeQuery timeQuery);

	/**
	 * Find all attributes of an entity in the define timeframe
	 *
	 * @param entityId   id to get attributes for
	 * @param timeQuery  time related query
	 * @param attributes the attributes to be included, if null or empty return all
	 * @return list of attribute instances
	 */
	LimitableResult<List<Attribute>> findAttributeByEntityId(String entityId, TimeQuery timeQuery, List<String> attributes, Integer limit, boolean backwards);

	/**
	 * Get the limit to be used for the given configuration
	 *
	 * @param entitiesNumber   - entities to be returned
	 * @param attributesNumber - attributes to be returned per entity
	 * @param lastN            - number of last values to be returned
	 * @return the limit to apply
	 */
	int getLimit(int entitiesNumber, int attributesNumber, Integer lastN);

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
	List<EntityIdTempResults> findEntityIdsAndTimeframesByQuery(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm, int pageSize, Optional<String> anchor);

	/**
	 * Count the number of entityId results for the given query
	 *
	 * @param idPattern pattern to check ids
	 * @param types     types to include
	 * @param timeQuery timeframe definition
	 * @param geoQuery  geo related query
	 * @param queryTerm ngsi query
	 * @return number of matching entities
	 */
	Number getCount(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm);

	/**
	 * Get the pagination information for the given query.
	 */
	PaginationInformation getPaginationInfo(Optional<List<String>> idList, Optional<String> idPattern, List<String> types, TimeQuery timeQuery, Optional<GeoQuery> geoQuery, Optional<QueryTerm> queryTerm, int pageSize, Optional<String> anchor);

	/**
	 * Return all sub attribute instances for the given attribute instance
	 *
	 * @param entityId            entity the attributes and subattributes are connected to
	 * @param attributeInstanceId id of the concrete attribute
	 * @param limit               number of instances to be retrieved
	 * @param backwards           should the instances be retrieved in reversed order.
	 * @return list of subattribute instances
	 */
	List<SubAttribute> findSubAttributeInstancesForAttributeAndEntity(String entityId, String attributeInstanceId, int limit, boolean backwards);

	/**
	 * Find all attributes for the given entities in that timequery.
	 *
	 * @param entityIds entities to be associated with the attributes
	 * @param timeQueryPart timeframe to search for
	 * @return list of attributes
	 */
	List<String> findAttributesByEntityIds(List<String> entityIds, String timeQueryPart);

	/**
	 * Get the list of timestamps that have opMode "create" for the given attribute.
	 *
	 * @param attributeId    id of the attribute to find the timestamps for
	 * @param entityId       id of the entity that are connected with the attribute
	 * @param isSubAttribute is the requestend attribute an attribute or a subattribute
	 * @return the list of timestamps
	 */
	List<Instant> getCreatedAtForAttribute(String attributeId, String entityId, boolean isSubAttribute);
}
