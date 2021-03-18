package org.fiware.mintaka.rest;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.context.ServerRequestContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.AcceptType;
import org.fiware.mintaka.domain.ApiDomainMapper;
import org.fiware.mintaka.domain.PaginationInformation;
import org.fiware.mintaka.domain.query.geo.GeoQuery;
import org.fiware.mintaka.domain.query.geo.Geometry;
import org.fiware.mintaka.domain.query.ngsi.QueryParser;
import org.fiware.mintaka.domain.query.ngsi.QueryTerm;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.fiware.mintaka.domain.query.temporal.TimeStampType;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;
import org.fiware.mintaka.persistence.LimitableResult;
import org.fiware.mintaka.service.EntityTemporalService;
import org.fiware.ngsi.api.TemporalRetrievalApi;
import org.fiware.ngsi.model.EntityTemporalListVO;
import org.fiware.ngsi.model.EntityTemporalVO;
import org.fiware.ngsi.model.GeoPropertyVO;
import org.fiware.ngsi.model.PropertyVO;
import org.fiware.ngsi.model.RelationshipVO;
import org.fiware.ngsi.model.TimerelVO;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation of the NGSI-LD temporal retrieval api
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class TemporalApiController implements TemporalRetrievalApi {

	public static final List<String> WELL_KNOWN_ATTRIBUTES = List.of("location", "observationSpace", "operationSpace", "unitCode");

	private static final Integer DEFAULT_LIMIT = 100;
	private static final String CONTENT_RANGE_HEADER_KEY = "Content-Range";
	private static final String DEFAULT_TIME_PROPERTY = "observedAt";
	private static final String DEFAULT_GEO_PROPERTY = "location";
	private static final String SYS_ATTRS_OPTION = "sysAttrs";
	private static final String TEMPORAL_VALUES_OPTION = "temporalValues";
	private static final String COUNT_OPTION = "count";
	private static final String LINK_HEADER_TEMPLATE = "<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"";
	public static final String COMMA_SEPERATOR = ",";
	public static final String TIMERELATION_ERROR_MSG_TEMPLATE = "The given timestamp type is not supported: %s";


	private final EntityTemporalService entityTemporalService;
	private final LdContextCache contextCache;
	private final QueryParser queryParser;
	private final ApiDomainMapper apiDomainMapper;

	@Override
	public HttpResponse<Object> queryTemporalEntities(
			@Nullable String link,
			@Nullable String id,
			@Nullable String idPattern,
			@Nullable @Size(min = 1) String type,
			@Nullable @Size(min = 1) String attrs,
			@Nullable @Size(min = 1) String q,
			@Nullable String georel,
			@Nullable String geometry,
			@Nullable String coordinates,
			@Nullable @Size(min = 1) String geoproperty,
			@Nullable TimerelVO timerel,
			@Nullable @Pattern(regexp = "^((\\d|[a-zA-Z]|_)+(:(\\d|[a-zA-Z]|_)+)?(#\\d+)?)$") @Size(min = 1) String timeproperty,
			@Nullable Instant timeAt,
			@Nullable Instant endTimeAt,
			@Nullable @Size(min = 1) String csf,
			Integer pageSize,
			URI pageAnchor,
			@Nullable String options,
			@Nullable @Min(1) Integer lastN) {

		AcceptType acceptType = getAcceptType();

		List<URL> contextUrls = contextCache.getContextURLsFromLinkHeader(link);
		String expandedGeoProperty = Optional.ofNullable(geoproperty)
				.filter(property -> !WELL_KNOWN_ATTRIBUTES.contains(property))
				.map(property -> contextCache.expandString(property, contextUrls))
				.orElse(DEFAULT_GEO_PROPERTY);
		TimeQuery timeQuery = new TimeQuery(apiDomainMapper.timeRelVoToTimeRelation(timerel), timeAt, endTimeAt, getTimeRelevantProperty(timeproperty));

		Optional<List<String>> optionalIdList = Optional.ofNullable(id).map(this::getIdList);
		Optional<String> optionalIdPattern = Optional.ofNullable(idPattern);
		List<String> expandedTypes = getExpandedTypes(contextUrls, type);
		Optional<QueryTerm> optionalQuery = Optional.ofNullable(q).map(queryString -> queryParser.toTerm(queryString, contextUrls));
		Optional<GeoQuery> optionalGeoQuery = getGeometryQuery(georel, geometry, coordinates, expandedGeoProperty);


		LimitableResult<List<EntityTemporalVO>> limitableResult = entityTemporalService.getEntitiesWithQuery(
				optionalIdList,
				optionalIdPattern,
				getExpandedTypes(contextUrls, type),
				getExpandedAttributes(contextUrls, attrs),
				optionalQuery,
				optionalGeoQuery,
				timeQuery,
				lastN,
				isSysAttrs(options),
				isTemporalValuesOptionSet(options),
				pageSize,
				Optional.ofNullable(pageAnchor).map(URI::toString));

		List<EntityTemporalVO> entityTemporalVOS = limitableResult.getResult();
		entityTemporalVOS.forEach(entityTemporalVO -> addContextToEntityTemporalVO(entityTemporalVO, contextUrls));

		Optional<PaginationInformation> paginationInformation = Optional.empty();
		if (entityTemporalVOS.size() == pageSize) {
			paginationInformation = Optional.of(
					entityTemporalService
							.getPaginationInfo(optionalIdList, optionalIdPattern, expandedTypes, optionalQuery, optionalGeoQuery, timeQuery, pageSize, Optional.ofNullable(pageAnchor).map(URI::toString)));
		}

		EntityTemporalListVO entityTemporalListVO = new EntityTemporalListVO();
		entityTemporalListVO.addAll(entityTemporalVOS);
		MutableHttpResponse<Object> mutableHttpResponse;
		if (limitableResult.isLimited()) {
			Range range = getRange(getTimestampListFromEntityTemporalList(entityTemporalVOS, timeQuery), timeQuery, lastN);
			mutableHttpResponse = HttpResponse
					.status(HttpStatus.PARTIAL_CONTENT)
					.body((Object) entityTemporalListVO)
					.header(CONTENT_RANGE_HEADER_KEY, getContentRange(range, lastN));
		} else {
			mutableHttpResponse = HttpResponse.ok(entityTemporalListVO);
		}
		paginationInformation.ifPresent(pi -> {
			mutableHttpResponse.header("Page-Size", String.valueOf(pi.getPageSize()));
			pi.getNextPage().ifPresent(np -> mutableHttpResponse.header("Next-Page", np));
			pi.getPreviousPage().ifPresent(pp -> mutableHttpResponse.header("Previous-Page", pp));
		});

		if (isCountOptionSet(options)) {
			Number totalCount = entityTemporalService.countMatchingEntities(optionalIdList, optionalIdPattern, expandedTypes, optionalQuery, optionalGeoQuery, timeQuery);
			mutableHttpResponse.header("NGSILD-Total-Count", totalCount.toString());
		}

		if (acceptType == AcceptType.JSON) {
			mutableHttpResponse.header("Link", getLinkHeader(contextUrls));
		}
		return mutableHttpResponse;
	}

	@Override
	public HttpResponse<Object> retrieveEntityTemporalById(
			URI entityId,
			@Nullable String link,
			@Nullable @Size(min = 1) String attrs,
			@Nullable String options,
			@Nullable TimerelVO timerel,
			@Nullable @Pattern(regexp = "^((\\d|[a-zA-Z]|_)+(:(\\d|[a-zA-Z]|_)+)?(#\\d+)?)$") @Size(min = 1) String timeproperty,
			@Nullable Instant timeAt,
			@Nullable Instant endTimeAt,
			@Nullable @Min(1) Integer lastN) {

		AcceptType acceptType = getAcceptType();
		List<URL> contextUrls = contextCache.getContextURLsFromLinkHeader(link);
		TimeQuery timeQuery = new TimeQuery(apiDomainMapper.timeRelVoToTimeRelation(timerel), timeAt, endTimeAt, getTimeRelevantProperty(timeproperty));

		Optional<LimitableResult<EntityTemporalVO>> optionalLimitableResult = entityTemporalService
				.getNgsiEntitiesWithTimerel(entityId.toString(),
						timeQuery,
						getExpandedAttributes(contextUrls, attrs),
						lastN,
						isSysAttrs(options),
						isTemporalValuesOptionSet(options));

		if (optionalLimitableResult.isEmpty()) {
			return HttpResponse.notFound();
		}

		MutableHttpResponse mutableHttpResponse;
		LimitableResult<EntityTemporalVO> limitableResult = optionalLimitableResult.get();
		if (limitableResult.isLimited()) {
			Range range = getRange(getTimestampListFromEntityTemporal(limitableResult.getResult(), timeQuery), timeQuery, lastN);
			mutableHttpResponse = HttpResponse
					.status(HttpStatus.PARTIAL_CONTENT)
					.body((Object) addContextToEntityTemporalVO(limitableResult.getResult(), contextUrls))
					.header(CONTENT_RANGE_HEADER_KEY, getContentRange(range, lastN));
		} else {
			mutableHttpResponse = HttpResponse.ok(addContextToEntityTemporalVO(limitableResult.getResult(), contextUrls));
		}
		if (acceptType == AcceptType.JSON) {
			mutableHttpResponse.header("Link", getLinkHeader(contextUrls));
		}
		return mutableHttpResponse;
	}

	private List<String> getIdList(String id) {
		return Arrays.asList(id.split(","));
	}

	private String getLinkHeader(List<URL> contextUrls) {
		// its either core or the current, since core is always the latest entry
		return String.format(LINK_HEADER_TEMPLATE, contextUrls.get(0));
	}

	private String getContentRange(Range range, Integer lastN) {
		String size = "*";
		if (lastN != null && lastN > 0) {
			size = lastN.toString();
		}
		return String.format("date-time %s-%s/%s", range.getStart(), range.getEnd(), size);
	}

	private List<Instant> getTimestampListFromEntityTemporalList(List<EntityTemporalVO> entityTemporalVOS, TimeQuery timeQuery) {
		return entityTemporalVOS
				.stream()
				.flatMap(entityTemporalVO -> getTimestampListFromEntityTemporal(entityTemporalVO, timeQuery).stream())
				.collect(Collectors.toList());
	}

	private List<Instant> getTimestampListFromEntityTemporal(EntityTemporalVO entityTemporalVO, TimeQuery timeQuery) {
		List<Instant> timeStampList = new ArrayList<>();
		timeStampList.addAll(Optional.ofNullable(entityTemporalVO.getObservationSpace())
				.stream()
				.flatMap(List::stream)
				.map(geoPropertyVO -> getTimestampFromGeoProperty(geoPropertyVO, timeQuery.getTimeStampType()))
				.collect(Collectors.toList()));
		timeStampList.addAll(Optional.ofNullable(entityTemporalVO.getOperationSpace())
				.stream()
				.flatMap(List::stream)
				.map(geoPropertyVO -> getTimestampFromGeoProperty(geoPropertyVO, timeQuery.getTimeStampType()))
				.collect(Collectors.toList()));
		timeStampList.addAll(Optional.ofNullable(entityTemporalVO.getLocation())
				.stream()
				.flatMap(List::stream)
				.map(geoPropertyVO -> getTimestampFromGeoProperty(geoPropertyVO, timeQuery.getTimeStampType()))
				.collect(Collectors.toList()));
		timeStampList.addAll(Optional.ofNullable(entityTemporalVO.getAdditionalProperties())
				.stream()
				.map(Map::values)
				.flatMap(Collection::stream)
				.flatMap(instanceList -> ((List<Object>) instanceList).stream())
				.map(propertyObject -> getTimestampFromPropertyObject(propertyObject, timeQuery.getTimeStampType()))
				.collect(Collectors.toList()));
		return timeStampList;
	}

	private Range getRange(List<Instant> timeStampList, TimeQuery timeQuery, Integer lastN) {
		if (lastN != null) {
			return getRangeWithLastN(timeStampList, timeQuery);
		} else {
			return getRangeWithoutLastN(timeStampList, timeQuery);
		}
	}

	private Range getRangeWithoutLastN(List<Instant> timeStamps, TimeQuery timeQuery) {
		TimeRelation timeRelation = Optional.ofNullable(timeQuery.getTimeRelation()).orElse(TimeRelation.BEFORE);
		timeStamps.sort(Comparator.naturalOrder());

		Instant startOfList = timeStamps.get(0);
		Instant endOfList = timeStamps.get(timeStamps.size() - 1);

		switch (timeRelation) {
			case BEFORE:
				return new Range(startOfList, endOfList);
			case BETWEEN:
			case AFTER:
				return new Range(timeQuery.getTimeAt(), endOfList);
			default:
				throw new InvalidTimeRelationException(String.format("Received an invalid timerelation: %s", timeRelation));
		}
	}

	private Range getRangeWithLastN(List<Instant> timeStamps, TimeQuery timeQuery) {
		TimeRelation timeRelation = Optional.ofNullable(timeQuery.getTimeRelation()).orElse(TimeRelation.AFTER);
		timeStamps.sort(Comparator.naturalOrder());

		switch (timeRelation) {
			case BETWEEN:
				return new Range(timeQuery.getEndTime(), timeStamps.get(0));
			case BEFORE:
				return new Range(timeQuery.getTimeAt(), timeStamps.get(0));
			case AFTER:
				return new Range(timeStamps.get(timeStamps.size() - 1), timeStamps.get(0));
			default:
				throw new InvalidTimeRelationException(String.format("Received an invalid timerelation: %s", timeRelation));
		}
	}

	private Instant getTimestampFromPropertyObject(Object propertyObject, TimeStampType timeStampType) {
		if (propertyObject instanceof RelationshipVO) {
			return getTimestampFromRelationShip((RelationshipVO) propertyObject, timeStampType);
		} else if (propertyObject instanceof PropertyVO) {
			return getTimestampFromProperty((PropertyVO) propertyObject, timeStampType);
		} else if (propertyObject instanceof GeoPropertyVO) {
			return getTimestampFromGeoProperty((GeoPropertyVO) propertyObject, timeStampType);
		}
		throw new PersistenceRetrievalException(String.format("The given propertyObject is not valid: %s", propertyObject));
	}

	private Instant getTimestampFromProperty(PropertyVO propertyVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return propertyVO.getCreatedAt();
			case OBSERVED_AT:
				return propertyVO.getObservedAt();
			case MODIFIED_AT:
			case TS:
				return propertyVO.getModifiedAt();
			default:
				throw new InvalidTimeRelationException(String.format(TIMERELATION_ERROR_MSG_TEMPLATE, timeStampType));
		}
	}

	private Instant getTimestampFromRelationShip(RelationshipVO relationshipVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return relationshipVO.getCreatedAt();
			case OBSERVED_AT:
				return relationshipVO.getObservedAt();
			case MODIFIED_AT:
			case TS:
				return relationshipVO.getModifiedAt();
			default:
				throw new InvalidTimeRelationException(String.format(TIMERELATION_ERROR_MSG_TEMPLATE, timeStampType));
		}
	}

	private Instant getTimestampFromGeoProperty(GeoPropertyVO geoPropertyVO, TimeStampType timeStampType) {
		switch (timeStampType) {
			case CREATED_AT:
				return geoPropertyVO.getCreatedAt();
			case OBSERVED_AT:
				return geoPropertyVO.getObservedAt();
			case MODIFIED_AT:
			case TS:
				return geoPropertyVO.getModifiedAt();
			default:
				throw new InvalidTimeRelationException(String.format("The given timestamp type is not supported: %s", timeStampType));
		}
	}

	/**
	 * Add the context urls to the entities temporal represenation
	 */
	private EntityTemporalVO addContextToEntityTemporalVO(EntityTemporalVO entityTemporalVO, List<URL> contextUrls) {
		if (contextUrls.size() > 1) {
			entityTemporalVO.atContext(contextUrls);
		} else {
			entityTemporalVO.atContext(contextUrls.get(0));
		}
		return entityTemporalVO;
	}

	/**
	 * Expand all attributes present in the attrs parameter
	 *
	 * @param contextUrls
	 * @param attrs
	 * @return
	 */
	private List<String> getExpandedAttributes(List<URL> contextUrls, String attrs) {
		if (attrs == null) {
			return List.of();
		}

		return Arrays.stream(attrs.split(COMMA_SEPERATOR))
				.map(attribute -> {
					if (WELL_KNOWN_ATTRIBUTES.contains(attribute)) {
						return attribute;
					}
					return contextCache.expandString(attribute, contextUrls);
				}).collect(Collectors.toList());
	}

	private List<String> getExpandedTypes(List<URL> contextUrls, String types) {
		return Optional.ofNullable(types)
				.map(al -> contextCache.expandStrings(Arrays.asList(types.split(COMMA_SEPERATOR)), contextUrls))
				.orElse(List.of());
	}

	/**
	 * Get the timeProperty string or the default property if null
	 *
	 * @param timeProperty timeProperty retrieved through the api
	 * @return timeProperty to be used
	 */
	private String getTimeRelevantProperty(String timeProperty) {
		return Optional.ofNullable(timeProperty).orElse(DEFAULT_TIME_PROPERTY);
	}

	private boolean isSysAttrs(String options) {
		if (options == null) {
			return false;
		}
		return Arrays.asList(options.split(COMMA_SEPERATOR)).contains(SYS_ATTRS_OPTION);
	}

	private boolean isTemporalValuesOptionSet(String options) {
		Optional<String> optionalOptions = Optional.ofNullable(options);
		if (optionalOptions.isEmpty()) {
			return false;
		}
		return Arrays.asList(options.split(COMMA_SEPERATOR)).contains(TEMPORAL_VALUES_OPTION);
	}

	private boolean isCountOptionSet(String options) {
		Optional<String> optionalOptions = Optional.ofNullable(options);
		if (optionalOptions.isEmpty()) {
			return false;
		}
		return Arrays.asList(options.split(COMMA_SEPERATOR)).contains(COUNT_OPTION);
	}

	private Optional<GeoQuery> getGeometryQuery(String georel, String geometry, String coordinates, String geoproperty) {
		if (georel == null && coordinates == null && geometry == null) {
			return Optional.empty();
		}

		if (georel == null || coordinates == null || geometry == null) {
			throw new IllegalArgumentException(
					String.format("When querying for geoRelations, all 3 parameters(georel: %s, coordinates: %s, geometry: %s) need to be present.", georel, coordinates, geometry));
		}

		return Optional.of(new GeoQuery(georel, Geometry.byName(geometry), coordinates, geoproperty));
	}

	private AcceptType getAcceptType() {
		return ServerRequestContext.currentRequest()
				.map(HttpRequest::getHeaders)
				.map(headers -> headers.get("Accept"))
				.map(AcceptType::getEnum)
				.orElse(AcceptType.JSON);
	}
}
