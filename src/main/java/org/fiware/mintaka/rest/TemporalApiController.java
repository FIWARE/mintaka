package org.fiware.mintaka.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.EntityTemporalService;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.exception.InvalidTimeRelationException;
import org.fiware.ngsi.api.TemporalRetrievalApi;
import org.fiware.ngsi.model.*;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of the NGSI-LD temporal retrieval api
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class TemporalApiController implements TemporalRetrievalApi {

	private static final String DEFAULT_TIME_PROPERTY = "observedAt";

	private final EntityTemporalService entityTemporalService;
	private final LdContextCache contextCache;

	@Override
	public HttpResponse<EntityTemporalListVO> queryTemporalEntities(@Nullable String link, @Nullable URI id, @Nullable String idPattern, @Nullable @Size(min = 1) String type, @Nullable @Size(min = 1) String attrs, @Nullable @Size(min = 1) String q, @Nullable String georel, @Nullable GeometryEnumVO geometry, @Nullable CoordinatesVO coordinates, @Nullable @Size(min = 1) String geoproperty, @Nullable TimerelVO timerel, @Nullable @Pattern(regexp = "^((\\d|[a-zA-Z]|_)+(:(\\d|[a-zA-Z]|_)+)?(#\\d+)?)$") @Size(min = 1) String timeproperty, @Nullable Date time, @Nullable Date endTime, @Nullable @Size(min = 1) String csf, @Nullable @Min(1) Integer limit, @Nullable String options, @Nullable @Min(1) Integer lastN) {
		return null;
	}

	@Override
	public HttpResponse<EntityTemporalVO> retrieveEntityTemporalById(URI entityId, @Nullable String link, @Nullable @Size(min = 1) String attrs, @Nullable String options, @Nullable TimerelVO timerel, @Nullable @Pattern(regexp = "^((\\d|[a-zA-Z]|_)+(:(\\d|[a-zA-Z]|_)+)?(#\\d+)?)$") @Size(min = 1) String timeproperty, @Nullable Date time, @Nullable Date endTime, @Nullable @Min(1) Integer lastN) {

		List<URL> contextUrls = LdContextCache.getContextURLsFromLinkHeader(link);
		Instant timeInstant = Optional.ofNullable(time).map(Date::toInstant).orElse(null);
		Instant endTimeInstant = Optional.ofNullable(endTime).map(Date::toInstant).orElse(null);
		validateTimeRelation(timeInstant, endTimeInstant, timerel);

		List<String> attributesList = Optional.ofNullable(attrs)
				.map(al -> contextCache.expandAttributes(Arrays.asList(attrs.split(",")), contextUrls))
				.orElse(List.of());

		EntityTemporalVO entityTemporalVO = entityTemporalService
				.getNgsiEntitiesWithTimerel(entityId.toString(), getTimeRelevantProperty(timeproperty), timerel, timeInstant, endTimeInstant, attributesList, lastN);
		if (contextUrls.size() > 1) {
			entityTemporalVO.atContext(contextUrls);
		} else {
			entityTemporalVO.atContext(contextUrls.get(0));
		}

		return HttpResponse.ok(entityTemporalVO);
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

	/**
	 * Validate the given time relation combination. Throws an {@link InvalidTimeRelationException} if its not valid.
	 * @param time timeReference as requested through the api
	 * @param endTime endpoint of the requested timeframe
	 * @param timerelVO time relation as requested through the api
	 */
	private void validateTimeRelation(Instant time, Instant endTime, TimerelVO timerelVO) {
		if (timerelVO == null && time == null && endTime == null) {
			return;
		}
		if (timerelVO == null) {
			throw new InvalidTimeRelationException("Did not receive a valid time relation config.");
		}
		switch (timerelVO) {
			case AFTER:
				if (time != null && endTime == null) {
					return;
				}
			case BEFORE:
				if (time != null && endTime == null) {
					return;
				}
			case BETWEEN:
				if (time != null && endTime != null) {
					return;
				}
			default:
				throw new InvalidTimeRelationException("Did not receive a valid time relation config.");
		}
	}

}
