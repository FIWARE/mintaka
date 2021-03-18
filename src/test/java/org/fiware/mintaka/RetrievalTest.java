package org.fiware.mintaka;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.fiware.mintaka.domain.AcceptType;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.fiware.mintaka.exception.ErrorType;
import org.fiware.mintaka.exception.ProblemDetails;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RetrievalTest extends ComposeTest {

	@DisplayName("Retrieve not found for not existing entities")
	@Test
	public void testGetEntityByIdNotFound() {
		assertNotFound(HttpRequest.GET("/temporal/entities/rn:ngsi-ld:store:not-found"), "For non existing entities a 404 should be returned.");
	}

	@DisplayName("Retrieve for non existent tenant.")
	@Test
	public void testNonExistentTenant() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getHeaders().add("NGSILD-Tenant", "non_existent");
		assertError(getRequest, ErrorType.NON_EXISTENT_TENANT, Optional.of("non_existent"));
	}

	@DisplayName("Requests with unsupported parameters should lead to a 400")
	@Test
	public void testGetWithUnsupportedParameter() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("myUnsupportedParam", "myValue");

		assertError(getRequest, ErrorType.BAD_REQUEST_DATA, Optional.empty());
	}

	@DisplayName("Requests with unsupported method should lead to a 422")
	@Test
	public void testUnsupportedMethod() {
		MutableHttpRequest getRequest = HttpRequest.POST("/temporal/entities/" + ENTITY_ID, "");
		assertError(getRequest, ErrorType.OPERATION_NOT_SUPPORTED, Optional.empty());
	}

	@DisplayName("Request with invalid timerelation should lead to 400")
	@ParameterizedTest
	@MethodSource("provideInvalidTimeRelations")
	public void testGetWithInvalidTimeRel(Map<String, String> parameter) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		parameter.entrySet().forEach(entry -> getRequest.getParameters().add(entry.getKey(), entry.getValue()));

		assertError(getRequest, ErrorType.INVALID_REQUEST, Optional.empty());
	}

	private void assertError(MutableHttpRequest getRequest, ErrorType errorType, Optional<String> instanceId) {
		try {
			mintakaTestClient.toBlocking().exchange(getRequest);
			fail("The request should have been rejected.");
		} catch (HttpClientResponseException e) {
			assertEquals(errorType.getStatus(), e.getStatus(), "The request should have been rejected.");
			Optional<ProblemDetails> problemDetails = e.getResponse().getBody(ProblemDetails.class);
			assertTrue(problemDetails.isPresent(), "Problem details should have been returned.");
			assertEquals(errorType.getType(), problemDetails.get().getType(), "Correct error type should have been returned");
			assertEquals(errorType.getStatus().getCode(), problemDetails.get().getStatus(), "Correct error status should have been returned.");
			instanceId.ifPresent(iid -> assertEquals(iid, problemDetails.get().getInstance(), "Correct instance id should have been returned."));
		}
	}

	private static Stream<Arguments> provideInvalidTimeRelations() {
		return Stream.of(
				Arguments.of(Map.of("timerel", "beforex", "timeAt", "1970-01-02T00:00:00Z")));
		//TODO: update micronaut codegen for better enum errors.
//		Arguments.of(Map.of("timerel", "beorex")));
	}

	@DisplayName("Retrieve entity without attributes if non-existent is requested.")
	@Test
	public void testGetEntityByIdWithNonExistingAttribute() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", "nonExisting");

		assertNotFound(getRequest, "If the requested attribute does not exist, a 404 should be returned.");
	}

	@DisplayName("Retrieve deleted entity should lead to 404.")
	@Test
	public void testGetDeletedEntityById() {
		assertNotFound(HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID), "A deleted entity should not be retrieved.");
	}

	@DisplayName("Retrieve deleted entity with timerel after deletion should lead to 404.")
	@Test
	public void testGetDeletedEntityByIdAfterDeletion() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID);
		request.getParameters().add("timerel", "after")
				.add("timeAt", "1970-01-02T00:00:00Z");
		assertNotFound(request, "A deleted entity should not be retrieved.");
	}

	@DisplayName("Retrieve entity with timerel before creation should lead to 404.")
	@Test
	public void testGetEntityByIdBeforeCreation() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + CREATED_AFTER_ENTITY_ID);
		request.getParameters().add("timerel", "before")
				.add("timeAt", "1970-01-02T00:00:00Z");
		assertNotFound(request, "An entity should not be retrieved before its creation.");
	}

	@DisplayName("Retrieve deleted entity with between timeframe after deletion should lead to 404.")
	@Test
	public void testGetDeletedEntityByIdAfterDeletionWithBetween() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID);
		request.getParameters().add("timerel", "between")
				.add("timeAt", "1970-01-01T10:00:00Z")
				.add("endTimeAt", "1970-01-02T00:00:00Z");
		assertNotFound(request, "A deleted entity should not be retrieved.");
	}

	@DisplayName("Retrieve entity with between timeframe before creation should lead to 404.")
	@Test
	public void testGetEntityByIdBeforeCreationWithBetween() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID);
		request.getParameters().add("timerel", "between")
				.add("timeAt", "1970-01-01T10:00:00Z")
				.add("endTimeAt", "1970-01-02T00:00:00Z");
		assertNotFound(request, "An entity should not be retrieved before its creation.");
	}

	@DisplayName("Retrieve the full entity. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityByIdWithoutTime(URI entityId, AcceptType acceptType) {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + entityId);
		request.getParameters().add("attrs", "temperature,open,storeName,polygon,multiPolygon,lineString,multiLineString,propertyWithSubProperty,relatedEntity");
		request.getHeaders().add("Accept", acceptType.getValue());
		HttpResponse<Map<String, Object>> response = mintakaTestClient.toBlocking().exchange(request, Map.class);
		if (acceptType == AcceptType.JSON) {
			assertTrue(response.getHeaders().contains("Link"), "The link header should have been included.");
		}

		Map<String, Object> entityTemporalMap = response.body();
		assertDefaultStoreTemporalEntity(entityId, entityTemporalMap, acceptType);

		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		} else {
			assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 2, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		}

		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with only the requested attribute. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTime(String propertyToRetrieve, URI entityID, AcceptType acceptType) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + entityID);
		getRequest.getParameters().add("attrs", propertyToRetrieve);
		getRequest.getHeaders().add("Accept", acceptType.getValue());

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityID, entityTemporalMap, acceptType);
		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(4, entityTemporalMap.size(), "Only id, type, context and the open attribute should have been returned.");
		} else {
			assertEquals(3, entityTemporalMap.size(), "Only id, type and the open attribute should have been returned.");
		}
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), NUMBER_OF_UPDATES + 1, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with multiple requested attributes. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityWithMultipleAttributesByIdWithoutTime(List<String> attributesList, URI entityId, AcceptType acceptType) {
		String propertyToRetrieve = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + entityId);
		getRequest.getParameters().add("attrs", propertyToRetrieve);
		getRequest.getHeaders().add("Accept", acceptType.getValue());
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityId, entityTemporalMap, acceptType);
		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		} else {
			assertEquals(attributesList.size() + 2, entityTemporalMap.size(), "Only id, type and the attributes should have been returned.");
		}
		assertAttributesInMap(entityTemporalMap, attributesList, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	// between
	@DisplayName("Retrieve the full entity between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityBetweenTimestamps(URI entityId, AcceptType acceptType) {
		assertAttributesBetween(FULL_ENTITY_ATTRIBUTES_LIST, entityId, acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestamps(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesBetween(List.of(attributeName), entityId, acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attributes between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestamps(List<String> subList, URI entityId, AcceptType acceptType) {
		assertAttributesBetween(subList, entityId, acceptType);
	}


	// before
	@DisplayName("Retrieve the full entity before the timestamp, default context.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetEntityBeforeTimestamp(AcceptType acceptType) {
		assertAttributesBefore(FULL_ENTITY_ATTRIBUTES_LIST, acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBeforeTimestamps(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesBefore(List.of(attributeName), acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBeforeTimestamps(List<String> subList, URI entityId, AcceptType acceptType) {
		assertAttributesBefore(subList, acceptType);
	}


	// after
	@DisplayName("Retrieve the full entity after the timestamp, default context.")
	@EnumSource(AcceptType.class)
	@ParameterizedTest
	public void testGetEntityAfterTimestamp(AcceptType acceptType) {
		assertAttributesAfter(FULL_ENTITY_ATTRIBUTES_LIST, acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesAfterTimestamps(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesAfter(List.of(attributeName), acceptType);
	}

	@DisplayName("Retrieve the entity with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesAfterTimestamps(List<String> subList, URI entityId, AcceptType acceptType) {
		assertAttributesAfter(subList, acceptType);
	}

	// lastN
	@DisplayName("Retrieve the last n full instances. default context.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetEntityLastNWithoutTime(AcceptType acceptType) {
		int lastN = 5;

		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		request.getParameters().add("lastN", String.valueOf(lastN));
		request.getHeaders().add("Accept", acceptType.getValue());
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(request, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap, acceptType);

		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		} else {
			assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 2, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		}

		// NUMBER_OF_UPDATES - lastN + 1 - go 5 steps back but include the 5th last.
		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances with only the requested attribute . No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTimeAndLastN(String propertyToRetrieve, URI entityId, AcceptType acceptType) {
		int lastN = 5;

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve).add("lastN", String.valueOf(lastN));
		getRequest.getHeaders().add("Accept", acceptType.getValue());
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap, acceptType);
		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(4, entityTemporalMap.size(), "Only id, type, context and the open attribute should have been returned.");
		} else {
			assertEquals(3, entityTemporalMap.size(), "Only id, type, context and the open attribute should have been returned.");
		}
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), lastN, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances with multiple requested attributes. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityWithMultipleAttributesByIdWithoutTimeAndLastN(List<String> attributesList, URI entityId, AcceptType acceptType) {
		int lastN = 5;
		String propertyToRetrieve = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve).add("lastN", String.valueOf(lastN));
		getRequest.getHeaders().add("Accept", acceptType.getValue());

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap, acceptType);
		if (acceptType == AcceptType.JSON_LD) {
			assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		} else {
			assertEquals(attributesList.size() + 2, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		}
		assertAttributesInMap(entityTemporalMap, attributesList, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances before the timestamp, default context.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetEntityBeforeTimestampWithLastN(AcceptType acceptType) {
		assertAttributesBeforeWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBeforeTimestampsWithLastN(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesBeforeWithLastN(List.of(attributeName), 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBeforeTimestampsWithLastN(List<String> subList, URI entityId, AcceptType acceptType) {
		assertAttributesBeforeWithLastN(subList, 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances after the timestamp, default context.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetEntityAfterTimestampWithLastN(AcceptType acceptType) {
		assertAttributesAfterWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesAfterTimestampsWithLastN(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesAfterWithLastN(List.of(attributeName), 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesAfterTimestampsWithLastN(List<String> subList, URI entityID, AcceptType acceptType) {
		assertAttributesAfterWithLastN(subList, 5, acceptType);
	}

	@DisplayName("Retrieve the last n instances between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityBetweenTimestampWithLastN(URI entityId, AcceptType acceptType) {
		assertAttributesBetweenWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5, entityId, acceptType);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestampsWithLastN(String attributeName, URI entityId, AcceptType acceptType) {
		assertAttributesBetweenWithLastN(List.of(attributeName), 5, entityId, acceptType);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestampsWithLastN(List<String> subList, URI entityId, AcceptType acceptType) {
		assertAttributesBetweenWithLastN(subList, 5, entityId, acceptType);
	}

	@DisplayName("Retrieve an entity that gets paged.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetPagedEntity(AcceptType acceptType) {
		Instant startTime = START_TIME_STAMP;
		Instant endTime = START_TIME_STAMP.plus(110, ChronoUnit.MINUTES);

		// initial query without range
		assertRange(startTime, endTime, Optional.empty(), Optional.empty(), acceptType);
		startTime = endTime;
		endTime = startTime.plus(111, ChronoUnit.MINUTES);
		// second query with after(due to partial content response)
		assertRange(startTime, endTime, Optional.of(TimeRelation.AFTER), Optional.empty(), acceptType);
		startTime = endTime;
		endTime = startTime.plus(111, ChronoUnit.MINUTES);
		// third query, similar to second
		assertRange(startTime, endTime, Optional.of(TimeRelation.AFTER), Optional.empty(), acceptType);
		startTime = endTime;

		// final query for the full end result
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + LIMITED_ENTITY_ID);
		request.getParameters()
				.add("timerel", TimeRelation.AFTER.name())
				.add("timeAt", LocalDateTime.ofInstant(startTime, ZoneOffset.UTC).toString());
		request.getHeaders().add("Accept", acceptType.getValue());
		HttpResponse<Map<String, Object>> response = mintakaTestClient.toBlocking().exchange(request, Map.class);
		assertEquals(HttpStatus.OK, response.getStatus(), "Last response shouldn't be partial anymore.");
		assertDefaultStoreTemporalEntity(LIMITED_ENTITY_ID, response.body(), acceptType);
		assertAttributesInMap(response.body(),
				List.of("temperature", "open", "storeName", "polygon", "multiPolygon", "lineString", "multiLineString", "propertyWithSubProperty", "relatedEntity"),
				18,
				startTime.plus(1, ChronoUnit.MINUTES),
				startTime.plus(18, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve an entity that gets paged limited by lastN.")
	@ParameterizedTest
	@EnumSource(AcceptType.class)
	public void testGetPagedEntityWithLastN(AcceptType acceptType) {
		int lastN = 350;
		Instant startTime = START_TIME_STAMP.plus(lastN - 110, ChronoUnit.MINUTES);
		Instant endTime = START_TIME_STAMP.plus(lastN, ChronoUnit.MINUTES);

		// initial query without range
		assertRange(startTime, endTime, Optional.empty(), Optional.of(lastN), acceptType);
		endTime = endTime.minus(111, ChronoUnit.MINUTES);
		startTime = endTime.minus(111, ChronoUnit.MINUTES);
		// second query with after(due to partial content response)
		assertRange(startTime, endTime, Optional.of(TimeRelation.BEFORE), Optional.of(lastN), acceptType);
		endTime = endTime.minus(111, ChronoUnit.MINUTES);
		startTime = endTime.minus(111, ChronoUnit.MINUTES);
		// third query, similar to second
		assertRange(startTime, endTime, Optional.of(TimeRelation.BEFORE), Optional.of(lastN), acceptType);
		endTime = startTime;

		// final query for the full end result
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + LIMITED_ENTITY_ID);
		request.getParameters()
				.add("timerel", TimeRelation.BEFORE.name())
				.add("timeAt", LocalDateTime.ofInstant(endTime, ZoneOffset.UTC).toString());
		request.getHeaders().add("Accept", acceptType.getValue());
		HttpResponse<Map<String, Object>> response = mintakaTestClient.toBlocking().exchange(request, Map.class);
		assertEquals(HttpStatus.OK, response.getStatus(), "Last response shouldn't be partial anymore.");
		assertDefaultStoreTemporalEntity(LIMITED_ENTITY_ID, response.body(), acceptType);
		assertAttributesInMap(response.body(),
				List.of("temperature", "open", "storeName", "polygon", "multiPolygon", "lineString", "multiLineString", "propertyWithSubProperty", "relatedEntity"),
				17,
				endTime.minus(17, ChronoUnit.MINUTES),
				endTime.minus(1, ChronoUnit.MINUTES));
	}

	private void assertRange(Instant expectedStartTime, Instant expectedEndTime, Optional<TimeRelation> queryRelation, Optional<Integer> optionalLastN, AcceptType acceptType) {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + LIMITED_ENTITY_ID);
		request.getParameters().add("attrs", "temperature,open,storeName,polygon,multiPolygon,lineString,multiLineString,propertyWithSubProperty,relatedEntity");
		request.getHeaders().add("Accept", acceptType.getValue());
		String expectedRangeHeader = getRangeHeader(expectedStartTime, expectedEndTime, optionalLastN);
		if (queryRelation.isPresent()) {
			switch (queryRelation.get()) {
				case BETWEEN:
					request.getParameters()
							.add("timerel", TimeRelation.BETWEEN.name())
							.add("timeAt", LocalDateTime.ofInstant(expectedStartTime, ZoneOffset.UTC).toString())
							.add("endTimeAt", LocalDateTime.ofInstant(expectedEndTime, ZoneOffset.UTC).toString());
					expectedEndTime = expectedEndTime.minus(1, ChronoUnit.MINUTES);
					expectedStartTime = expectedStartTime.plus(1, ChronoUnit.MINUTES);
					break;
				case BEFORE:
					request.getParameters()
							.add("timerel", TimeRelation.BEFORE.name())
							.add("timeAt", LocalDateTime.ofInstant(expectedEndTime, ZoneOffset.UTC).toString());
					expectedEndTime = expectedEndTime.minus(1, ChronoUnit.MINUTES);
					break;
				case AFTER:
					request.getParameters()
							.add("timerel", TimeRelation.AFTER.name())
							.add("timeAt", LocalDateTime.ofInstant(expectedStartTime, ZoneOffset.UTC).toString());
					expectedStartTime = expectedStartTime.plus(1, ChronoUnit.MINUTES);
					break;
			}
		}

		optionalLastN.ifPresent(lastN -> request.getParameters().add("lastN", lastN.toString()));

		HttpResponse<Map<String, Object>> response = mintakaTestClient.toBlocking().exchange(request, Map.class);
		assertEquals(HttpStatus.PARTIAL_CONTENT, response.getStatus(), "Only parts of the history should be returned.");
		assertDefaultStoreTemporalEntity(LIMITED_ENTITY_ID, response.body(), acceptType);
		String rangeHeader = response.getHeaders().get("Content-Range");
		assertEquals(
				expectedRangeHeader,
				rangeHeader,
				"Range header should contain the retrieved range.");
		assertAttributesInMap(response.body(),
				List.of("temperature", "open", "storeName", "polygon", "multiPolygon", "lineString", "multiLineString", "propertyWithSubProperty", "relatedEntity"),
				111,
				expectedStartTime,
				expectedEndTime);
	}

	private String getRangeHeader(Instant start, Instant end, Optional<Integer> lastN) {
		String size = lastN.map(Object::toString).orElse("*");
		String headerTemplate = "date-time %s-%s/%s";

		if (lastN.isPresent()) {
			return String.format(headerTemplate, LocalDateTime.ofInstant(end, ZoneOffset.UTC), LocalDateTime.ofInstant(start, ZoneOffset.UTC), size);
		} else {
			return String.format(headerTemplate, LocalDateTime.ofInstant(start, ZoneOffset.UTC), LocalDateTime.ofInstant(end, ZoneOffset.UTC), size);
		}
	}
}
