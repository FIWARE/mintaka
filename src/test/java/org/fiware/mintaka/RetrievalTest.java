package org.fiware.mintaka;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RetrievalTest extends ComposeTest {

	@DisplayName("Retrieve not found for not existing entities")
	@Test
	public void testGetEntityByIdNotFound() {
		assertNotFound(HttpRequest.GET("/temporal/entities/rn:ngsi-ld:store:not-found"), "For non existing entities a 404 should be returned.");
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
				.add("time", "1970-01-02T00:00:00Z");
		assertNotFound(request, "A deleted entity should not be retrieved.");
	}

	@DisplayName("Retrieve entity with timerel before creation should lead to 404.")
	@Test
	public void testGetEntityByIdBeforeCreation() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + CREATED_AFTER_ENTITY_ID);
		request.getParameters().add("timerel", "before")
				.add("time", "1970-01-02T00:00:00Z");
		assertNotFound(request, "An entity should not be retrieved before its creation.");
	}

	@DisplayName("Retrieve deleted entity with between timeframe after deletion should lead to 404.")
	@Test
	public void testGetDeletedEntityByIdAfterDeletionWithBetween() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID);
		request.getParameters().add("timerel", "between")
				.add("time", "1970-01-01T10:00:00Z")
				.add("endTime", "1970-01-02T00:00:00Z");
		assertNotFound(request, "A deleted entity should not be retrieved.");
	}

	@DisplayName("Retrieve entity with between timeframe before creation should lead to 404.")
	@Test
	public void testGetEntityByIdBeforeCreationWithBetween() {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + DELETED_ENTITY_ID);
		request.getParameters().add("timerel", "between")
				.add("time", "1970-01-01T10:00:00Z")
				.add("endTime", "1970-01-02T00:00:00Z");
		assertNotFound(request, "An entity should not be retrieved before its creation.");
	}

	@DisplayName("Retrieve the full entity. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityByIdWithoutTime(URI entityId) {
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(HttpRequest.GET("/temporal/entities/" + entityId), Map.class);
		assertDefaultStoreTemporalEntity(entityId, entityTemporalMap);

		assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");

		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with only the requested attribute. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTime(String propertyToRetrieve, URI entityID) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + entityID);
		getRequest.getParameters().add("attrs", propertyToRetrieve);

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityID, entityTemporalMap);
		assertEquals(entityTemporalMap.size(), 4, "Only id, type, context and the open attribute should have been returned.");
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), NUMBER_OF_UPDATES + 1, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with multiple requested attributes. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityWithMultipleAttributesByIdWithoutTime(List<String> attributesList, URI entityId) {
		String propertyToRetrieve = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + entityId);
		getRequest.getParameters().add("attrs", propertyToRetrieve);

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityId, entityTemporalMap);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(entityTemporalMap, attributesList, NUMBER_OF_UPDATES + 1, START_TIME_STAMP, START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	// between
	@DisplayName("Retrieve the full entity between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityBetweenTimestamps(URI entityId) {
		assertAttributesBetween(FULL_ENTITY_ATTRIBUTES_LIST, entityId);
	}

	@DisplayName("Retrieve the entity with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestamps(String attributeName, URI entityId) {
		assertAttributesBetween(List.of(attributeName), entityId);
	}

	@DisplayName("Retrieve the entity with the requested attributes between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestamps(List<String> subList, URI entityId) {
		assertAttributesBetween(subList, entityId);
	}


	// before
	@DisplayName("Retrieve the full entity before the timestamp, default context.")
	@Test
	public void testGetEntityBeforeTimestamp() {
		assertAttributesBefore(FULL_ENTITY_ATTRIBUTES_LIST);
	}

	@DisplayName("Retrieve the entity with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBeforeTimestamps(String attributeName) {
		assertAttributesBefore(List.of(attributeName));
	}

	@DisplayName("Retrieve the entity with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBeforeTimestamps(List<String> subList) {
		assertAttributesBefore(subList);
	}


	// after
	@DisplayName("Retrieve the full entity after the timestamp, default context.")
	@Test
	public void testGetEntityAfterTimestamp() {
		assertAttributesAfter(FULL_ENTITY_ATTRIBUTES_LIST);
	}

	@DisplayName("Retrieve the entity with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesAfterTimestamps(String attributeName) {
		assertAttributesAfter(List.of(attributeName));
	}

	@DisplayName("Retrieve the entity with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesAfterTimestamps(List<String> subList) {
		assertAttributesAfter(subList);
	}


	// lastN
	@DisplayName("Retrieve the last n full instances. default context.")
	@Test
	public void testGetEntityLastNWithoutTime() {
		int lastN = 5;

		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		request.getParameters().add("lastN", String.valueOf(lastN));
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(request, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap);

		assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");

		// NUMBER_OF_UPDATES - lastN + 1 - go 5 steps back but include the 5th last.
		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances with only the requested attribute . No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTimeAndLastN(String propertyToRetrieve) {
		int lastN = 5;

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve).add("lastN", String.valueOf(lastN));

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap);
		assertEquals(entityTemporalMap.size(), 4, "Only id, type, context and the open attribute should have been returned.");
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), lastN, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances with multiple requested attributes. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityWithMultipleAttributesByIdWithoutTimeAndLastN(List<String> attributesList) {
		int lastN = 5;
		String propertyToRetrieve = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve).add("lastN", String.valueOf(lastN));

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(ENTITY_ID, entityTemporalMap);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(entityTemporalMap, attributesList, lastN, START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances before the timestamp, default context.")
	@Test
	public void testGetEntityBeforeTimestampWithLastN() {
		assertAttributesBeforeWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBeforeTimestampsWithLastN(String attributeName) {
		assertAttributesBeforeWithLastN(List.of(attributeName), 5);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute before the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBeforeTimestampsWithLastN(List<String> subList) {
		assertAttributesBeforeWithLastN(subList, 5);
	}

	@DisplayName("Retrieve the last n instances after the timestamp, default context.")
	@Test
	public void testGetEntityAfterTimestampWithLastN() {
		assertAttributesAfterWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesAfterTimestampsWithLastN(String attributeName) {
		assertAttributesAfterWithLastN(List.of(attributeName), 5);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute after the timestamp, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesAfterTimestampsWithLastN(List<String> subList) {
		assertAttributesAfterWithLastN(subList, 5);
	}

	@DisplayName("Retrieve the last n instances between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	public void testGetEntityBetweenTimestampWithLastN(URI entityId) {
		assertAttributesBetweenWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5, entityId);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestampsWithLastN(String attributeName, URI entityId) {
		assertAttributesBetweenWithLastN(List.of(attributeName), 5, entityId);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestampsWithLastN(List<String> subList, URI entityId) {
		assertAttributesBetweenWithLastN(subList, 5, entityId);
	}

}
