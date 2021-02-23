package org.fiware.mintaka;

import com.github.jsonldjava.core.JsonLdConsts;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import lombok.extern.slf4j.Slf4j;
import org.fiware.ngsi.api.EntitiesApiTestClient;
import org.fiware.ngsi.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class ComposeTest {

	private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
	private static final ExecutorService PARALLEL_INITIALIZER_SERVICE = Executors.newCachedThreadPool();

	private static final URI ENTITY_ID = URI.create("urn:ngsi-ld:store:" + UUID.randomUUID().toString());
	private static final URI DELETED_ENTITY_ID = URI.create("urn:ngsi-ld:store:deleted-" + UUID.randomUUID().toString());
	private static final URI CREATED_AFTER_ENTITY_ID = URI.create("urn:ngsi-ld:store:after-" + UUID.randomUUID().toString());
	private static final URI DATA_MODEL_CONTEXT = URI.create("https://fiware.github.io/data-models/context.jsonld");
	private static final URI CORE_CONTEXT = URI.create("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld");
	private static final String ORION_LD_HOST = "orion-ld";
	private static final String TIMESCALE_HOST = "orion-ld";
	private static final int ORION_LD_PORT = 1026;
	private static final int TIMESCALE_PORT = 5432;
	//2 hours of updates
	public static final int NUMBER_OF_UPDATES = 120;


	private static final Instant START_TIME_STAMP = Instant.ofEpochMilli(0);
	private Clock clock;
	private static EmbeddedServer embeddedServer;
	private static ApplicationContext applicationContext;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private HttpClient mintakaTestClient;
	private EntitiesApiTestClient entitiesApiTestClient;
	public static final List<String> FULL_ENTITY_ATTRIBUTES_LIST = List.of(
			"temperature",
			"open",
			"storeName",
			"lineString",
			"polygon",
			"multiPolygon",
			"multiLineString",
			"propertyWithSubProperty",
			"relatedEntity");

	@BeforeAll
	public static void setupEnv() {
		DockerComposeContainer dockerComposeContainer = new DockerComposeContainer(new File("src/test/resources/docker-compose/docker-compose-it.yml"))
				.withExposedService("orion-ld", ORION_LD_PORT)
				.withExposedService("timescale", TIMESCALE_PORT);

		dockerComposeContainer.waitingFor(ORION_LD_HOST, new HttpWaitStrategy()
				.withReadTimeout(Duration.of(1, ChronoUnit.MINUTES)).forPort(ORION_LD_PORT).forPath("/version")).start();
		embeddedServer = ApplicationContext.run(EmbeddedServer.class, PropertySource.of(
				"test", Map.of("datasource.default.host", dockerComposeContainer.getServiceHost(ORION_LD_HOST, ORION_LD_PORT),
						"datasource.default.port", dockerComposeContainer.getServicePort(ORION_LD_HOST, ORION_LD_PORT)
				)));

		applicationContext = embeddedServer.getApplicationContext();
	}

	@BeforeEach
	public void setup() throws MalformedURLException {
		entitiesApiTestClient = applicationContext.getBean(EntitiesApiTestClient.class);
		clock = mock(Clock.class);

		if (!INITIALIZED.getAndSet(true)) {
			List<Future> futureList = new ArrayList<>();
			futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createMovingEntity(URI.create("urn:ngsi-ld:store:moving-store2"))));
			futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createMovingEntity(URI.create("urn:ngsi-ld:store:moving-store"))));
			futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistory(ENTITY_ID, START_TIME_STAMP)));
			futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistoryWithDeletion()));
			futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistory(CREATED_AFTER_ENTITY_ID, START_TIME_STAMP.plus(1, ChronoUnit.YEARS))));
			await().atMost(5, TimeUnit.MINUTES).until(() -> !futureList.stream().anyMatch(f -> !f.isDone()));
		}

		HttpClientConfiguration configuration = new DefaultHttpClientConfiguration();
		//high timeout is required, because github-action runners are not that powerful
		configuration.setReadTimeout(Duration.ofSeconds(30));
		mintakaTestClient = new DefaultHttpClient(new URL("http://" + embeddedServer.getHost() + ":" + embeddedServer.getPort()), configuration);
	}

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

	@DisplayName("Intial test for running a temporal query including geo querying.")
	@Test
	public void testTempQuery() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("type", "store")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "Point")
				.add("coordinates", "[5,5,0]")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T00:13:00Z");
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);

		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
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

	@DisplayName("Retrieve the full entity in temporalValues representation. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideEntityIds")
	@Disabled("Temporal representation is currently invalid json-ld, needs to be fixed first.")
	public void testGetEntityByIdWithoutTimeTemporalValues(URI entityId) {
		MutableHttpRequest request = HttpRequest.GET("/temporal/entities/" + entityId);
		request.getParameters().add("options", "temporalValues");
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(request, Map.class);
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

	/*
	 *  TEST SUPPORT
	 */

	// provider methods

	private static Stream<Arguments> provideFullEntityAttributeStrings() {
		return FULL_ENTITY_ATTRIBUTES_LIST
				.stream()
				.flatMap(attribute -> Stream.of(Arguments.of(attribute, ENTITY_ID), Arguments.of(attribute, DELETED_ENTITY_ID)));
	}

	private static Stream<Arguments> provideCombinedAttributeStrings() {
		return Lists.partition(FULL_ENTITY_ATTRIBUTES_LIST, 3)
				.stream()
				.flatMap(attribute -> Stream.of(Arguments.of(attribute, ENTITY_ID), Arguments.of(attribute, DELETED_ENTITY_ID)));
	}

	private static Stream<Arguments> provideEntityIds() {
		return Stream.of(Arguments.of(ENTITY_ID), Arguments.of(DELETED_ENTITY_ID));
	}

	// assertions

	private void assertAttributesBetween(List<String> attributesList, URI entityId) {
		assertAttributesBetweenWithLastN(attributesList, null, entityId);
	}

	private void assertAttributesBetweenWithLastN(List<String> attributesList, Integer lastN, URI entityId) {
		String timerelation = "between";
		String startTime = "1970-01-01T00:30:00Z";
		String endTime = "1970-01-01T00:45:00Z";

		String attributesParam = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + entityId);
		getRequest.getParameters()
				.add("timerel", timerelation)
				.add("time", startTime)
				.add("endTime", endTime);

		if (attributesList != FULL_ENTITY_ATTRIBUTES_LIST) {
			getRequest.getParameters().add("attrs", attributesParam);
		}

		// start time should be the first after the requested.
		Instant expectedStart = START_TIME_STAMP.plus(31, ChronoUnit.MINUTES);

		if (lastN != null) {
			getRequest.getParameters()
					.add("lastN", String.valueOf(lastN));
			expectedStart = START_TIME_STAMP.plus(45 - lastN, ChronoUnit.MINUTES);
		}

		Integer expectedInstances = lastN != null ? lastN : 14;

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(
				entityTemporalMap,
				attributesList,
				expectedInstances,
				expectedStart,
				START_TIME_STAMP.plus(44, ChronoUnit.MINUTES));
	}

	private void assertAttributesBefore(List<String> attributesList) {
		assertAttributesBeforeWithLastN(attributesList, null);
	}

	private void assertAttributesBeforeWithLastN(List<String> attributesList, Integer lastN) {
		String timerelation = "before";
		String time = "1970-01-01T00:30:00Z";

		String attributesParam = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters()
				.add("timerel", timerelation)
				.add("time", time);

		if (attributesList != FULL_ENTITY_ATTRIBUTES_LIST) {
			getRequest.getParameters().add("attrs", attributesParam);
		}

		Instant expectedStart = START_TIME_STAMP;

		if (lastN != null) {
			getRequest.getParameters()
					.add("lastN", String.valueOf(lastN));
			expectedStart = expectedStart.plus(30 - lastN, ChronoUnit.MINUTES);
		}

		Integer expectedInstances = lastN != null ? lastN : 30;

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the requested attribute should have been returned.");
		assertAttributesInMap(
				entityTemporalMap,
				attributesList,
				expectedInstances,
				expectedStart,
				START_TIME_STAMP.plus(29, ChronoUnit.MINUTES));
	}

	private void assertAttributesAfter(List<String> attributesList) {
		assertAttributesAfterWithLastN(attributesList, null);
	}

	private void assertAttributesAfterWithLastN(List<String> attributesList, Integer lastN) {
		String timerelation = "after";
		String time = "1970-01-01T00:30:00Z";

		String attributesParam = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters()
				.add("timerel", timerelation)
				.add("time", time);
		if (attributesList != FULL_ENTITY_ATTRIBUTES_LIST) {
			getRequest.getParameters().add("attrs", attributesParam);
		}

		// 1 less then updates, because exclusive after
		Instant expectedStart = START_TIME_STAMP.plus(NUMBER_OF_UPDATES - 89, ChronoUnit.MINUTES);

		if (lastN != null) {
			getRequest.getParameters()
					.add("lastN", String.valueOf(lastN));
			// +1, because the last existing update is included
			expectedStart = START_TIME_STAMP.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES);
		}
		Integer expectedInstances = lastN != null ? lastN : 90;

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(
				entityTemporalMap,
				attributesList,
				expectedInstances,
				expectedStart,
				START_TIME_STAMP.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	private void assertAttributesInMap(Map<String, Object> entityTemporalMap, List<String> attributesList, Integer expectedInstances, Instant startTimeStamp, Instant endTimeStamp) {
		attributesList
				.stream()
				.forEach(propertyName -> {
					Object temporalProperty = entityTemporalMap.get(propertyName);
					assertNotNull(temporalProperty, "All entities should have been retrieved.");
					assertTrue(temporalProperty instanceof List, "A list of the properties should have been retrieved.");
					List<Map<String, Object>> listRepresentation = (List) temporalProperty;
					assertEquals(listRepresentation.size(), expectedInstances, "All instances should have been returned(created + 100 updates).");
					assertInstanceInTimeFrame(listRepresentation, expectedInstances, startTimeStamp, endTimeStamp);
				});
	}


	private void assertInstanceInTimeFrame(List<Map<String, Object>> attributeInstanceList, Integer expectedInstancs, Instant startTimeStamp, Instant endTimeStamp) {
		List<Instant> observedAtList = attributeInstanceList
				.stream()
				.map(ai -> (String) ai.get("observedAt"))
				.map(timeString -> OffsetDateTime.parse(timeString).toInstant())
				.sorted(Comparator.naturalOrder())
				.collect(Collectors.toList());

		assertEquals(startTimeStamp, observedAtList.get(0), "The attribute list should start at the initial timestamp.");
		assertEquals(endTimeStamp, observedAtList.get(expectedInstancs - 1), "The attribute list should end with the last element.");

	}

	private void assertDefaultStoreTemporalEntity(URI entityId, Map<String, Object> entityTemporalMap) {
		assertNotNull(entityTemporalMap, "A temporal entity should have been returned.");

		assertEquals(entityTemporalMap.get(JsonLdConsts.CONTEXT),
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
				"The core context should be present if nothing else is requested.");
		assertEquals(entityTemporalMap.get("type"), "store", "The correct type of the entity should be retrieved.");
		assertEquals(entityTemporalMap.get("id"), entityId.toString(), "The requested entity should have been retrieved.");
	}

	private void assertNotFound(HttpRequest request, String msg) {
		try {
			mintakaTestClient.toBlocking().retrieve(request, Map.class);
		} catch (HttpClientResponseException e) {
			if (!e.getStatus().equals(HttpStatus.NOT_FOUND)) {
				fail(msg);
			}
		}
	}

	// helper

	private List<Map<String, Object>> retrieveListRepresentationForProperty(String property, Map<String, Object> entityTemporalMap) {
		Object temporalProperty = entityTemporalMap.get(property);
		assertNotNull(temporalProperty, "All entities should have been retrieved.");
		assertTrue(temporalProperty instanceof List, "A list of the properties should have been retrieved.");
		return (List) temporalProperty;
	}


	// create
	private void createMovingEntity(URI entityId) {
		when(clock.instant()).thenReturn(START_TIME_STAMP);

		double lat = 0;
		double longi = 0;

		Instant currentTime = START_TIME_STAMP;
		PointVO pointVO = new PointVO();
		pointVO.type(PointVO.Type.POINT);
		pointVO.coordinates().add(lat);
		pointVO.coordinates().add(longi);
		GeoPropertyVO pointProperty = getNewGeoProperty();
		pointProperty.value(pointVO);
		EntityVO entityVO = new EntityVO()
				.atContext(CORE_CONTEXT)
				.id(entityId)
				.location(pointProperty)
				.observationSpace(null)
				.operationSpace(null)
				.type("store");
		PropertyVO temperatureProperty = getNewPropety().value(Math.random());
		entityVO.setAdditionalProperties(Map.of("temperature", temperatureProperty));
		entitiesApiTestClient.createEntity(entityVO);

		for (int i = 0; i < 200; i++) {
			lat++;
			longi++;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi);
		}
		for (int i = 200; i < 300; i++) {
			lat--;
			longi--;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi);
		}
		for (int i = 300; i < 400; i++) {
			lat--;
			longi--;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi);
		}
	}

	private void updateLatLong(URI entityId, double lat, double longi) {
		PointVO pointVO;
		GeoPropertyVO pointProperty;
		PropertyVO temperatureProperty;
		pointVO = new PointVO();
		pointVO.type(PointVO.Type.POINT);
		pointVO.coordinates().add(lat);
		pointVO.coordinates().add(longi);
		pointProperty = getNewGeoProperty();
		pointProperty.value(pointVO);

		EntityFragmentVO entityFragmentVO = new EntityFragmentVO().atContext(CORE_CONTEXT)
				.location(pointProperty)
				.observationSpace(null)
				.operationSpace(null)
				.type("store");
		temperatureProperty = getNewPropety().value(Math.random());
		entityFragmentVO.setAdditionalProperties(Map.of("temperature", temperatureProperty));
		entitiesApiTestClient.updateEntityAttrs(entityId, entityFragmentVO);
	}


	private void createEntityHistoryWithDeletion() {
		createEntityHistory(DELETED_ENTITY_ID, START_TIME_STAMP);
		entitiesApiTestClient.removeEntityById(DELETED_ENTITY_ID, null);
	}

	private void createEntityHistory(URI entityId, Instant startTimeStamp) {
		when(clock.instant()).thenReturn(startTimeStamp);

		Instant currentTime = startTimeStamp;
		EntityVO entityVO = new EntityVO()
				.atContext(CORE_CONTEXT)
				.id(entityId)
				.location(null)
				.observationSpace(null)
				.operationSpace(null)
				.type("store");

		entityVO.setAdditionalProperties(getAdditionalProperties());

		try {
			entitiesApiTestClient.createEntity(entityVO);
		} catch (HttpClientResponseException e) {
			if (e.getStatus().equals(HttpStatus.CONFLICT)) {
				// db is already initialized
				return;
			}
			throw new RuntimeException("Was not able to initialize data.");
		}

		for (int i = 0; i < NUMBER_OF_UPDATES; i++) {
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			// evolve over time
			EntityFragmentVO entityFragmentVO = new EntityFragmentVO().atContext(CORE_CONTEXT)
					.location(null)
					.observationSpace(null)
					.operationSpace(null)
					.type("store");
			entityFragmentVO.setAdditionalProperties(getAdditionalProperties());
			entitiesApiTestClient.updateEntityAttrs(entityId, entityFragmentVO);
		}
	}

	private Map<String, Object> getAdditionalProperties() {
		RelationshipVO relationshipVO = getNewRelationship()._object(URI.create("my:related:entity"));


		PropertyVO propertyWithSubProperty = getNewPropety().value("propWithSubProp");
		propertyWithSubProperty.setAdditionalProperties(Map.of("mySubProperty", getNewPropety().value("subValue")));

		PropertyVO temperatureProperty = getNewPropety().value(10.3);
		PropertyVO openProperty = getNewPropety().value(true);
		PropertyVO storeNameProperty = getNewPropety().value("myStore");
		PointVO pointVO = new PointVO();
		pointVO.type(PointVO.Type.POINT);
		pointVO.coordinates().add(123.0);
		pointVO.coordinates().add(321.0);
		GeoPropertyVO pointProperty = getNewGeoProperty();
		pointProperty.value(pointVO);

		GeoPropertyVO polygonProperty = getNewGeoProperty();
		PolygonVO polygonVO = new PolygonVO();
		polygonVO.type(PolygonVO.Type.POLYGON);
		polygonVO.coordinates().add(
				getLinearRingDef(
						List.of(
								getPositionDef(-10, -10),
								getPositionDef(10, -10),
								getPositionDef(10, 10),
								getPositionDef(-10, 10),
								getPositionDef(-10, -10))));
		polygonProperty.value(polygonVO);

		GeoPropertyVO multiPolygonProperty = getNewGeoProperty();
		multiPolygonProperty.value(getMultiPolygon());

		GeoPropertyVO lineStringProperty = getNewGeoProperty();
		LineStringVO lineStringVO = new LineStringVO();
		lineStringVO.type(LineStringVO.Type.LINESTRING);
		lineStringVO.coordinates().addAll(
				List.of(
						getPositionDef(0, 1),
						getPositionDef(5, 10),
						getPositionDef(10, 15),
						getPositionDef(15, 20),
						getPositionDef(20, 25)));
		lineStringProperty.value(lineStringVO);

		GeoPropertyVO multiLineStringProperty = getNewGeoProperty();
		multiLineStringProperty.value(getMultiLineString());

		return Map.of(
				"temperature", temperatureProperty,
				"open", openProperty,
				"storeName", storeNameProperty,
				"polygon", polygonProperty,
				"multiPolygon", multiPolygonProperty,
				"lineString", lineStringProperty,
				"multiLineString", multiLineStringProperty,
				"propertyWithSubProperty", propertyWithSubProperty,
				"relatedEntity", relationshipVO);
	}

	private GeoPropertyVO getNewGeoProperty() {
		return new GeoPropertyVO().type(GeoPropertyVO.Type.GEOPROPERTY).observedAt(clock.instant());
	}

	private PropertyVO getNewPropety() {
		return new PropertyVO().type(PropertyVO.Type.PROPERTY).observedAt(clock.instant());
	}

	private RelationshipVO getNewRelationship() {
		return new RelationshipVO().type(RelationshipVO.Type.RELATIONSHIP).observedAt(clock.instant());
	}

	private LinearRingDefinitionVO getLinearRingDef(List<PositionDefinitionVO> positionDefinitionVOS) {
		LinearRingDefinitionVO linearRingDefinitionVO = new LinearRingDefinitionVO();
		linearRingDefinitionVO.addAll(positionDefinitionVOS);
		return linearRingDefinitionVO;
	}

	private PositionDefinitionVO getPositionDef(double lng, double lat) {
		PositionDefinitionVO positionDefinitionVO = new PositionDefinitionVO();
		positionDefinitionVO.add(lng);
		positionDefinitionVO.add(lat);
		return positionDefinitionVO;
	}

	private MultiPolygonVO getMultiPolygon() {
		PolygonDefinitionVO p1 = new PolygonDefinitionVO();
		p1.add(
				getLinearRingDef(
						List.of(
								getPositionDef(102.0, 2.0),
								getPositionDef(103.0, 2.0),
								getPositionDef(103.0, 3.0),
								getPositionDef(102.0, 3.0),
								getPositionDef(102.0, 2.0))));
		PolygonDefinitionVO p2 = new PolygonDefinitionVO();
		p2.add(
				getLinearRingDef(
						List.of(
								getPositionDef(100.0, 0.0),
								getPositionDef(101.0, 0.0),
								getPositionDef(101.0, 1.0),
								getPositionDef(100.0, 1.0),
								getPositionDef(100.0, 0.0))));
		PolygonDefinitionVO p3 = new PolygonDefinitionVO();
		p3.add(
				getLinearRingDef(
						List.of(
								getPositionDef(100.2, 0.2),
								getPositionDef(100.8, 0.2),
								getPositionDef(100.8, 0.8),
								getPositionDef(100.2, 0.8),
								getPositionDef(100.2, 0.2))));


		MultiPolygonVO multiPolygonVO = new MultiPolygonVO();
		multiPolygonVO.coordinates(List.of(p1, p2, p3));
		multiPolygonVO.type(MultiPolygonVO.Type.MULTIPOLYGON);

		return multiPolygonVO;
	}

	private MultiLineStringVO getMultiLineString() {
		LineStringDefinitionVO l1 = new LineStringDefinitionVO();
		l1.addAll(
				List.of(
						getPositionDef(1, 2),
						getPositionDef(5, 15),
						getPositionDef(10, 20)));
		LineStringDefinitionVO l2 = new LineStringDefinitionVO();
		l2.addAll(
				List.of(
						getPositionDef(10, 20),
						getPositionDef(50, 150),
						getPositionDef(100, 200)));
		LineStringDefinitionVO l3 = new LineStringDefinitionVO();
		l3.addAll(
				List.of(
						getPositionDef(210, 220),
						getPositionDef(250, 2150),
						getPositionDef(2100, 2200)));
		MultiLineStringVO multiLineStringVO = new MultiLineStringVO();
		multiLineStringVO.coordinates(List.of(l1, l2, l3));
		multiLineStringVO.type(MultiLineStringVO.Type.MULTILINESTRING);
		return multiLineStringVO;
	}


}
