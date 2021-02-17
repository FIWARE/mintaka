package org.fiware.mintaka;

import com.github.jsonldjava.core.JsonLdConsts;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.intThat;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class ComposeTest {

	private static final URI ENTITY_ID = URI.create("urn:ngsi-ld:store:4" + UUID.randomUUID().toString());
	private static final URI DATA_MODEL_CONTEXT = URI.create("https://fiware.github.io/data-models/context.jsonld");
	private static final URI CORE_CONTEXT = URI.create("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld");
	private static final String ORION_LD_HOST = "orion-ld";
	private static final String TIMESCALE_HOST = "orion-ld";
	private static final int ORION_LD_PORT = 1026;
	private static final int TIMESCALE_PORT = 5432;
	//2 hours of updates
	public static final int NUMBER_OF_UPDATES = 120;


	private final Instant startTimeStamp = Instant.ofEpochMilli(0);
	private Clock clock;
	private static EmbeddedServer embeddedServer;
	private static ApplicationContext applicationContext;

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
		when(clock.instant()).thenReturn(startTimeStamp);

		createEntityHistory();

		HttpClientConfiguration configuration = new DefaultHttpClientConfiguration();
		//high timeout is required, because github-action runners are not that powerful
		configuration.setReadTimeout(Duration.ofSeconds(30));
		mintakaTestClient = new DefaultHttpClient(new URL("http://" + embeddedServer.getHost() + ":" + embeddedServer.getPort()), configuration);
	}

	@DisplayName("Retrieve not found for not existing entities")
	@Test
	public void testGetEntityByIdNotFound() {
		try {
			mintakaTestClient.toBlocking().retrieve(HttpRequest.GET("/temporal/entities/rn:ngsi-ld:store:not-found"), Map.class);
		} catch (HttpClientResponseException e) {
			if (!e.getStatus().equals(HttpStatus.NOT_FOUND)) {
				fail("For non existing entities a 404 should be returned.");
			}
		}
	}

	@DisplayName("Retrieve entity without attributes if non-existent is requested.")
	@Test
	public void testGetEntityByIdWithNonExistingAttribute() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", "nonExisting");

		assertEmptyEntityTemporal(mintakaTestClient.toBlocking().retrieve(getRequest, Map.class));

	}

	private void assertEmptyEntityTemporal(Map<String, Object> entityTemporalMap) {
		assertDefaultStoreTemporalEntity(entityTemporalMap);
		assertEquals(entityTemporalMap.size(), 3, "Only id, type and context should have been returned.");
	}

	@DisplayName("Request timeframe before that not exists")
	@Test
	public void  testGetEntityByIdBeforeExists() {

	}

	@DisplayName("Retrieve the full entity. No timeframe definition, default context.")
	@Test
	public void testGetEntityByIdWithoutTime() {
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(HttpRequest.GET("/temporal/entities/" + ENTITY_ID), Map.class);
		assertDefaultStoreTemporalEntity(entityTemporalMap);

		assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");

		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, NUMBER_OF_UPDATES + 1, startTimeStamp, startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with only the requested attribute. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTime(String propertyToRetrieve) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve);

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityTemporalMap);
		assertEquals(entityTemporalMap.size(), 4, "Only id, type, context and the open attribute should have been returned.");
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), NUMBER_OF_UPDATES + 1, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, NUMBER_OF_UPDATES + 1, startTimeStamp, startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the entity with multiple requested attributes. No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityWithMultipleAttributesByIdWithoutTime(List<String> attributesList) {
		String propertyToRetrieve = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve);

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityTemporalMap);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(entityTemporalMap, attributesList, NUMBER_OF_UPDATES + 1, startTimeStamp, startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	// between

	@DisplayName("Retrieve the full entity between the timestamps, default context.")
	@Test
	public void testGetEntityBetweenTimestamps() {
		assertAttributesBetween(FULL_ENTITY_ATTRIBUTES_LIST);
	}

	@DisplayName("Retrieve the entity with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestamps(String attributeName) {
		assertAttributesBetween(List.of(attributeName));
	}

	@DisplayName("Retrieve the entity with the requested attributes between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestamps(List<String> subList) {
		assertAttributesBetween(subList);
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
		assertDefaultStoreTemporalEntity(entityTemporalMap);

		assertEquals(FULL_ENTITY_ATTRIBUTES_LIST.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");

		// NUMBER_OF_UPDATES - lastN + 1 - go 5 steps back but include the 5th last.
		assertAttributesInMap(entityTemporalMap, FULL_ENTITY_ATTRIBUTES_LIST, lastN, startTimeStamp.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
	}

	@DisplayName("Retrieve the last n instances with only the requested attribute . No timeframe definition, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityWithSingleAttributeByIdWithoutTimeAndLastN(String propertyToRetrieve) {
		int lastN = 5;

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters().add("attrs", propertyToRetrieve).add("lastN", String.valueOf(lastN));

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertDefaultStoreTemporalEntity(entityTemporalMap);
		assertEquals(entityTemporalMap.size(), 4, "Only id, type, context and the open attribute should have been returned.");
		List<Map<String, Object>> listRepresentation = retrieveListRepresentationForProperty(propertyToRetrieve, entityTemporalMap);

		assertFalse(listRepresentation.isEmpty(), "There should be some updates for the requested property.");
		assertEquals(listRepresentation.size(), lastN, "All instances should have been returned(created + 100 updates).");

		assertInstanceInTimeFrame(listRepresentation, lastN, startTimeStamp.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
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
		assertDefaultStoreTemporalEntity(entityTemporalMap);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(entityTemporalMap, attributesList, lastN, startTimeStamp.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES), startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
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
	@Test
	public void testGetEntityBetweenTimestampWithLastN() {
		assertAttributesBetweenWithLastN(FULL_ENTITY_ATTRIBUTES_LIST, 5);
	}

	@DisplayName("Retrieve the last n instances for the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideFullEntityAttributeStrings")
	public void testGetEntityAttributesBetweenTimestampsWithLastN(String attributeName) {
		assertAttributesBetweenWithLastN(List.of(attributeName), 5);
	}

	@DisplayName("Retrieve the last n instances with the requested attribute between the timestamps, default context.")
	@ParameterizedTest
	@MethodSource("provideCombinedAttributeStrings")
	public void testGetEntityMultipleAttributesBetweenTimestampsWithLastN(List<String> subList) {
		assertAttributesBetweenWithLastN(subList, 5);
	}

	private void assertAttributesBetween(List<String> attributesList) {
		assertAttributesBetweenWithLastN(attributesList, null);
	}

	private void assertAttributesBetweenWithLastN(List<String> attributesList, Integer lastN) {
		String timerelation = "between";
		String startTime = "1970-01-01T00:30:00Z";
		String endTime = "1970-01-01T00:45:00Z";

		String attributesParam = String.join(",", attributesList);

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/" + ENTITY_ID);
		getRequest.getParameters()
				.add("timerel", timerelation)
				.add("time", startTime)
				.add("endTime", endTime);

		if (attributesList != FULL_ENTITY_ATTRIBUTES_LIST) {
			getRequest.getParameters().add("attrs", attributesParam);
		}

		// start time should be the first after the requested.
		Instant expectedStart = startTimeStamp.plus(31, ChronoUnit.MINUTES);

		if (lastN != null) {
			getRequest.getParameters()
					.add("lastN", String.valueOf(lastN));
			expectedStart = startTimeStamp.plus(45 - lastN, ChronoUnit.MINUTES);
		}

		Integer expectedInstances = lastN != null ? lastN : 14;

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(
				entityTemporalMap,
				attributesList,
				expectedInstances,
				expectedStart,
				startTimeStamp.plus(44, ChronoUnit.MINUTES));
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

		Instant expectedStart = startTimeStamp;

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
				startTimeStamp.plus(29, ChronoUnit.MINUTES));
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
		Instant expectedStart = startTimeStamp.plus(NUMBER_OF_UPDATES - 89, ChronoUnit.MINUTES);

		if (lastN != null) {
			getRequest.getParameters()
					.add("lastN", String.valueOf(lastN));
			// +1, because the last existing update is included
			expectedStart = startTimeStamp.plus(NUMBER_OF_UPDATES - lastN + 1, ChronoUnit.MINUTES);
		}
		Integer expectedInstances = lastN != null ? lastN : 90;

		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(getRequest, Map.class);
		assertEquals(attributesList.size() + 3, entityTemporalMap.size(), "Only id, type, context and the attributes should have been returned.");
		assertAttributesInMap(
				entityTemporalMap,
				attributesList,
				expectedInstances,
				expectedStart,
				startTimeStamp.plus(NUMBER_OF_UPDATES, ChronoUnit.MINUTES));
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

	private static Stream<Arguments> provideFullEntityAttributeStrings() {
		return FULL_ENTITY_ATTRIBUTES_LIST.stream().map(Arguments::of);
	}

	private static Stream<Arguments> provideCombinedAttributeStrings() {
		return Lists.partition(FULL_ENTITY_ATTRIBUTES_LIST, 3).stream().map(Arguments::of);
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


	private List<Map<String, Object>> retrieveListRepresentationForProperty(String property, Map<String, Object> entityTemporalMap) {
		Object temporalProperty = entityTemporalMap.get(property);
		assertNotNull(temporalProperty, "All entities should have been retrieved.");
		assertTrue(temporalProperty instanceof List, "A list of the properties should have been retrieved.");
		return (List) temporalProperty;
	}

	private void assertDefaultStoreTemporalEntity(Map<String, Object> entityTemporalMap) {
		assertNotNull(entityTemporalMap, "A temporal entity should have been returned.");

		assertEquals(entityTemporalMap.get(JsonLdConsts.CONTEXT),
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
				"The core context should be present if nothing else is requested.");
		assertEquals(entityTemporalMap.get("type"), "store", "The correct type of the entity should be retrieved.");
		assertEquals(entityTemporalMap.get("id"), ENTITY_ID.toString(), "The requested entity should have been retrieved.");
	}

	private void createEntityHistory() {
		Instant currentTime = startTimeStamp;
		EntityVO entityVO = new EntityVO()
				.atContext(CORE_CONTEXT)
				.id(ENTITY_ID)
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
			entitiesApiTestClient.updateEntityAttrs(ENTITY_ID, entityFragmentVO);
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
