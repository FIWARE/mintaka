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
import org.junit.jupiter.params.provider.Arguments;
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
import static org.mockito.Mockito.*;

@Slf4j
public abstract class ComposeTest {

	protected static final AtomicBoolean SETUP = new AtomicBoolean(false);
	protected static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
	protected static final ExecutorService PARALLEL_INITIALIZER_SERVICE = Executors.newCachedThreadPool();

	protected static final URI ENTITY_ID = URI.create("urn:ngsi-ld:store:" + UUID.randomUUID().toString());
	protected static final URI DELETED_ENTITY_ID = URI.create("urn:ngsi-ld:store:deleted-" + UUID.randomUUID().toString());
	protected static final URI CREATED_AFTER_ENTITY_ID = URI.create("urn:ngsi-ld:store:after-" + UUID.randomUUID().toString());
	protected static final URI DATA_MODEL_CONTEXT = URI.create("https://fiware.github.io/data-models/context.jsonld");
	protected static final URI CORE_CONTEXT = URI.create("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld");
	protected static final String ORION_LD_HOST = "orion-ld";
	protected static final String TIMESCALE_HOST = "orion-ld";
	protected static final int ORION_LD_PORT = 1026;
	protected static final int TIMESCALE_PORT = 5432;
	//2 hours of updates
	public static final int NUMBER_OF_UPDATES = 120;

	private static final DockerComposeContainer DOCKER_COMPOSE_CONTAINER = new DockerComposeContainer(new File("src/test/resources/docker-compose/docker-compose-it.yml"))
			.withExposedService("orion-ld", ORION_LD_PORT)
			.withExposedService("timescale", TIMESCALE_PORT);

	{
		synchronized (SETUP) {
			DOCKER_COMPOSE_CONTAINER.waitingFor(ORION_LD_HOST, new HttpWaitStrategy()
					.withReadTimeout(Duration.of(1, ChronoUnit.MINUTES)).forPort(ORION_LD_PORT).forPath("/version"));
		}
	}

	protected static final Instant START_TIME_STAMP = Instant.ofEpochMilli(0);
	protected Clock clock;
	protected static EmbeddedServer embeddedServer;
	protected static ApplicationContext applicationContext;

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	protected HttpClient mintakaTestClient;
	protected EntitiesApiTestClient entitiesApiTestClient;
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
		synchronized (SETUP) {
			if (!SETUP.getAndSet(true)) {
				DOCKER_COMPOSE_CONTAINER.start();
			}
		}

		embeddedServer = ApplicationContext.run(EmbeddedServer.class, PropertySource.of(
				"test", Map.of("datasource.default.host", DOCKER_COMPOSE_CONTAINER.getServiceHost(ORION_LD_HOST, ORION_LD_PORT),
						"datasource.default.port", DOCKER_COMPOSE_CONTAINER.getServicePort(ORION_LD_HOST, ORION_LD_PORT)
				)));

		applicationContext = embeddedServer.getApplicationContext();

	}

	@BeforeEach
	public void setup() throws MalformedURLException {
		entitiesApiTestClient = applicationContext.getBean(EntitiesApiTestClient.class);
		clock = mock(Clock.class);

		synchronized (INITIALIZED) {
			if (!INITIALIZED.getAndSet(true)) {
				List<Future> futureList = new ArrayList<>();
				futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createMovingEntity(URI.create("urn:ngsi-ld:car:moving-car-2"))));
				futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createMovingEntity(URI.create("urn:ngsi-ld:car:moving-car"))));
				futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistory(ENTITY_ID, START_TIME_STAMP)));
				futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistoryWithDeletion()));
				futureList.add(PARALLEL_INITIALIZER_SERVICE.submit(() -> createEntityHistory(CREATED_AFTER_ENTITY_ID, START_TIME_STAMP.plus(1, ChronoUnit.YEARS))));
				await().atMost(5, TimeUnit.MINUTES).until(() -> !futureList.stream().anyMatch(f -> !f.isDone()));
			}
		}


		HttpClientConfiguration configuration = new DefaultHttpClientConfiguration();
		//high timeout is required, because github-action runners are not that powerful
		configuration.setReadTimeout(Duration.ofSeconds(30));
		mintakaTestClient = new DefaultHttpClient(new URL("http://" + embeddedServer.getHost() + ":" + embeddedServer.getPort()), configuration);
	}



	/*
	 *  TEST SUPPORT
	 */

	// provider methods

	protected static Stream<Arguments> provideFullEntityAttributeStrings() {
		return FULL_ENTITY_ATTRIBUTES_LIST
				.stream()
				.flatMap(attribute -> Stream.of(Arguments.of(attribute, ENTITY_ID), Arguments.of(attribute, DELETED_ENTITY_ID)));
	}

	protected static Stream<Arguments> provideCombinedAttributeStrings() {
		return Lists.partition(FULL_ENTITY_ATTRIBUTES_LIST, 3)
				.stream()
				.flatMap(attribute -> Stream.of(Arguments.of(attribute, ENTITY_ID), Arguments.of(attribute, DELETED_ENTITY_ID)));
	}

	protected static Stream<Arguments> provideEntityIds() {
		return Stream.of(Arguments.of(ENTITY_ID), Arguments.of(DELETED_ENTITY_ID));
	}

	// assertions

	protected void assertAttributesBetween(List<String> attributesList, URI entityId) {
		assertAttributesBetweenWithLastN(attributesList, null, entityId);
	}

	protected void assertAttributesBetweenWithLastN(List<String> attributesList, Integer lastN, URI entityId) {
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

	protected void assertAttributesBefore(List<String> attributesList) {
		assertAttributesBeforeWithLastN(attributesList, null);
	}

	protected void assertAttributesBeforeWithLastN(List<String> attributesList, Integer lastN) {
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

	protected void assertAttributesAfter(List<String> attributesList) {
		assertAttributesAfterWithLastN(attributesList, null);
	}

	protected void assertAttributesAfterWithLastN(List<String> attributesList, Integer lastN) {
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

	protected void assertAttributesInMap(Map<String, Object> entityTemporalMap, List<String> attributesList, Integer expectedInstances, Instant startTimeStamp, Instant endTimeStamp) {
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


	protected void assertInstanceInTimeFrame(List<Map<String, Object>> attributeInstanceList, Integer expectedInstancs, Instant startTimeStamp, Instant endTimeStamp) {
		List<Instant> observedAtList = attributeInstanceList
				.stream()
				.map(ai -> (String) ai.get("observedAt"))
				.map(timeString -> OffsetDateTime.parse(timeString).toInstant())
				.sorted(Comparator.naturalOrder())
				.collect(Collectors.toList());

		assertEquals(startTimeStamp, observedAtList.get(0), "The attribute list should start at the initial timestamp.");
		assertEquals(endTimeStamp, observedAtList.get(expectedInstancs - 1), "The attribute list should end with the last element.");

	}

	protected void assertDefaultStoreTemporalEntity(URI entityId, Map<String, Object> entityTemporalMap) {
		assertNotNull(entityTemporalMap, "A temporal entity should have been returned.");

		assertEquals(entityTemporalMap.get(JsonLdConsts.CONTEXT),
				"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
				"The core context should be present if nothing else is requested.");
		assertEquals(entityTemporalMap.get("type"), "store", "The correct type of the entity should be retrieved.");
		assertEquals(entityTemporalMap.get("id"), entityId.toString(), "The requested entity should have been retrieved.");
	}

	protected void assertNotFound(HttpRequest request, String msg) {
		try {
			mintakaTestClient.toBlocking().retrieve(request, Map.class);
		} catch (HttpClientResponseException e) {
			if (!e.getStatus().equals(HttpStatus.NOT_FOUND)) {
				fail(msg);
			}
		}
	}

	// helper

	protected List<Map<String, Object>> retrieveListRepresentationForProperty(String property, Map<String, Object> entityTemporalMap) {
		Object temporalProperty = entityTemporalMap.get(property);
		assertNotNull(temporalProperty, "All entities should have been retrieved.");
		assertTrue(temporalProperty instanceof List, "A list of the properties should have been retrieved.");
		return (List) temporalProperty;
	}


	// create
	protected void createMovingEntity(URI entityId) {
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
				.type("car");
		PropertyVO temperatureProperty = getNewPropety().value(25);
		PropertyVO radioProperty = getNewPropety().value(true);
		PropertyVO driverProperty = getNewPropety().value("Stefan");
		entityVO.setAdditionalProperties(Map.of("radio", radioProperty, "temperature", temperatureProperty, "driver", driverProperty));
		entitiesApiTestClient.createEntity(entityVO);

		for (int i = 0; i < 100; i++) {
			lat++;
			longi++;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi, Optional.of(25), Optional.of(true), Optional.of("Stefan"));
		}
		for (int i = 100; i < 200; i++) {
			lat++;
			longi++;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi, Optional.of(25), Optional.of(true), Optional.of("Mira"));
		}
		for (int i = 200; i < 300; i++) {
			lat--;
			longi--;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi, Optional.of(20), Optional.of(false), Optional.of("Franzi"));
		}
		for (int i = 300; i < 400; i++) {
			lat--;
			longi--;
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			updateLatLong(entityId, lat, longi, Optional.of(15), Optional.of(true), Optional.empty());
		}
	}

	protected void updateLatLong(URI entityId, double lat, double longi, Optional<Integer> optionalTemp, Optional<Boolean> optionalRadio, Optional<String> optionalDriver) {
		PointVO pointVO;
		GeoPropertyVO pointProperty;
		PropertyVO temperatureProperty;
		PropertyVO radioProperty;
		PropertyVO driverProperty;
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
		temperatureProperty = getNewPropety().value(optionalTemp.orElseGet(() -> (int) (Math.random() * 10)));
		radioProperty = getNewPropety().value(optionalRadio.orElse(true));
		driverProperty = getNewPropety().value(optionalDriver.orElse("unknown"));

		entityFragmentVO.setAdditionalProperties(Map.of("temperature", temperatureProperty, "radio", radioProperty, "driver", driverProperty));
		entitiesApiTestClient.updateEntityAttrs(entityId, entityFragmentVO);
	}


	protected void createEntityHistoryWithDeletion() {
		createEntityHistory(DELETED_ENTITY_ID, START_TIME_STAMP);
		entitiesApiTestClient.removeEntityById(DELETED_ENTITY_ID, null);
	}

	protected void createEntityHistory(URI entityId, Instant startTimeStamp) {
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

	protected Map<String, Object> getAdditionalProperties() {
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

	protected GeoPropertyVO getNewGeoProperty() {
		return new GeoPropertyVO().type(GeoPropertyVO.Type.GEOPROPERTY).observedAt(clock.instant());
	}

	protected PropertyVO getNewPropety() {
		return new PropertyVO().type(PropertyVO.Type.PROPERTY).observedAt(clock.instant());
	}

	protected RelationshipVO getNewRelationship() {
		return new RelationshipVO().type(RelationshipVO.Type.RELATIONSHIP).observedAt(clock.instant());
	}

	protected LinearRingDefinitionVO getLinearRingDef(List<PositionDefinitionVO> positionDefinitionVOS) {
		LinearRingDefinitionVO linearRingDefinitionVO = new LinearRingDefinitionVO();
		linearRingDefinitionVO.addAll(positionDefinitionVOS);
		return linearRingDefinitionVO;
	}

	protected PositionDefinitionVO getPositionDef(double lng, double lat) {
		PositionDefinitionVO positionDefinitionVO = new PositionDefinitionVO();
		positionDefinitionVO.add(lng);
		positionDefinitionVO.add(lat);
		return positionDefinitionVO;
	}

	protected MultiPolygonVO getMultiPolygon() {
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

	protected MultiLineStringVO getMultiLineString() {
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
