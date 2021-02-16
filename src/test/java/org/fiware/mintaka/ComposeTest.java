package org.fiware.mintaka;

import com.github.jsonldjava.core.JsonLdConsts;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import lombok.extern.slf4j.Slf4j;
import org.fiware.ngsi.api.EntitiesApiTestClient;
import org.fiware.ngsi.model.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
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
	public static final int NUMBER_OF_UPDATES = 100;

	private EntitiesApiTestClient entitiesApiTestClient;

	private final Instant startTimeStamp = Instant.ofEpochMilli(0);
	private Clock clock;
	private static EmbeddedServer embeddedServer;
	private static ApplicationContext applicationContext;

	private HttpClient mintakaTestClient;

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

		mintakaTestClient = HttpClient.create(new URL("http://" + embeddedServer.getHost() + ":" + embeddedServer.getPort()));
	}

	@DisplayName("Retrieve the full entity with out a timeframe definition in the default context.")
	@Test
	public void testGetEntityByIdWithoutTime() {
		Map<String, Object> entityTemporalMap = mintakaTestClient.toBlocking().retrieve(HttpRequest.GET("/temporal/entities/" + ENTITY_ID), Map.class);
		assertNotNull(entityTemporalMap, "A temporal entity should have been returned.");

		assertEquals(entityTemporalMap.get(JsonLdConsts.CONTEXT), "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", "The core context should be present if nothing else is requested.");
		assertEquals(entityTemporalMap.get("type"), "store", "The correct type of the entity should be retrieved.");
		List.of("temperature", "open", "storeName", "relatedEntity", "lineString", "polygon", "multiPolygon", "multiLineString", "propertyWithSubProperty", "relatedEntity")
				.stream()
				.forEach(propertyName -> {
					Object temporalProperty = entityTemporalMap.get(propertyName);
					assertNotNull(temporalProperty, "All entities should have been retrieved.");
					assertTrue(temporalProperty instanceof List, "A list of the properties should have been retrieved.");
					List<Map<String, Object>> listRepresentation = (List) temporalProperty;
					assertEquals(listRepresentation.size(), NUMBER_OF_UPDATES + 1, "All instances should have been returned(created + 100 updates).");
					listRepresentation.stream().forEach(element -> {
						Object createdAtObj = element.get("createdAt");
						assertNotNull(createdAtObj, "CreatedAt should be present for all elements.");
						assertTrue(createdAtObj instanceof Long, "CreatedAt should be a timestamp.");
					});
				});
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

		entitiesApiTestClient.createEntity(entityVO);

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
		return new GeoPropertyVO().type(GeoPropertyVO.Type.GEOPROPERTY).observedAt(Date.from(clock.instant()));
	}

	private PropertyVO getNewPropety() {
		return new PropertyVO().type(PropertyVO.Type.PROPERTY).observedAt(Date.from(clock.instant()));
	}

	private RelationshipVO getNewRelationship() {
		return new RelationshipVO().type(RelationshipVO.Type.RELATIONSHIP).observedAt(Date.from(clock.instant()));
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
