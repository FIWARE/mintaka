package org.fiware.mintaka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.extern.slf4j.Slf4j;
import org.fiware.ngsi.api.EntitiesApiTestClient;
import org.fiware.ngsi.api.TemporalRetrievalApiTestClient;
import org.fiware.ngsi.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@MicronautTest
@Slf4j
public class ComposeTest {

	@Inject
	private TemporalRetrievalApiTestClient temporalRetrievalApiTestClient;

	@Inject
	private ObjectMapper objectMapper;

	@Inject
	private EntitiesApiTestClient entitiesApiTestClient;

	private Instant currentTime = Instant.now();
	private Clock clock;

	@BeforeEach
	public void setup() {
		clock = mock(Clock.class);
		when(clock.instant()).thenReturn(currentTime);
	}

	@Test
	public void test() throws JsonProcessingException {

		URI context = URI.create("https://fiware.github.io/data-models/context.jsonld");

		URI entityId = URI.create("urn:ngsi-ld:store:4" + UUID.randomUUID().toString());

		EntityVO entityVO = new EntityVO()
				.atContext(context)
				.id(entityId)
				.location(null)
				.observationSpace(null)
				.operationSpace(null)
				.type("store");

		entityVO.setAdditionalProperties(getAdditionalProperties());
		assertEquals(entitiesApiTestClient.createEntity(entityVO).getStatus(), HttpStatus.CREATED, "The entity should have been created.");

		for (int i = 0; i < 200; i++) {
			currentTime = currentTime.plus(1, ChronoUnit.MINUTES);
			when(clock.instant()).thenReturn(currentTime);
			// evolve over time
			EntityFragmentVO entityFragmentVO = new EntityFragmentVO().atContext(context)
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
