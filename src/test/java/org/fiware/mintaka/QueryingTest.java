package org.fiware.mintaka;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.Mapping;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class QueryingTest extends ComposeTest {

	@DisplayName("Test running a temporal query including geo querying without timerel.")
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	public void testTempWithGeoQueryInTempValues(boolean setLastN) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "LineString")
				.add("coordinates", "[[5,5],[7,7]]");

		if (setLastN) {
			getRequest.getParameters().add("lastN", "10");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// we expect the two cars to be inside the area two times each, between 00:04 and 00:08 and again between 06:32 and 06.36
		List expectedValues = List.of("1970-01-01T00:05:00Z", "1970-01-01T00:06:00Z", "1970-01-01T00:07:00Z", "1970-01-01T00:08:00Z", "1970-01-01T00:09:00Z",
				"1970-01-01T06:33:00Z", "1970-01-01T06:34:00Z", "1970-01-01T06:35:00Z", "1970-01-01T06:36:00Z", "1970-01-01T06:37:00Z");

		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			assertEquals(expectedValues, temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList()), "All time elements should be present.");
		});
	}

	@DisplayName("Test running a temporal query including an ids and type.")
	@ParameterizedTest
	@MethodSource("provideCarIds")
	public void testTempWithIdAndTypeInTempValues(List<URI> ids) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("id", ids.stream().map(Object::toString).collect(Collectors.joining(",")))
				.add("type", "car")
				.add("attrs", "driver");

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);

		assertEquals(ids.size(), entryList.size(), "The matching entity should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("driver"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("driver");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(301, timeList.size(), "All 300 entries should be present.");
			assertEquals("1970-01-01T00:00:00Z", timeList.get(0), "The first entry should be present");
			assertEquals("1970-01-01T05:00:00Z", timeList.get(300), "The last entry should be present");
		});
	}

	@DisplayName("Test running a temporal query including geo querying.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithGeoQueryInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("type", "car")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "LineString")
				.add("coordinates", "[[5,5],[7,7]]");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// we expect the two cars to be inside the area two times each, between 00:04 and 00:08 and again between 06:32 and 06.36
		List expectedValues = List.of("1970-01-01T00:05:00Z", "1970-01-01T00:06:00Z", "1970-01-01T00:07:00Z", "1970-01-01T00:08:00Z", "1970-01-01T00:09:00Z",
				"1970-01-01T06:33:00Z", "1970-01-01T06:34:00Z", "1970-01-01T06:35:00Z", "1970-01-01T06:36:00Z", "1970-01-01T06:37:00Z");

		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("temperature");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("temperature");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(expectedValues, timeList, "All time elements should be present.");
		});
	}

	@DisplayName("Test running a temporal query including ngsi querying.")
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	public void testTempWithNgsiQueryInTempValues(boolean setLastN) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("attrs", "temperature,driver")
				.add("q", "temperature>20")
				.add("timerel", "between")
				.add("timeAt", "1970-01-01T00:01:00Z")
				.add("endTimeAt", "1970-01-01T07:30:00Z");

		if (setLastN) {
			getRequest.getParameters().add("lastN", "199");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// temp should only be high enough until
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(199, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T03:20:00Z", timeList.get(198), "Last time should be at the half");
			assertEquals("1970-01-01T00:02:00Z", timeList.get(0), "First time should be at the start");
		});
	}

	@DisplayName("Test running a temporal query including ngsi AND querying.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiAndQueryInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("attrs", "temperature,radio")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature<18;radio==true");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}
		// get car when radio was on and temp was below 18 -> only the time at the end of the time window
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("temperature");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("temperature");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(100, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:40:00Z", timeList.get(99), "Last time should be at the half");
			assertEquals("1970-01-01T05:01:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi AND querying and geo query.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiAndQueryAndGeoInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature<18;radio==true")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "LineString")
				.add("coordinates", "[[5,5],[7,7]]");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}
		// get car when radio was on and temp was below 18 and ts near [[5,5],[7,7]]-> only the short time frame at the end of the time window
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("temperature");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("temperature");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(5, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:37:00Z", timeList.get(4), "Last time should be at the half");
			assertEquals("1970-01-01T06:33:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi OR querying.")
	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	public void testTempWithNgsiORQueryInTempValues(boolean setLastN) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("attrs", "temperature")
				.add("q", "temperature>20|radio==false")
				.add("timerel", "between")
				.add("timeAt", "1970-01-01T00:01:00Z")
				.add("endTimeAt", "1970-01-01T07:30:00Z");

		if (setLastN) {
			getRequest.getParameters().add("lastN", "299");
		}

		// get car when radio was of or temp was above 20 -> first ~3/4 of the drive
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(299, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T05:00:00Z", timeList.get(298), "Last time should be at the half");
			assertEquals("1970-01-01T00:02:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi OR and AND querying.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiORandANDQueryInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("attrs", "temperature,radio,driver")
				.add("type", "car")
				.add("q", "(temperature>20|radio==false);driver==\"Mira\"");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}
		// get car when radio was of or temp was above 20 and the driver was "mira" -> second quarter of the drive
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("temperature");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("temperature");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(100, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T03:20:00Z", timeList.get(99), "Last time should be at the half");
			assertEquals("1970-01-01T01:41:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi OR and AND querying with subattrs.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiORandANDQueryWithSubAttrsInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "(temperature>20|radio==false);driver==\"Mira\";motor.fuel!=0.7");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}
		// get car when radio was off or temp was above 20 and the driver was "mira" and fuel was not 0.7 -> entry 100-150
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			assertTrue(entry.containsKey("motor"), "The motor property should be present.");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("temperature");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("temperature");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(50, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T02:30:00Z", timeList.get(49), "Last time should be at the half of the second quarter");
			assertEquals("1970-01-01T01:41:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query with ngsi range.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiRangeInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("attrs", "driver,motor")
				.add("type", "car")
				.add("q", "motor.fuel==0.6..0.8");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}
		// get car when fuel is between 0.6 and 0.8 -> the 200 entries in the middle
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("motor");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> property = (Map<String, List>) entry.get("motor");
				List<List> temporalValuesList = property.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals(200, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T05:00:00Z", timeList.get(199), "Last time should be at the half of the end of 3rd quarter");
			assertEquals("1970-01-01T01:41:00Z", timeList.get(0), "First time should be at the start at 2nd quarter");
		});
	}

	@DisplayName("Test running a temporal query with ngsi list.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndBefore")
	public void testTempWithNgsiListInTempValues(TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("idPattern", ".*")
				.add("attrs", "driver,motor")
				.add("type", "car")
				.add("q", "driver==[\"Mira\",\"Franzi\"]");

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}
		if (timeRelation == TimeRelation.BEFORE) {
			getRequest.getParameters()
					.add("timerel", "before")
					.add("timeAt", "1970-01-01T07:30:00Z");
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T00:01:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}

		// get car when the requested drivers where set -> the 200 entries for Franzi, 100 for Mira
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");

		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			assertTrue(entry.containsKey("motor"), "The motor property should be present.");
			String entityId = (String) entry.get("id");
			List timeList;
			if (sysAttrs.isPresent()) {
				List<Map> properties = (List<Map>) entry.get("driver");
				timeList = properties.stream()
						.peek(property -> assertNotNull(property.get("createdAt")))
						.peek(property -> assertNotNull(property.get("modifiedAt")))
						.map(property -> property.get("observedAt")).sorted().collect(Collectors.toList());
			} else {
				Map<String, List> driverProperty = (Map<String, List>) entry.get("driver");
				List<List> temporalValuesList = driverProperty.get("values");
				timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			}
			assertEquals("1970-01-01T01:41:00Z", timeList.get(0), "First time should be at the start at 2nd quarter");
			if (entityId.equals(CAR_2_ID.toString())) {
				assertEquals(100, timeList.size(), "All expected times should be present");
				assertEquals("1970-01-01T03:20:00Z", timeList.get(99), "Last time should be at the half of the end of 3rd quarter");
			} else if (entityId.equals(CAR_1_ID.toString())) {
				assertEquals(200, timeList.size(), "All expected times should be present");
				assertEquals("1970-01-01T05:00:00Z", timeList.get(199), "Last time should be at the half of the end of 3rd quarter");
			} else {
				fail("No other entity should have been returned.");
			}
		});

	}

	@DisplayName("Test running a temporal query with ngsi json value.")
	@Test
	public void testTempWithNgsiJsonInTempValues() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "driver,motor")
				.add("type", "car")
				.add("q", "trunk[cases]==3")
				.add("timerel", "between")
				.add("timeAt", "1970-01-01T00:01:00Z")
				.add("endTimeAt", "1970-01-01T07:30:00Z");
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			assertTrue(entry.containsKey("motor"), "The motor property should be present.");
			Map<String, List> temporalPropertyMap = (Map<String, List>) entry.get("motor");
			List<List> temporalValuesList = temporalPropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(199, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:40:00Z", timeList.get(198), "Last time should be at the end");
			assertEquals("1970-01-01T00:02:00Z", timeList.get(0), "First time should be at the start");
		});
	}

	@DisplayName("Test running a temporal query with without attribute present in the req timeframe.")
	@ParameterizedTest
	@MethodSource("getRelationsBetweenAndAfter")
	public void testTempWithNgsiQueryNoAttrInReqTimeFrameInTempValues(TimeRelation relation) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "driver,motor")
				.add("type", "car")
				.add("q", "driver==\"Franzi\"");

		if (relation == TimeRelation.AFTER) {
			getRequest.getParameters()
					.add("timerel", "after")
					.add("timeAt", "1970-01-01T03:30:00Z");
		}
		if (relation == TimeRelation.BETWEEN) {
			getRequest.getParameters()
					.add("timerel", "between")
					.add("timeAt", "1970-01-01T03:30:00Z")
					.add("endTimeAt", "1970-01-01T07:30:00Z");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(1, entryList.size(), "Only the entity with driver 'Franzi' should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			assertTrue(entry.containsKey("motor"), "The motor property should be present.");
			Map<String, List> temporalPropertyMap = (Map<String, List>) entry.get("motor");
			List<List> temporalValuesList = temporalPropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(190, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:40:00Z", timeList.get(189), "Last time should be at the end");
			assertEquals("1970-01-01T03:31:00Z", timeList.get(0), "First time should be at half");
		});
	}

	@DisplayName("Test count option")
	@Test
	public void testCountOptionSet() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "temperature")
				.add("options", "count")
				.add("pageSize", "2");
		HttpResponse<List<Map<String, Object>>> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The request should succeed.");
		assertNotNull(entryListResponse.getHeaders().get("NGSILD-Total-Count"), "Count should be present");
		assertEquals("5", entryListResponse.getHeaders().get("NGSILD-Total-Count"), "5 entites should match.");
	}

	@DisplayName("Retrieve query results with entity pagination")
	@Test
	public void testEntityPagination() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "temperature")
				.add("pageSize", "2");
		List<String> allReturnedEntities = new ArrayList<>();
		HttpResponse<List<Map<String, Object>>> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The request should succeed.");
		assertNull(entryListResponse.getHeaders().get("Previous-Page"), "First page should have no previous");
		assertEquals("2", entryListResponse.getHeaders().get("Page-Size"), "A header indicating the total number should be returend");
		assertEquals(2, entryListResponse.body().size(), "Only two of the entities should have been returned");
		entryListResponse.body().stream().forEach(entry -> allReturnedEntities.add((String) entry.get("id")));

		String nextPageAnchor = entryListResponse.getHeaders().get("Next-Page");

		getRequest.getParameters().add("pageAnchor", nextPageAnchor);
		entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The request should succeed.");
		assertNotNull(entryListResponse.getHeaders().get("Previous-Page"), "Previous page should have been returned.");
		assertEquals("2", entryListResponse.getHeaders().get("Page-Size"), "A header indicating the total number should be returend");
		assertEquals(2, entryListResponse.body().size(), "Only two of the entities should have been returned");
		entryListResponse.body().stream().forEach(entry -> allReturnedEntities.add((String) entry.get("id")));
		nextPageAnchor = entryListResponse.getHeaders().get("Next-Page");

		getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "temperature")
				.add("pageSize", "2")
				.add("pageAnchor", nextPageAnchor);
		entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The request should succeed.");
		assertEquals(1, entryListResponse.body().size(), "Only the one missing entity should be returend.");
		assertNull(entryListResponse.getHeaders().get("Next-Page"), "Last page should have no next.");
		entryListResponse.body().stream().forEach(entry -> allReturnedEntities.add((String) entry.get("id")));
	}

	@DisplayName("Retrieve query results with temporal pagination.")
	@Test
	public void testTemporalPaginationQuery() {
		// we have 2 entities and do not limit the number of attributes to be retrieved(default of 10 is assumed) -> MAX_LIMIT(1000) / (2 * 10) -> 50 instances each
		String expectedRangeHeader = "date-time 1970-01-01T00:00-1970-01-01T00:49/*";
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car");

		HttpResponse<List<Map<String, Object>>> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.PARTIAL_CONTENT, entryListResponse.getStatus(), "Only parts of the history should be returned.");
		assertEquals(expectedRangeHeader, entryListResponse.getHeaders().get("Content-Range"), "The range should have been limited to the first 166 entries.");
	}

	@DisplayName("Query for non existing values")
	@Test
	public void testQueryForNonExisting() {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("type", "doesNotExist");
		HttpResponse<List> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The result should be ok.");
		assertTrue(entryListResponse.getBody().get().isEmpty(), "The result should be an empty list.");
	}

	private static Stream<Arguments> getRelationsBetweenAndAfter() {
		return Stream.of(Arguments.of(TimeRelation.BETWEEN), Arguments.of(TimeRelation.AFTER));
	}

	private static Stream<Arguments> getRelationsBetweenAndBefore() {
		return Stream.of(
				Arguments.of(TimeRelation.BETWEEN, Optional.of("sysAttrs")),
				Arguments.of(TimeRelation.BEFORE, Optional.of("sysAttrs")),
				Arguments.of(TimeRelation.BETWEEN, Optional.empty()),
				Arguments.of(TimeRelation.BEFORE, Optional.empty()));
	}

	private static Stream<Arguments> provideCarIds() {
		return Stream.of(Arguments.of(List.of(CAR_1_ID, CAR_2_ID)), Arguments.of(List.of(CAR_1_ID)), Arguments.of(List.of(CAR_2_ID)));
	}
}
