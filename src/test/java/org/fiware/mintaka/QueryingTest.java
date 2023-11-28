package org.fiware.mintaka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.fiware.ngsi.model.QueryVO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
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
	@MethodSource("provideBooleanValues")
	public void testTempWithGeoQueryInTempValues(RequestType requestType, boolean setLastN) {

		MutableHttpRequest getRequest = getRequest(
				requestType,
				Optional.empty(),
				Optional.of(".*"),
				Optional.of("car"),
				Optional.empty(),
				Optional.empty(),
				Optional.of("near;maxDistance==300000"),
				Optional.of("LineString"),
				Optional.of("[[5,5],[7,7]]"),
				Optional.of("before"),
				Optional.of("1990-01-01T07:30:00Z"),
				Optional.empty(),
				Optional.empty());

		getRequest.getParameters()
				.add("options", "temporalValues");

		if (setLastN) {
			getRequest.getParameters().add("lastN", "20");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// we expect the two cars to be inside the area four times each, between 00:04 and 00:10, between 03:04 and 03:10, 03:32 and 03:38 and again between 06:32 and 06.38
		List expectedValues = List.of("1970-01-01T00:05:00Z" ,"1970-01-01T00:06:00Z" ,"1970-01-01T00:07:00Z" ,
				"1970-01-01T00:08:00Z" ,"1970-01-01T00:09:00Z" ,"1970-01-01T03:05:00Z" ,"1970-01-01T03:06:00Z" ,"1970-01-01T03:07:00Z" ,
				"1970-01-01T03:08:00Z" ,"1970-01-01T03:09:00Z" ,"1970-01-01T03:33:00Z" ,"1970-01-01T03:34:00Z" ,"1970-01-01T03:35:00Z" ,
				"1970-01-01T03:36:00Z" ,"1970-01-01T03:37:00Z" ,"1970-01-01T06:33:00Z" ,"1970-01-01T06:34:00Z" ,"1970-01-01T06:35:00Z" ,
				"1970-01-01T06:36:00Z" ,"1970-01-01T06:37:00Z");

		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			assertEquals(expectedValues, temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList()), "All time elements should be present.");
		});
	}

	@DisplayName("Test running a temporal query including an id and type.")
	@ParameterizedTest
	@MethodSource("provideCarIds")
	public void testTempWithIdAndTypeInTempValues(RequestType requestType, List<URI> ids) {

		MutableHttpRequest request;
		if (requestType == RequestType.GET) {
			request = getGetRequest(Optional.of(ids.stream().map(Object::toString).collect(Collectors.joining(","))),
					Optional.empty(),
					Optional.of("car"),
					Optional.of("driver"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1990-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		} else {
			request = getPostRequest(Optional.of(ids),
					Optional.empty(),
					Optional.of("car"),
					Optional.of("driver"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1990-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}

		request.getParameters()
				.add("options", "temporalValues");

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(request, List.class);

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
	public void testTempWithGeoQueryInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.empty(),
					Optional.of("near;maxDistance==300000"),
					Optional.of("LineString"),
					Optional.of("[[5,5],[7,7]]"),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.empty(),
					Optional.of("near;maxDistance==300000"),
					Optional.of("LineString"),
					Optional.of("[[5,5],[7,7]]"),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}
		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
		}

		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// we expect the two cars to be inside the area four times each, between 00:04 and 00:10, between 03:04 and 03:10, 03:32 and 03:38 and again between 06:32 and 06.38
		List expectedValues = List.of("1970-01-01T00:05:00Z" ,"1970-01-01T00:06:00Z" ,"1970-01-01T00:07:00Z" ,
				"1970-01-01T00:08:00Z" ,"1970-01-01T00:09:00Z" ,"1970-01-01T03:05:00Z" ,"1970-01-01T03:06:00Z" ,"1970-01-01T03:07:00Z" ,
				"1970-01-01T03:08:00Z" ,"1970-01-01T03:09:00Z" ,"1970-01-01T03:33:00Z" ,"1970-01-01T03:34:00Z" ,"1970-01-01T03:35:00Z" ,
				"1970-01-01T03:36:00Z" ,"1970-01-01T03:37:00Z" ,"1970-01-01T06:33:00Z" ,"1970-01-01T06:34:00Z" ,"1970-01-01T06:35:00Z" ,
				"1970-01-01T06:36:00Z" ,"1970-01-01T06:37:00Z");

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
	@MethodSource("provideBooleanValues")
	public void testTempWithNgsiQueryInTempValues(RequestType requestType, boolean setLastN) {
		MutableHttpRequest getRequest = getRequest(
				requestType,
				Optional.empty(),
				Optional.of(".*"),
				Optional.of("car"),
				Optional.of("temperature,driver"),
				Optional.of("temperature>20"),
				Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.of("between"),
				Optional.of("1970-01-01T00:01:00Z"),
				Optional.of("1970-01-01T07:30:00Z"),
				Optional.empty());

		getRequest.getParameters()
				.add("options", "temporalValues");

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
	public void testTempWithNgsiAndQueryInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("temperature,radio"),
					Optional.of("temperature<18;radio==true"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("temperature,radio"),
					Optional.of("temperature<18;radio==true"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}
		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	public void testTempWithNgsiAndQueryAndGeoInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.of("temperature<18;radio==true"),
					Optional.of("near;maxDistance==300000"),
					Optional.of("LineString"),
					Optional.of("[[5,5],[7,7]]"),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());

		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.of("temperature<18;radio==true"),
					Optional.of("near;maxDistance==300000"),
					Optional.of("LineString"),
					Optional.of("[[5,5],[7,7]]"),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	@MethodSource("provideBooleanValues")
	public void testTempWithNgsiORQueryInTempValues(RequestType requestType, boolean setLastN) {

		MutableHttpRequest getRequest = getRequest(
				requestType,
				Optional.empty(),
				Optional.of(".*"),
				Optional.of("car"),
				Optional.of("temperature"),
				Optional.of("temperature>20|radio==false"),
				Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.of("between"),
				Optional.of("1970-01-01T00:01:00Z"),
				Optional.of("1970-01-01T07:30:00Z"),
				Optional.empty());

		getRequest.getParameters()
				.add("options", "temporalValues");

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
	public void testTempWithNgsiORandANDQueryInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {

		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("temperature,radio,driver"),
					Optional.of("(temperature>20|radio==false);driver==\"Mira\""),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("temperature,radio,driver"),
					Optional.of("(temperature>20|radio==false);driver==\"Mira\""),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	public void testTempWithNgsiORandANDQueryWithSubAttrsInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.of("(temperature>20|radio==false);driver==\"Mira\";motor.fuel!=0.7"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.empty(),
					Optional.of("(temperature>20|radio==false);driver==\"Mira\";motor.fuel!=0.7"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}


		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	public void testTempWithNgsiRangeInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("motor.fuel==0.6..0.8"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("motor.fuel==0.6..0.8"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}
		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	public void testTempWithNgsiListInTempValues(RequestType requestType, TimeRelation timeRelation, Optional<String> sysAttrs) {
		MutableHttpRequest getRequest = null;

		if (timeRelation == TimeRelation.BEFORE) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("driver==[\"Mira\",\"Franzi\"]"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("before"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (timeRelation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("driver==[\"Mira\",\"Franzi\"]"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T00:01:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}

		if (sysAttrs.isPresent()) {
			getRequest.getParameters().add("options", sysAttrs.get());
		} else {
			getRequest.getParameters().add("options", "temporalValues");
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
	@ParameterizedTest
	@EnumSource(RequestType.class)
	public void testTempWithNgsiJsonInTempValues(RequestType requestType) {
		MutableHttpRequest getRequest = getRequest(
				requestType,
				Optional.empty(),
				Optional.of(".*"),
				Optional.of("car"),
				Optional.of("driver,motor"),
				Optional.of("trunk[cases]==3"),
				Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.of("between"),
				Optional.of("1970-01-01T00:01:00Z"),
				Optional.of("1970-01-01T07:30:00Z"),
				Optional.empty());

		getRequest.getParameters()
				.add("options", "temporalValues");

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
	public void testTempWithNgsiQueryNoAttrInReqTimeFrameInTempValues(RequestType requestType, TimeRelation relation) {
		MutableHttpRequest getRequest = null;


		if (relation == TimeRelation.AFTER) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("driver==\"Franzi\""),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("after"),
					Optional.of("1970-01-01T03:30:00Z"),
					Optional.empty(),
					Optional.empty());
		}
		if (relation == TimeRelation.BETWEEN) {
			getRequest = getRequest(
					requestType,
					Optional.empty(),
					Optional.of(".*"),
					Optional.of("car"),
					Optional.of("driver,motor"),
					Optional.of("driver==\"Franzi\""),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.of("between"),
					Optional.of("1970-01-01T03:30:00Z"),
					Optional.of("1970-01-01T07:30:00Z"),
					Optional.empty());
		}

		getRequest.getParameters()
				.add("options", "temporalValues");

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
	@ParameterizedTest
	@ValueSource(strings = {"pageSize", "limit"})
	public void testCountOptionSet(String paginationParameterName) {
		MutableHttpRequest getRequest = getGetRequest(
				Optional.empty(),
				Optional.of(".*"),
				Optional.empty(),
				Optional.of("temperature"),
				Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.empty(),
				Optional.of("before"),
				Optional.of("1990-01-01T03:30:00Z"),
				Optional.empty(),
				Optional.empty());

		getRequest.getParameters()
				.add("options", "count")
				.add(paginationParameterName, "2");

		HttpResponse<List<Map<String, Object>>> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The request should succeed.");
		assertNotNull(entryListResponse.getHeaders().get("NGSILD-Results-Count"), "Count should be present");
		assertEquals("5", entryListResponse.getHeaders().get("NGSILD-Results-Count"), "5 entites should match.");
	}

	@DisplayName("Retrieve query results with entity pagination")
	@ParameterizedTest
	@ValueSource(strings = {"pageSize", "limit"})
	public void testEntityPagination(String paginationParameterName) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("attrs", "temperature")
				.add("timerel", "after")
				.add("timeAt", "1960-01-01T03:30:00Z")
				.add(paginationParameterName, "2");
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
				.add("timerel", "after")
				.add("timeAt", "1960-01-01T03:30:00Z")
				.add(paginationParameterName, "2")
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
				.add("timerel", "before")
				.add("timeAt", "1990-01-01T00:00:00Z")
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
				.add("timerel", "after")
				.add("timeAt", "1960-01-01T03:30:00Z")
				.add("type", "doesNotExist");
		HttpResponse<List> entryListResponse = mintakaTestClient.toBlocking().exchange(getRequest, List.class);
		assertEquals(HttpStatus.OK, entryListResponse.getStatus(), "The result should be ok.");
		assertTrue(entryListResponse.getBody().get().isEmpty(), "The result should be an empty list.");
	}

	private MutableHttpRequest getPostRequest(
			// entityInfo
			Optional<List<URI>> id,
			Optional<String> idPattern,
			Optional<String> type,
			// attrs
			Optional<String> attrs,
			// q
			Optional<String> q,
			// geo query
			Optional<String> georel,
			Optional<String> geometry,
			Optional<String> coordinates,
			// temporal query
			Optional<String> timerel,
			Optional<String> timeAt,
			Optional<String> endTimeAt,
			Optional<String> timeProperty) {

		QueryVO queryVO = new QueryVO();
		if (id.isPresent()) {
			List<URI> idList = id.get();
			if (idList.size() == 1) {
				queryVO.entities().id(idList.get(0).toString());
			} else {
				queryVO.entities().id(idList.stream().map(URI::toString).collect(Collectors.toList()));
			}
		}
		idPattern.ifPresent(queryVO.entities()::idPattern);
		type.ifPresent(queryVO.entities()::type);
		q.ifPresent(queryVO::q);

		if (attrs.isPresent()) {
			queryVO.attrs(Arrays.stream(attrs.get().split(",")).collect(Collectors.toList()));
		}

		georel.ifPresent(queryVO.geoQ()::georel);
		geometry.ifPresent(queryVO.geoQ()::geometry);
		if (coordinates.isPresent()) {
			queryVO.geoQ().coordinates(Arrays.stream(coordinates.get().split(",")).collect(Collectors.toList()));
		}

		timerel.ifPresent(queryVO.temporalQ()::timerel);
		if (timeAt.isPresent()) {
			queryVO.temporalQ().timeAt(Instant.parse(timeAt.get()));
		}
		if (endTimeAt.isPresent()) {
			queryVO.temporalQ().endTimeAt(Instant.parse(endTimeAt.get()));
		}
		timeProperty.ifPresent(queryVO.temporalQ()::timeproperty);

		ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.registerModule(new JavaTimeModule());
		try {
			return HttpRequest.POST("/temporal/entityOperations/query", objectMapper.writeValueAsString(queryVO));
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private MutableHttpRequest getRequest(
			RequestType requestType,
			// entityInfo
			Optional<List<URI>> id,
			Optional<String> idPattern,
			Optional<String> type,
			// attrs
			Optional<String> attrs,
			// q
			Optional<String> q,
			// geo query
			Optional<String> georel,
			Optional<String> geometry,
			Optional<String> coordinates,
			// temporal query
			Optional<String> timerel,
			Optional<String> timeAt,
			Optional<String> endTimeAt,
			Optional<String> timeProperty) {
		switch (requestType) {
			case GET:
				String ids = null;
				if (id.isPresent()) {
					ids = id.get().stream().map(URI::toString).collect(Collectors.joining(","));
				}
				return getGetRequest(Optional.ofNullable(ids), idPattern, type, attrs, q, georel, geometry, coordinates, timerel, timeAt, endTimeAt, timeProperty);
			case POST:
			default:
				return getPostRequest(id, idPattern, type, attrs, q, georel, geometry, coordinates, timerel, timeAt, endTimeAt, timeProperty);
		}
	}

	private MutableHttpRequest getGetRequest(
			// entityInfo
			Optional<String> id,
			Optional<String> idPattern,
			Optional<String> type,
			// attrs
			Optional<String> attrs,
			// q
			Optional<String> q,
			// geo query
			Optional<String> georel,
			Optional<String> geometry,
			Optional<String> coordinates,
			// temporal query
			Optional<String> timerel,
			Optional<String> timeAt,
			Optional<String> endTimeAt,
			Optional<String> timeProperty) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");

		id.ifPresent(idString -> getRequest.getParameters().add("id", idString));
		idPattern.ifPresent(idPatternString -> getRequest.getParameters().add("idPattern", idPatternString));
		type.ifPresent(typeString -> getRequest.getParameters().add("type", typeString));
		attrs.ifPresent(attrsString -> getRequest.getParameters().add("attrs", attrsString));
		q.ifPresent(qString -> getRequest.getParameters().add("q", qString));
		georel.ifPresent(georelString -> getRequest.getParameters().add("georel", georelString));
		geometry.ifPresent(geometryString -> getRequest.getParameters().add("geometry", geometryString));
		coordinates.ifPresent(coordinatesString -> getRequest.getParameters().add("coordinates", coordinatesString));
		timerel.ifPresent(timerelString -> getRequest.getParameters().add("timerel", timerelString));
		timeAt.ifPresent(timeAtString -> getRequest.getParameters().add("timeAt", timeAtString));
		endTimeAt.ifPresent(endTimeAtString -> getRequest.getParameters().add("endTimeAt", endTimeAtString));
		timeProperty.ifPresent(timePropertyString -> getRequest.getParameters().add("timeproperty", timePropertyString));
		return getRequest;
	}

	private static Stream<Arguments> getRelationsBetweenAndAfter() {
		return Stream.of(
				Arguments.of(RequestType.POST, TimeRelation.BETWEEN), Arguments.of(RequestType.POST, TimeRelation.AFTER),
				Arguments.of(RequestType.GET, TimeRelation.BETWEEN), Arguments.of(RequestType.GET, TimeRelation.AFTER));
	}

	private static Stream<Arguments> getRelationsBetweenAndBefore() {
		return Stream.of(
				Arguments.of(RequestType.POST, TimeRelation.BETWEEN, Optional.of("sysAttrs")),
				Arguments.of(RequestType.POST, TimeRelation.BEFORE, Optional.of("sysAttrs")),
				Arguments.of(RequestType.POST, TimeRelation.BETWEEN, Optional.empty()),
				Arguments.of(RequestType.POST, TimeRelation.BEFORE, Optional.empty()),
				Arguments.of(RequestType.GET, TimeRelation.BETWEEN, Optional.of("sysAttrs")),
				Arguments.of(RequestType.GET, TimeRelation.BEFORE, Optional.of("sysAttrs")),
				Arguments.of(RequestType.GET, TimeRelation.BETWEEN, Optional.empty()),
				Arguments.of(RequestType.GET, TimeRelation.BEFORE, Optional.empty()));
	}

	private static Stream<Arguments> provideCarIds() {
		return Stream.of(
				Arguments.of(RequestType.POST, List.of(CAR_1_ID, CAR_2_ID)), Arguments.of(RequestType.POST, List.of(CAR_1_ID)), Arguments.of(RequestType.POST, List.of(CAR_2_ID)),
				Arguments.of(RequestType.GET, List.of(CAR_1_ID, CAR_2_ID)), Arguments.of(RequestType.GET, List.of(CAR_1_ID)), Arguments.of(RequestType.GET, List.of(CAR_2_ID)));
	}

	private static Stream<Arguments> provideBooleanValues() {
		return Stream.of(
				Arguments.of(RequestType.POST, true), Arguments.of(RequestType.POST, false),
				Arguments.of(RequestType.GET, true), Arguments.of(RequestType.GET, false));
	}

	enum RequestType {
		POST,
		GET;
	}
}
