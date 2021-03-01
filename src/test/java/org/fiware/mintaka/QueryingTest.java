package org.fiware.mintaka;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryingTest extends ComposeTest {

	@DisplayName("Test running a temporal query including geo querying.")
	@Test
	public void testTempWithGeoQueryInTempValues() throws JsonProcessingException {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "LineString")
				.add("coordinates", "[[5,5],[7,7]]")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		// we expect the two cars to be inside the area two times each, between 00:04 and 00:08 and again between 06:32 and 06.36
		List expectedValues = List.of("1970-01-01T00:04:00Z", "1970-01-01T00:05:00Z", "1970-01-01T00:06:00Z", "1970-01-01T00:07:00Z", "1970-01-01T00:08:00Z",
				"1970-01-01T06:32:00Z", "1970-01-01T06:33:00Z", "1970-01-01T06:34:00Z", "1970-01-01T06:35:00Z", "1970-01-01T06:36:00Z");

		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			assertEquals(expectedValues, temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList()), "All time elements should be present.");
		});
	}

	@DisplayName("Test running a temporal query including ngsi querying.")
	@Test
	public void testTempWithNgsiQueryInTempValues() throws JsonProcessingException {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature>20")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
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
	@Test
	public void testTempWithNgsiAndQueryInTempValues() throws JsonProcessingException {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature<18;radio==true")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
		// get car when radio was on and temp was below 18 -> only the time at the end of the time window
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(100, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:40:00Z", timeList.get(99), "Last time should be at the half");
			assertEquals("1970-01-01T05:01:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi AND querying and geo query.")
	@Test
	public void testTempWithNgsiAndQueryAndGeoInTempValues() throws JsonProcessingException {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature<18;radio==true")
				.add("georel", "near;maxDistance==300000")
				.add("geometry", "LineString")
				.add("coordinates", "[[5,5],[7,7]]")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
		// get car when radio was on and temp was below 18 and ts near [[5,5],[7,7]]-> only the short time frame at the end of the time window
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(5, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T06:36:00Z", timeList.get(4), "Last time should be at the half");
			assertEquals("1970-01-01T06:32:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi OR querying.")
	@Test
	public void testTempWithNgsORQueryInTempValues() throws JsonProcessingException {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "temperature>20|radio==false")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
		// get car when radio was of or temp was above 20 -> first ~3/4 of the drive
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(299, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T05:00:00Z", timeList.get(298), "Last time should be at the half");
			assertEquals("1970-01-01T00:02:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}

	@DisplayName("Test running a temporal query including ngsi OR and AND querying.")
	@Test
	public void testTempWithNgsORandANDQueryInTempValues() throws JsonProcessingException {

		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getParameters()
				.add("options", "temporalValues")
				.add("idPattern", ".*")
				.add("type", "car")
				.add("q", "(temperature>20|radio==false);driver==\"Mira\"")
				.add("timerel", "between")
				.add("time", "1970-01-01T00:01:00Z")
				.add("endTime", "1970-01-01T07:30:00Z");
		// get car when radio was of or temp was above 20 and the driver was "mira" -> second quarter of the drive
		List<Map<String, Object>> entryList = mintakaTestClient.toBlocking().retrieve(getRequest, List.class);
		assertEquals(2, entryList.size(), "Both matching entities should have been returned.");
		entryList.forEach(entry -> {
			assertTrue(entry.containsKey("temperature"), "The temperature property should be present.");
			assertTrue(entry.containsKey("radio"), "The radio property should be present.");
			assertTrue(entry.containsKey("driver"), "The driver property should be present.");
			Map<String, List> temperaturePropertyMap = (Map<String, List>) entry.get("temperature");
			List<List> temporalValuesList = temperaturePropertyMap.get("values");
			List timeList = temporalValuesList.stream().map(list -> list.get(1)).sorted().collect(Collectors.toList());
			assertEquals(100, timeList.size(), "All expected times should be present");
			assertEquals("1970-01-01T03:20:00Z", timeList.get(99), "Last time should be at the half");
			assertEquals("1970-01-01T01:41:00Z", timeList.get(0), "First time should be radio on at the end part");
		});
	}
}
