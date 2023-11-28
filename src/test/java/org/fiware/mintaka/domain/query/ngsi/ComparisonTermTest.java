package org.fiware.mintaka.domain.query.ngsi;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.fiware.mintaka.QueryingTest;
import org.fiware.mintaka.context.ContextProperties;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.query.temporal.TimeRelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URL;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

class ComparisonTermTest {

	QueryParser queryParser;

	@BeforeEach
	public void setup() {
		LdContextCache ldContextCache = new LdContextCache(new ContextProperties());
		ldContextCache.initDefaultContext();
		queryParser = new QueryParser(ldContextCache);
	}
	private static Stream<Arguments> getQueries() {
		return Stream.of(
				Arguments.of("motor.fuel==0.6..0.8", " instanceid in (SELECT attrinstanceid  FROM subattributes sa WHERE  number BETWEEN 0.6 AND 0.8 and id='https://uri.etsi.org/ngsi-ld/default-context/fuel')"),
				Arguments.of("motor.fuel==0,6..0,8", " instanceid in (SELECT attrinstanceid  FROM subattributes sa WHERE  number BETWEEN 0.6 AND 0.8 and id='https://uri.etsi.org/ngsi-ld/default-context/fuel')")
		);
	}
	@ParameterizedTest
	@MethodSource("getQueries")
	void toSQLQuery(String rawQuery, String expectedResult) throws Exception {
		QueryTerm query = queryParser.toTerm(rawQuery, List.of(new URL("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")));
		Assertions.assertEquals(expectedResult,query.toSQLQuery());
	}

	@ParameterizedTest
	@MethodSource("getNumbers")
	void parseNumber(String input, Number expected) throws ParseException {
		Assertions.assertEquals(expected,NumberFormat.getInstance().parse(input));
	}
	private static Stream<Arguments> getNumbers() {
		return Stream.of(
				Arguments.of("0.6", 0.6),
				Arguments.of("0,6", 0.6)
		);
	}
}