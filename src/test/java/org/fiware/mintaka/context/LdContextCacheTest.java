package org.fiware.mintaka.context;

import com.apicatalog.jsonld.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class LdContextCacheTest {

	private LdContextCache ldContextCache;

	@BeforeEach
	public void setup() {
		ldContextCache = new LdContextCache(new ContextProperties());
		ldContextCache.initDefaultContext();
	}

	@DisplayName("Get context document from the urls.")
	@ParameterizedTest
	@MethodSource("getContextObjects")
	void getContextDocument(Object contextURLs) {

		assertDoesNotThrow(() -> ldContextCache.getContextDocument(contextURLs));
	}

	private static Stream<Arguments> getContextObjects() throws MalformedURLException {
		return Stream.of(
				Arguments.of(List.of("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", "https://schema.lab.fiware.org/ld/context")),
				// core
				Arguments.of("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"),
				Arguments.of(new URL("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")),
				Arguments.of(URI.create("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")),
				// alternative
				Arguments.of("https://schema.lab.fiware.org/ld/context"),
				Arguments.of(new URL("https://schema.lab.fiware.org/ld/context")),
				Arguments.of(URI.create("https://schema.lab.fiware.org/ld/context"))
		);
	}
}