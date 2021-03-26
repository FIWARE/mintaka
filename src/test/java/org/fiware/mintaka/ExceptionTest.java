package org.fiware.mintaka;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.fiware.mintaka.domain.query.temporal.TimeQuery;
import org.fiware.mintaka.persistence.EntityRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExceptionTest extends ComposeTest {

	@DisplayName("Test request with an invalid context")
	@ParameterizedTest
	@ValueSource(strings = {"https://no-context.org"})
	public void testInvalidContextRequest(String invalidContext) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getHeaders()
				.add("Link", String.format("<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json", invalidContext));
		try {
			mintakaTestClient.toBlocking().retrieve(getRequest);
			fail("Retrieval with invalid context should not be possible.");
		} catch (HttpClientResponseException e) {
			assertEquals(HttpStatus.SERVICE_UNAVAILABLE, e.getResponse().getStatus(), "If context cannot be retrieved, a 503 should be returned.");
		}
	}

	@DisplayName("Test request with an invalid context uri")
	@ParameterizedTest
	@ValueSource(strings = {"invalidURI", "", "ht://some-url.com"})
	public void testInvalidContextURIRequest(String invalidContext) {
		MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
		getRequest.getHeaders()
				.add("Link", String.format("<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json", invalidContext));
		try {
			mintakaTestClient.toBlocking().retrieve(getRequest);
			fail("Retrieval with invalid context should not be possible.");
		} catch (HttpClientResponseException e) {
			assertEquals(HttpStatus.BAD_REQUEST, e.getResponse().getStatus(), "If context uri is illeagel, 400 should be returned.");
		}
	}

	@Test
	public void testInternalErrorOnDBProblem() {
		Optional<EntityRepository> repositoryBean = applicationContext.findBean(EntityRepository.class);
		try {
			EntityRepository mockRepository = mock(EntityRepository.class);
			applicationContext.inject(mockRepository);
			when(mockRepository.findEntityIdsAndTimeframesByQuery(any(Optional.class), any(Optional.class), anyList(), any(TimeQuery.class), any(Optional.class), any(Optional.class), anyInt(), any(Optional.class)))
					.thenThrow(new RuntimeException("Unexpected error."));
			MutableHttpRequest getRequest = HttpRequest.GET("/temporal/entities/");
			try {
				mintakaTestClient.toBlocking().retrieve(getRequest);
				fail("In case of db errors, the retrieval should respond an error.");
			} catch (HttpClientResponseException e) {
				assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, e.getStatus(), "A 500 should have been returned.");
			}
		} finally {
			applicationContext.inject(repositoryBean.get());
		}
	}
}
