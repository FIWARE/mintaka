package org.fiware.mintaka.exception;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

/**
 * Handle exceptions due to invalid timerelations.
 */
@Produces
@Singleton
@Requires(classes = {InvalidTimeRelationException.class, ExceptionHandler.class})
@Slf4j
public class InvalidTimeRelationExceptionHandler implements ExceptionHandler<InvalidTimeRelationException, HttpResponse> {

	@Override
	public HttpResponse handle(HttpRequest request, InvalidTimeRelationException exception) {
		log.info("Received an invalid time relation.");
		log.debug("Error was: ", exception);
		return HttpResponse.badRequest(String.format("Received an invalid timerelation: {}.", exception.getMessage()));
	}
}
