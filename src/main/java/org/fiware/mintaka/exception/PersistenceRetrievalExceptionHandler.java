package org.fiware.mintaka.exception;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

/**
 * Handle all {@link PersistenceRetrievalException} and map them to an internal error.
 */
@Produces
@Singleton
@Requires(classes = {PersistenceRetrievalException.class, ExceptionHandler.class})
@Slf4j
public class PersistenceRetrievalExceptionHandler extends NGSICompliantExceptionHandler<PersistenceRetrievalException> {

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.INTERNAL_ERROR;
	private static final String ERROR_TITLE = "Error on db retrieval.";

	@Override
	public ErrorType getAssociatedErrorType() {
		return ASSOCIATED_ERROR;
	}

	@Override
	public HttpStatus getStatus() {
		return ASSOCIATED_ERROR.getStatus();
	}

	@Override
	public String getErrorTitle() {
		return ERROR_TITLE;
	}

	@Override
	public String getInstance(HttpRequest request, PersistenceRetrievalException exception) {
		return null;
	}
}
