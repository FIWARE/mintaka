package org.fiware.mintaka.exception;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

/**
 * Handle all {@link ContextRetrievalException}.
 */
@Produces
@Singleton
@Requires(classes = {ContextRetrievalException.class, ExceptionHandler.class})
@Slf4j
public class ContextRetrievalExceptionHandler extends NGSICompliantExceptionHandler<ContextRetrievalException> {

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.LD_CONTEXT_NOT_AVAILABLE;
	private static final String ERROR_TITLE = "Context is not available.";


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
	public String getInstance(HttpRequest request, ContextRetrievalException exception) {
		return exception.getContextAddress();
	}
}