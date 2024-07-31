package org.fiware.mintaka.exception;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

/**
 * Handle all not-found cases
 */
@Produces
@Singleton
@Requires(classes = {NotFoundException.class, ExceptionHandler.class})
@Slf4j
public class NotFoundExceptionHandler extends NGSICompliantExceptionHandler<NotFoundException>{

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.RESOURCE_NOT_FOUND;
	private static final String ERROR_TITLE = "Resource Not Found.";

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
	public String getInstance(HttpRequest request, NotFoundException exception) {
		return null;
	}
}
