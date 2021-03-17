package org.fiware.mintaka.exception;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.multitenancy.tenantresolver.HttpHeaderTenantResolverConfiguration;
import io.micronaut.transaction.exceptions.CannotCreateTransactionException;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Handler for mapping {@link CannotCreateTransactionException}. Provides the requested tenant as instance.
 */
@Produces
@Singleton
@Requires(classes = {CannotCreateTransactionException.class, ExceptionHandler.class})
@Slf4j
public class CannotCreateTransactionExceptionHandler extends NGSICompliantExceptionHandler<CannotCreateTransactionException> {

	@Inject
	private HttpHeaderTenantResolverConfiguration configuration;

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.NON_EXISTENT_TENANT;
	private static final String ERROR_TITLE = "Requested tenant does not exist.";

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
	public String getInstance(HttpRequest request, CannotCreateTransactionException exception) {
		return request.getHeaders().get(configuration.getHeaderName());
	}
}
