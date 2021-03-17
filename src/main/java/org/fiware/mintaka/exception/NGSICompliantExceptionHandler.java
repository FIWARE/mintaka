package org.fiware.mintaka.exception;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

/**
 * Base exception handler to produce NGSI-LD compliant http responses.
 * @param <T> exception type, extends {@link Throwable}
 */
@Slf4j
public abstract class NGSICompliantExceptionHandler<T extends Throwable> implements ExceptionHandler<T, HttpResponse<ProblemDetails>> {

	@Override
	public HttpResponse<ProblemDetails> handle(HttpRequest request, T exception) {
		log.info(getErrorTitle());
		log.debug("Error was: ", exception);
		return HttpResponse.status(getStatus())
				.body(
						new ProblemDetails(
								getAssociatedErrorType().getType(),
								getErrorTitle(),
								getStatus().getCode(),
								exception.getMessage(),
								getInstance(request, exception)));
	}

	/**
	 * Return the {@link ErrorType} associated with the concrete exception
	 * @return the error-type
	 */
	public abstract ErrorType getAssociatedErrorType();

	/**
	 * Return the http status associated with the exception
	 * @return the http status
	 */
	public abstract HttpStatus getStatus();

	/**
	 * Return the error title to be used in the problem details
	 * @return the error title
	 */
	public abstract String getErrorTitle();

	/**
	 * Id of an instance associated with the error.
	 * @param request erroneous request
	 * @param exception the concrete exception
	 * @return the instance id, null if no such instance can be identified.
	 */
	@Nullable
	public abstract String getInstance(HttpRequest request, T exception);
}