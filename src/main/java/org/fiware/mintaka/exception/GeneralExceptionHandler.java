package org.fiware.mintaka.exception;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

/**
 * Handler to catch all not specifically(unexpected) handled exceptions and map them NGSI compliant.
 */
@Produces
@Singleton
@RequiredArgsConstructor
@Requires(classes = {Exception.class, ExceptionHandler.class})
@Slf4j
public class GeneralExceptionHandler implements ExceptionHandler<Exception, HttpResponse<ProblemDetails>> {

	private final ContextRetrievalExceptionHandler contextRetrievalExceptionHandler;
	private final JacksonConversionExceptionHandler jacksonConversionExceptionHandler;

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.INTERNAL_ERROR;
	private static final String ERROR_TITLE = "Unexpected error.";

	@Override
	public HttpResponse<ProblemDetails> handle(HttpRequest request, Exception exception) {
		// in case the error was thrown on serialization, we need a special handling

 		if (exception instanceof CodecException && exception.getCause() instanceof JsonMappingException) {
			return handleCodecException(request, exception.getCause().getCause());
		}

		log.info(ERROR_TITLE);
		log.debug("Error was: ", exception);
		return HttpResponse.status(ASSOCIATED_ERROR.getStatus())
				.body(
						new ProblemDetails(
								ASSOCIATED_ERROR.getType(),
								ERROR_TITLE,
								ASSOCIATED_ERROR.getStatus().getCode(),
								exception.getMessage(),
								null));
	}

	private HttpResponse<ProblemDetails> handleCodecException(HttpRequest request, Throwable cause) {
		if (cause instanceof ContextRetrievalException) {
			return contextRetrievalExceptionHandler.handle(request, (ContextRetrievalException) cause);
		} else if (cause instanceof JacksonConversionException) {
			return jacksonConversionExceptionHandler.handle(request, (JacksonConversionException) cause);
		} else {
			return this.handle(request, (Exception) cause);
		}
	}
}
