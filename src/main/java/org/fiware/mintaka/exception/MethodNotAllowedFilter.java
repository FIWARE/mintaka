package org.fiware.mintaka.exception;

import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

import java.util.Optional;

/**
 * Filter out all 405 responses an map them to 422 as defined by the NGSI-LD api.
 */
@Filter("/**")
public class MethodNotAllowedFilter implements HttpServerFilter {

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.OPERATION_NOT_SUPPORTED;

	@Override
	public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {

		return Publishers.map(chain.proceed(request), this::handleMethodNotAllowed);
	}


	private MutableHttpResponse handleMethodNotAllowed(MutableHttpResponse response) {
		if (response.status().equals(HttpStatus.METHOD_NOT_ALLOWED)) {
			Optional<String> optionalErrorMessage = response.getBody().map(Object::toString);
			return HttpResponse
					.status(ASSOCIATED_ERROR.getStatus())
					.body(new ProblemDetails(ASSOCIATED_ERROR.getType(),
							"Method is not supported.",
							ASSOCIATED_ERROR.getStatus().getCode(),
							optionalErrorMessage.orElse("Requested method is not supported."),
							null));
		}
		return response;
	}
}
