package org.fiware.mintaka.rest;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpAttributes;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.FilterChain;
import io.micronaut.http.filter.HttpFilter;
import io.micronaut.web.router.UriRouteMatch;
import io.reactivex.Flowable;
import org.checkerframework.checker.units.qual.A;
import org.fiware.mintaka.exception.ErrorType;
import org.fiware.mintaka.exception.ProblemDetails;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filter out requests with unsupported parameters.
 * Be aware: this requires the http-parameter names to exactly match the matched method's parameter name.
 */
@Filter("/**")
public class InvalidParametersFilter implements HttpFilter {

	private static final ErrorType ASSOCIATED_ERROR = ErrorType.BAD_REQUEST_DATA;
	private static final String ERROR_TITLE = "Unsupported parameters";

	@Override
	public Publisher<? extends HttpResponse<?>> doFilter(HttpRequest<?> request, FilterChain chain) {
		Optional<UriRouteMatch> optionalRouteMatch = request
				.getAttributes()
				.get(HttpAttributes.ROUTE_MATCH.toString(), UriRouteMatch.class);
		if (optionalRouteMatch.isPresent()) {
			UriRouteMatch routeMatch = optionalRouteMatch.get();
			List<Argument> argumentList = routeMatch.getRequiredArguments();
			Set<String> argumentNames = argumentList.stream().map(Argument::getName).collect(Collectors.toSet());
			Set<String> parameterNames = request.getParameters().asMap().keySet();
			if (!argumentNames.containsAll(parameterNames)) {
				argumentNames.forEach(parameterNames::remove);
				ProblemDetails problemDetails = new ProblemDetails(
						ASSOCIATED_ERROR.getType(),
						ERROR_TITLE,
						ASSOCIATED_ERROR.getStatus().getCode(),
						String.format("Received unsupported parameter(s): %s", parameterNames),
						null);
				return Flowable.just(HttpResponse.badRequest(problemDetails));
			}
		}
		return chain.proceed(request);
	}
}

