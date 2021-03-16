package org.fiware.mintaka.rest;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpAttributes;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.FilterChain;
import io.micronaut.http.filter.HttpFilter;
import io.micronaut.web.router.UriRouteMatch;
import io.reactivex.Flowable;
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
				return Flowable.just(HttpResponse.badRequest(String.format("Received an unsupported parameter: %s", parameterNames)));
			}
		}
		return chain.proceed(request);
	}
}

