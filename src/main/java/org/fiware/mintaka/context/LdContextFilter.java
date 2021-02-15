package org.fiware.mintaka.context;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;

@Slf4j
@Filter(Filter.MATCH_ALL_PATTERN)
public class LdContextFilter implements HttpServerFilter {

	@Override
	public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
		return Flowable.fromPublisher(chain.proceed(request)).doOnNext(r -> {
			r.body();
		});
	}
}
