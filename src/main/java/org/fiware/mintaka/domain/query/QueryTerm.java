package org.fiware.mintaka.domain.query;

import io.micronaut.http.annotation.Get;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public abstract class QueryTerm {

	protected final String term;

	public abstract String toSQLQuery();
}
