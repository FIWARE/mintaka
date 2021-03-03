package org.fiware.mintaka.domain.query.ngsi;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Abstract superclass for all query terms
 */
@RequiredArgsConstructor
@Getter
public abstract class QueryTerm {

	protected final String term;

	/**
	 * Return the sql string representation of the query term
	 * @return the sql string
	 */
	public abstract String toSQLQuery();
}
