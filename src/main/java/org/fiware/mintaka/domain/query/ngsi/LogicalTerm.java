package org.fiware.mintaka.domain.query.ngsi;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of a logical query term. A logical term is the connecatenation of a number of terms(comparison or logical), alternating with the
 * connection LogicalConnectionTerms
 */
@Getter
@ToString
public class LogicalTerm extends QueryTerm {

	private final List<QueryTerm> subTerms = new ArrayList<>();

	public LogicalTerm(String term) {
		super(term);
	}

	public LogicalTerm addSubTerm(QueryTerm subTerm) {
		subTerms.add(subTerm);
		return this;
	}

	@Override
	public String toSQLQuery() {
		String query = "SELECT entityId, observedAt FROM attributes WHERE entityId in ";

		for (QueryTerm subTerm : subTerms) {
			if (subTerm instanceof LogicalConnectionTerm) {
				query += String.format(" %s ", subTerm.toSQLQuery());
			} else {
				query += String.format("(%s)", subTerm.toSQLQuery());
			}
		}
		return query;
	}
}
