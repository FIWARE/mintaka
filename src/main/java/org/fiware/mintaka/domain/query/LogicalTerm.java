package org.fiware.mintaka.domain.query;

import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
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
