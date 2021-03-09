package org.fiware.mintaka.domain.query.ngsi;

import lombok.Getter;
import lombok.ToString;

/**
 * Query term to represent the logical connection of two terms, e.g. AND or OR
 */
@ToString
public class LogicalConnectionTerm extends QueryTerm {

	@Getter
	private final LogicalOperator operator;

	public LogicalConnectionTerm(LogicalOperator operator) {
		super(operator.getValue());
		this.operator = operator;
	}

	@Override
	public String toSQLQuery() {
		return operator.getDbOperator();
	}
}
