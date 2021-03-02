package org.fiware.mintaka.domain.query;

import lombok.Getter;
import lombok.ToString;

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
