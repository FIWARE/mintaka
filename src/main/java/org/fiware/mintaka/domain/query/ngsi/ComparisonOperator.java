package org.fiware.mintaka.domain.query.ngsi;

import java.util.Arrays;

/**
 * Operator for comparisons, according to NGSI-LD api
 */
public enum ComparisonOperator {

	EQUAL("==", "="),
	UNEQUAL("!=", "!="),
	GREATER_EQ(">=", ">="),
	GREATER(">", ">"),
	LESS_EQ("<=", "<="),
	LESS("<", "<"),
	PATTERN("~=", "~"),
	NOT_PATTERN("!~=", "NOT ~");

	private final String value;
	private final String dbOperator;

	ComparisonOperator(String value, String dbOperator) {
		this.value = value;
		this.dbOperator = dbOperator;
	}

	public static ComparisonOperator byName(String value) {
		return Arrays.stream(values())
				.filter(v -> v.getValue().equals(value))
				.findAny()
				.orElseThrow(() -> new IllegalArgumentException("Unknown value '" + value + "'."));
	}

	public String getValue() {
		return value;
	}

	public String getDbOperator() {
		return this.dbOperator;
	}

}
