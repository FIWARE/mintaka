package org.fiware.mintaka.domain.query.ngsi;

import java.util.Arrays;

/**
 * Representation of a logical operator according to the ngsi-ld api
 */
public enum LogicalOperator {

	AND(";", "AND", ';'),
	OR("|", "OR", '|');

	private final String value;
	private final char charValue;
	private final String dbOperator;

	LogicalOperator(String value, String dbOperator, char charValue) {
		this.value = value;
		this.dbOperator = dbOperator;
		this.charValue = charValue;
	}

	public static LogicalOperator byName(String value) {
		return Arrays.stream(values())
				.filter(v -> v.getValue().equals(value))
				.findAny()
				.orElseThrow(() -> new IllegalArgumentException("Unknown value '" + value + "'."));
	}

	public static LogicalOperator byCharValue(char charValue) {
		return Arrays.stream(values())
				.filter(v -> v.getCharValue() == charValue)
				.findAny()
				.orElseThrow(() -> new IllegalArgumentException("Unknown value '" + charValue + "'."));
	}

	public String getValue() {
		return value;
	}
	public char getCharValue() {return charValue;};
	public String getDbOperator() {
		return this.dbOperator;
	}
}
