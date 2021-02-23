package org.fiware.mintaka.domain;

import java.util.StringTokenizer;

/**
 * Enum for the possible requested timepropertys
 */
public enum TimeStampType {

	CREATED_AT("createdAt"),
	MODIFIED_AT("modifiedAt"),
	OBSERVED_AT("observedAt"),;

	private final String name;

	TimeStampType(String name) {
		this.name = name;
	}
}
