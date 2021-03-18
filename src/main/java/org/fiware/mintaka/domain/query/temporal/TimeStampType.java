package org.fiware.mintaka.domain.query.temporal;

/**
 * Enum for the possible requested timepropertys
 */
public enum TimeStampType {

	CREATED_AT("createdAt"),
	MODIFIED_AT("modifiedAt"),
	OBSERVED_AT("observedAt"),
	TS("ts");

	private final String value;

	TimeStampType(String value) {
		this.value = value;
	}

	public String value() {
		return value;
	}

	public static TimeStampType getEnum(String value) {
		for(TimeStampType v : values())
			if(v.value.equalsIgnoreCase(value)) return v;
		throw new IllegalArgumentException();
	}
}
