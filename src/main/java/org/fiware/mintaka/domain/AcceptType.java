package org.fiware.mintaka.domain;

import lombok.Getter;

public enum AcceptType {
	JSON("application/json"),
	JSON_LD("application/ld+json");

	@Getter
	private final String value;

	AcceptType(String value) {
		this.value = value;
	}

	public static AcceptType getEnum(String value) {
		for(AcceptType v : values())
			if(v.value.equalsIgnoreCase(value)) return v;
		throw new IllegalArgumentException();
	}
}
