package org.fiware.mintaka.domain;

import lombok.Getter;

public enum AcceptType {
	JSON("application/json"),
	DEFAULT("*/*"),
	JSON_LD("application/ld+json");

	@Getter
	private final String value;

	AcceptType(String value) {
		this.value = value;
	}

	public static AcceptType getEnum(String value) {
		// in case of accept all, application/json should be used.
		if(DEFAULT.getValue().equalsIgnoreCase(value)) {
			return JSON;
		}
		for(AcceptType v : values())
			if(v.value.equalsIgnoreCase(value)) return v;
		throw new IllegalArgumentException(String.format("Invalid accept type. Only %s and %s is supported.", JSON.value, JSON_LD.value));
	}
}
