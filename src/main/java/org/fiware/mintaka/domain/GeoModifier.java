package org.fiware.mintaka.domain;

public enum GeoModifier {

	MAX_DISTANCE("maxDistance"),
	MIN_DISTANCE("minDistance");

	private final String name;

	GeoModifier(String name) {
		this.name = name;
	}

	public static GeoModifier getEnum(String value) {
		for(GeoModifier v : values())
			if(v.name.equalsIgnoreCase(value)) return v;
		throw new IllegalArgumentException();
	}
}
