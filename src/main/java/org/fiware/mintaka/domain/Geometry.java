package org.fiware.mintaka.domain;

import java.util.Arrays;

public enum Geometry {

	POINT("Point", "geopoint"),
	LINESTRING("LineString", "geolinestring"),
	MULTILINESTRING("MultiLineString", "geomultilinestring"),
	POLYGON("Polygon", "geopolygon"),
	MULTIPOLYGON("MultiPolygon", "geomultipolygon");

	private final String value;
	private final String dbFieldName;

	Geometry(String value, String dbFieldName) {
		this.value = value;
		this.dbFieldName = dbFieldName;
	}

	public static Geometry byName(String value) {
		return Arrays.stream(values())
				.filter(v -> v.getValue().equals(value))
				.findAny()
				.orElseThrow(() -> new IllegalArgumentException("Unknown value '" + value + "'."));
	}

	public String getValue() {
		return value;
	}

	public String getDbFieldName() {
		return this.dbFieldName;
	}
}
