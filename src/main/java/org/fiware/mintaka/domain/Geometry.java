package org.fiware.mintaka.domain;

public enum Geometry {

	POINT("Point", "geopoint"),
	LINESTRING("LineString", "geolinestring"),
	MULTILINESTRING("MultiLineString", "geomultilinestring"),
	POLYGON("Polygon", "geopolygon"),
	MULTIPOLYGON("MultiPolygon", "geomultipolygon");

	private final String value;
	private final String dbFieldName;

	private Geometry(String value, String dbFieldName) {
		this.value = value;
		this.dbFieldName = dbFieldName;
	}

	public String getDbFieldName() {
		return this.dbFieldName;
	}
}
