package org.fiware.mintaka.domain.query.geo;

/**
 * Supported geo operations(as defined by NGSI-LD v1.3.1)
 */
public enum GeoOperation {

	NEAR("near", "ST_DWithin"),
	EQUALS("equals", "ST_Equals"),
	DISJOINT("disjoint", "ST_Disjoint"),
	INTERSECT("intersects", "ST_Intersects"),
	WITHIN("within", "ST_Within"),
	CONTAINS("contains", "ST_Contains"),
	OVERLAPS("overlaps", "ST_Overlaps");

	private final String name;
	private final String postgisFunction;

	GeoOperation(String name, String postgisFunction) {
		this.name = name;
		this.postgisFunction = postgisFunction;
	}

	public static GeoOperation getEnum(String value) {
		for(GeoOperation v : values())
			if(v.name.equalsIgnoreCase(value)) return v;
		throw new IllegalArgumentException();
	}

	public String getPostgisFunction() {
		return this.postgisFunction;
	}
}
