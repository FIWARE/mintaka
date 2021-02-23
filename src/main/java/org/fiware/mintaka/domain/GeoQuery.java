package org.fiware.mintaka.domain;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GeoQuery {

	private static final String SQL_AND_OPERATOR = " AND ";
	private static final String NGSI_LD_GEO_EQUALS = "==";

	private final String geoRel;
	private final Geometry geometry;
	private final String geomString;
	@Getter
	private final String geoProperty;

	public GeoQuery(String geoRel, Geometry geometry, String coordinates, String geoProperty) {

		this.geoRel = geoRel;
		this.geometry = geometry;
		this.geomString = getStGeomString(geometry, coordinates);
		this.geoProperty = geoProperty;
	}


	public String getSqlRepresentation() {
		List<String> geoOperations =
				Lists.partition(Arrays.asList(geoRel.split(";")), 2)
						.stream()
						.map(this::getOperationSqlString)
						.collect(Collectors.toList());
		return String.join(SQL_AND_OPERATOR, geoOperations);
	}

	private String getOperationSqlString(List<String> operation) {
		if (!(operation.size() == 2 || operation.size() == 1)) {
			throw new IllegalArgumentException(String.format("The requested geo operation is invalid: %s. Full query: %s", operation, geoRel));
		}
		GeoOperation geoOperation = GeoOperation.getEnum(operation.get(0));

		switch (geoOperation) {
			case NEAR:
				String[] splittedModifier = operation.get(1).split(NGSI_LD_GEO_EQUALS);
				return getNearOperation(geoOperation, GeoModifier.getEnum(splittedModifier[0]), Long.valueOf(splittedModifier[1]));
			// they all have the same method signature.
			case EQUALS:
			case WITHIN:
			case CONTAINS:
			case DISJOINT:
			case OVERLAPS:
			case INTERSECT:
				return geoOperation.getPostgisFunction() + "(" + geomString + ", attribute." + geometry.getDbFieldName() + ")";
			default:
				throw new IllegalArgumentException(String.format("Received an unsupported geoOperation: %s. Full query: %s", operation, geoRel));
		}
	}

	private String getNearOperation(GeoOperation geoOperation, GeoModifier modifier, Long value) {
		switch (modifier) {
			case MAX_DISTANCE:
				return geoOperation.getPostgisFunction() + "(" + geomString + ", attribute." + geometry.getDbFieldName() + "," + value + ")";
			case MIN_DISTANCE:
				return "NOT " + geoOperation.getPostgisFunction() + "(" + geomString + ", attribute." + geometry.getDbFieldName() + "," + value + ")";
			default:
				throw new IllegalArgumentException(String.format("Received an unsupported modifier. Full query: %s.", geoRel));
		}
	}

	private String getStGeomString(Geometry geometry, String coordinates) {
		return "ST_GeomFromGeoJSON('{\"type\": \"" + geometry.name() + "\", \"coordinates\":" + coordinates + "}')";
	}

}
