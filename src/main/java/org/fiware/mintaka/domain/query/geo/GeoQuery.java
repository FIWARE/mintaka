package org.fiware.mintaka.domain.query.geo;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Pojo to hold informations about a geography query according to ngsi-ld
 */
public class GeoQuery {

	private static final String SQL_AND_OPERATOR = " AND ";
	private static final String NGSI_LD_GEO_EQUALS = "==";
	private static final List<String> GEO_DB_FIELDS = Arrays.stream(Geometry.values()).map(Geometry::getDbFieldName).collect(Collectors.toList());

	private final String geoRel;
	private final String geomString;
	@Getter
	private final String geoProperty;

	public GeoQuery(String geoRel, Geometry geometry, String coordinates, String geoProperty) {
		this.geoRel = geoRel;
		this.geomString = getStGeomString(geometry, coordinates);
		this.geoProperty = geoProperty;
	}

	/**
	 * Return  an sql representation of the query
	 * @return the sql string
	 */
	public String toSQLQuery() {
		List<String> geoOperations =
				Lists.partition(Arrays.asList(geoRel.split(";")), 2)
						.stream()
						.map(this::getOperationSqlString)
						.collect(Collectors.toList());
		return String.join(SQL_AND_OPERATOR, geoOperations);
	}

	/**
	 * Return the sql according to the given operation
	 */
	private String getOperationSqlString(List<String> operation) {
		if (!(operation.size() == 2 || operation.size() == 1)) {
			throw new IllegalArgumentException(String.format("The requested geo operation is invalid: %s. Full query: %s", operation, geoRel));
		}
		GeoOperation geoOperation = GeoOperation.getEnum(operation.get(0));

		switch (geoOperation) {
			case NEAR:
				if(operation.size()!= 2) {
					throw new IllegalArgumentException("Near requires a maxDistance or minDistance configuration.");
				}
				String[] splittedModifier = operation.get(1).split(NGSI_LD_GEO_EQUALS);
				return getNearOperation(geoOperation, GeoModifier.getEnum(splittedModifier[0]), Long.valueOf(splittedModifier[1]));
			// they all have the same method signature.
			case EQUALS:
			case WITHIN:
			case CONTAINS:
			case DISJOINT:
			case OVERLAPS:
			case INTERSECT:
				return
						"(" + GEO_DB_FIELDS.stream().map(field -> geoOperation.getPostgisFunction() + "(" + geomString + ", attribute." + field + ")").collect(Collectors.joining(" OR ")) + ")";
			default:
				throw new IllegalArgumentException(String.format("Received an unsupported geoOperation: %s. Full query: %s", operation, geoRel));
		}
	}

	/**
	 * Get a near query based on the given operation
	 */
	private String getNearOperation(GeoOperation geoOperation, GeoModifier modifier, Long value) {
		switch (modifier) {
			case MAX_DISTANCE:
				return "(" + GEO_DB_FIELDS.stream().map(field -> geoOperation.getPostgisFunction() + "(" + geomString + ", attribute." + field + "," + value + ")").collect(Collectors.joining(" OR ")) + ")";
			case MIN_DISTANCE:
				return "(" + GEO_DB_FIELDS.stream().map(field -> geoOperation.getPostgisFunction() + "NOT (" + geomString + ", attribute." + field + "," + value + ")").collect(Collectors.joining(" OR ")) + ")";
			default:
				throw new IllegalArgumentException(String.format("Received an unsupported modifier. Full query: %s.", geoRel));
		}
	}

	/**
	 * Add SRID informations for the given geometry
	 */
	private String getStGeomString(Geometry geometry, String coordinates) {
		switch (geometry) {
			case POINT:
			case MULTIPOLYGON:
			case POLYGON:
			case MULTILINESTRING:
			case LINESTRING:
				return "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry.name() + "\", \"coordinates\":" + coordinates + "}'), 4326)";
			default:
				throw new IllegalArgumentException(String.format("Received an unsupported geometry: %s", geometry));
		}
	}

}
