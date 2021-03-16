package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Temporal representation of a geo property according to the NGSI-LD spec
 */
@Data
public class TemporalValueGeoProperty extends AbstractTemporalValue {

	public static final String JSON_PROPERTY_VALUES = "values";

	public String type = "GeoProperty";

	@JsonProperty(JSON_PROPERTY_VALUES)
	private List<List<Object>> values;

}
