package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.fiware.ngsi.model.GeoPropertyVO;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Temporal representation of an entity according to the NGSI-LD spec
 */
@Data
public class TemporalValuesEntity {

	public static final String JSON_PROPERTY_AT_CONTEXT = "@context";
	public static final String JSON_PROPERTY_LOCATION = "location";
	public static final String JSON_PROPERTY_OBSERVATION_SPACE = "observationSpace";
	public static final String JSON_PROPERTY_OPERATION_SPACE = "operationSpace";
	public static final String JSON_PROPERTY_ID = "id";
	public static final String JSON_PROPERTY_TYPE = "type";

	@JsonProperty(JSON_PROPERTY_AT_CONTEXT)
	private Object atContext;

	@JsonProperty(JSON_PROPERTY_LOCATION)
	private TemporalValueProperty location;

	@JsonProperty(JSON_PROPERTY_OBSERVATION_SPACE)
	private TemporalValueProperty observationSpace;

	@JsonProperty(JSON_PROPERTY_OPERATION_SPACE)
	private TemporalValueProperty operationSpace;

	@JsonProperty(JSON_PROPERTY_ID)
	private URI id;

	@JsonProperty(JSON_PROPERTY_TYPE)
	private String type;

	private Map<String, AbstractTemporalValue> additionalProperties;

	@JsonAnyGetter
	public Map<String, AbstractTemporalValue> getAdditionalProperties() {
		return additionalProperties;
	}
}
