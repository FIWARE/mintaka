package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * Temporal representation of a relationship according to the NGSI-LD spec
 */
@Data
public class TemporalValueRelationship extends AbstractTemporalValue {

	public static final String JSON_PROPERTY_OBJECTS = "objects";

	public String type = "Relationship";

	@JsonProperty(JSON_PROPERTY_OBJECTS)
	private List<List<Object>> objects;

}
