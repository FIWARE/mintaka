package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import static org.fiware.mintaka.domain.TemporalValuesEntity.JSON_PROPERTY_TYPE;

public abstract class AbstractTemporalValue {

	@JsonProperty(JSON_PROPERTY_TYPE)
	public String type;

}
