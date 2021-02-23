package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TemporalValueProperty extends AbstractTemporalValue {

	public static final String JSON_PROPERTY_VALUES = "values";

	public String type = "Property";

	@JsonProperty(JSON_PROPERTY_VALUES)
	private List<List<Object>> values;

}
