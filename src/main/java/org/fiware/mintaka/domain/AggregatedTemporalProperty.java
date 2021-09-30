package org.fiware.mintaka.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

import static org.fiware.mintaka.domain.TemporalValuesEntity.JSON_PROPERTY_TYPE;

@Data
public class AggregatedTemporalProperty {

	private static final String MAX_PROPERTY = "max";
	private static final String MIN_PROPERTY = "min";
	private static final String AVG_PROPERTY = "avg";
	private static final String TOTAL_COUNT_PROPERTY = "totalCount";
	private static final String DISTINCT_COUNT_PROPERTY = "distinctCount";
	private static final String SUM_PROPERTY = "sum";
	private static final String STD_DEV_PROPERTY = "stddev";
	private static final String SUM_SQ_PROPERTY = "sumsq";

	@JsonIgnore
	public String id;

	@JsonProperty(JSON_PROPERTY_TYPE)
	public String type;

	@JsonProperty(MAX_PROPERTY)
	private List<List> maxList = null;
	@JsonProperty(MIN_PROPERTY)
	private List<List> minList = null;
	@JsonProperty(AVG_PROPERTY)
	private List<List> avgList = null;
	@JsonProperty(TOTAL_COUNT_PROPERTY)
	private List<List> totalCountList = null;
	@JsonProperty(DISTINCT_COUNT_PROPERTY)
	private List<List> distinctCountList = null;
	@JsonProperty(SUM_PROPERTY)
	private List<List> sumList = null;
	@JsonProperty(STD_DEV_PROPERTY)
	private List<List> stddevList = null;
	@JsonProperty(SUM_SQ_PROPERTY)
	private List<List> sumsqList = null;
}
