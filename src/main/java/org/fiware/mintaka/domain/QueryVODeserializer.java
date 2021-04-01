package org.fiware.mintaka.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.fiware.ngsi.model.EntityInfoVO;
import org.fiware.ngsi.model.GeoQueryVO;
import org.fiware.ngsi.model.QueryVO;
import org.fiware.ngsi.model.TemporalQueryVO;

import java.io.IOException;
import java.util.Optional;

/**
 * Dedicated deserializer for QueryVOs, to cleanup empty objects.
 */
@Slf4j
public class QueryVODeserializer extends StdDeserializer<QueryVO> implements ResolvableDeserializer {

	private final JsonDeserializer<?> defaultDeserializer;

	public QueryVODeserializer(JsonDeserializer<?> defaultDeserializer, Class<?> clazz) {
		super(clazz);
		this.defaultDeserializer = defaultDeserializer;
	}

	@Override
	public void resolve(DeserializationContext ctxt) throws JsonMappingException {
		((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
	}

	@Override
	public QueryVO deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException {

		Object itemObj = defaultDeserializer.deserialize(p, ctxt);
		if(itemObj instanceof QueryVO) {
			return cleanQueryVO((QueryVO) itemObj);
		}
		throw new IllegalArgumentException("Was not able deserialze the query.");
	}


	private QueryVO cleanQueryVO(QueryVO queryVO) {
		if (isEntityInfoEmpty(queryVO.entities())) {
			queryVO.entities(null);
		}
		if (isTemporalQueryEmpty(queryVO.temporalQ())) {
			queryVO.temporalQ(null);
		}
		if(isGeoQueryEmpty(queryVO.geoQ())) {
			queryVO.geoQ(null);
		}
		return queryVO;
	}

	private boolean isGeoQueryEmpty(GeoQueryVO geoQueryVO) {
		if (Optional.ofNullable(geoQueryVO. coordinates()).filter(list -> !list.isEmpty()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(geoQueryVO.geometry()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(geoQueryVO.geoproperty()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(geoQueryVO.georel()).isPresent()) {
			return false;
		}
		return true;
	}

	private boolean isTemporalQueryEmpty(TemporalQueryVO temporalQueryVO) {
		if (Optional.ofNullable(temporalQueryVO.endTimeAt()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(temporalQueryVO.timeAt()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(temporalQueryVO.timerel()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(temporalQueryVO.timeproperty()).isPresent()) {
			return false;
		}
		return true;
	}

	private boolean isEntityInfoEmpty(EntityInfoVO entityInfoVO) {
		if (Optional.ofNullable(entityInfoVO.getId()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(entityInfoVO.getIdPattern()).isPresent()) {
			return false;
		} else if (Optional.ofNullable(entityInfoVO.getType()).isPresent()) {
			return false;
		}
		return true;
	}
}
