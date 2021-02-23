package org.fiware.mintaka.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.ngsi.model.EntityTemporalListVO;
import org.fiware.ngsi.model.EntityTemporalVO;

import javax.inject.Singleton;
import java.io.IOException;

/**
 * Specific serializer for the results of an temporal query.
 */
@Slf4j
@RequiredArgsConstructor
@Singleton
public class EntityTemporalListVOSerializer extends JsonSerializer<EntityTemporalListVO> {

	@Override
	public Class<EntityTemporalListVO> handledType() {
		return EntityTemporalListVO.class;
	}

	@Override
	public void serialize(EntityTemporalListVO entityTemporalListVO, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		JsonSerializer entityTemporalSerializer = serializers.findValueSerializer(EntityTemporalVO.class);
		int lastPos = entityTemporalListVO.size() - 1;
		gen.writeStartArray(entityTemporalListVO, entityTemporalListVO.size());
		for (int i = 0; i < lastPos; i++) {
				entityTemporalSerializer.serialize(entityTemporalListVO.get(i), gen, serializers);
				gen.writeRaw(", ");
		}
		entityTemporalSerializer.serialize(entityTemporalListVO.get(lastPos), gen, serializers);
		gen.writeEndArray();
	}
}
