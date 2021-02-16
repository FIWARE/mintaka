package org.fiware.mintaka.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.ngsi.model.EntityTemporalVO;

import java.io.IOException;
import java.util.Map;

/**
 * Serializer for {@link EntityTemporalVO} to a json-ld string.
 */
@Slf4j
@RequiredArgsConstructor
public class EntityTemporalSerializer extends JsonSerializer<EntityTemporalVO> {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final JsonLdOptions JSON_LD_OPTIONS = new JsonLdOptions();
	private final LdContextCache ldContextCache;

	@Override
	public Class<EntityTemporalVO> handledType() {
		return EntityTemporalVO.class;
	}

	/*
	 * Serializes an {@link EntityTemporalVO} to a compacted JsonLD representation, using the context linked in the VO.
	 */
	@Override
	public void serialize(EntityTemporalVO value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

		try {
			Map mapRepresentation = OBJECT_MAPPER.convertValue(value, Map.class);
			// expand the raw object
			Object expandedObject = JsonLdProcessor.expand(mapRepresentation);
			// extract the referenced context from the cache
			Object context = ldContextCache.getContext(value.atContext());
			// compact the expanded object with the present context
			Map<String, Object> compactedObject = JsonLdProcessor.compact(expandedObject, context, JSON_LD_OPTIONS);
			// remove the full context
			compactedObject.remove(JsonLdConsts.CONTEXT);
			// add context references
			compactedObject.put("@context", value.atContext());
			// write the serialized object back to the generator
			gen.writeRaw(JsonUtils.toPrettyString(compactedObject));
		}
		catch(IOException e){
			log.error("Was not able to deserialize object", e);
			// bubble to fulfill interface
			throw e;
		}
	}



}
