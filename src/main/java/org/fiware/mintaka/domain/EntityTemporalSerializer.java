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

@Slf4j
@RequiredArgsConstructor
public class EntityTemporalSerializer extends JsonSerializer<EntityTemporalVO> {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final LdContextCache ldContextCache;

	@Override
	public Class<EntityTemporalVO> handledType() {
		return EntityTemporalVO.class;
	}

	@Override
	public void serialize(EntityTemporalVO value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

		try {
			Map o = OBJECT_MAPPER.convertValue(value, Map.class);
			Object expandedObject = JsonLdProcessor.expand(o);
			Object context = ldContextCache.getContext(value.atContext());
			Map<String, Object> compactedObject = JsonLdProcessor.compact(expandedObject, context, new JsonLdOptions());
			compactedObject.remove(JsonLdConsts.CONTEXT);
			// do not embed the full context.
			compactedObject.put("@context", value.atContext());
			String serializedObject = JsonUtils.toPrettyString(compactedObject);
			gen.writeRaw(serializedObject);
		}
		catch(IOException e){
			log.error("Was not able to deserialize object", e);
			// bubble to fulfill interface
			throw e;
		}
	}



}
