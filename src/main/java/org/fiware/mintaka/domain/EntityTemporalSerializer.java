package org.fiware.mintaka.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.context.ServerRequestContext;
import io.micronaut.runtime.http.scope.RequestScope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.Opt;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.ngsi.model.EntityTemporalVO;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.jsonldjava.core.JsonLdOptions.JSON_LD_1_1;

/**
 * Serializer for {@link EntityTemporalVO} to a json-ld string.
 */
@Slf4j
@RequiredArgsConstructor
@Singleton
public class EntityTemporalSerializer extends JsonSerializer<EntityTemporalVO> {

	private static final String OPTIONS_KEY = "options";
	private static final String TIME_PROPERTY_KEY = "timeproperty";
	private static final String TEMPORAL_VALUES_OPTION = "temporalValues";

	private final TemporalValuesMapper temporalValuesMapper;

	// we cannot take the bean from the context, since that will be circular reference, e.g. stack-overflow
	private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	{
		OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		OBJECT_MAPPER.registerModule(new JavaTimeModule());
	}

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
			Map<String, Object> mapRepresentation;
			// decide about the representation type.
			if (isTemporalValuesOptionSet()) {
				TemporalValuesEntity tve = temporalValuesMapper.entityTemporalToTemporalValuesEntity(value, getRequestedTimeProperty());
				mapRepresentation = OBJECT_MAPPER.convertValue(tve, Map.class);
			} else {
				mapRepresentation = OBJECT_MAPPER.convertValue(value, Map.class);
			}
			// extract the referenced context from the cache
			Object context = ldContextCache.getContext(value.atContext());
			// compact the expanded object with the present context
			Map<String, Object> compactedObject = JsonLdProcessor.compact(mapRepresentation, context, JSON_LD_OPTIONS);
			// remove the full context
			compactedObject.remove(JsonLdConsts.CONTEXT);
			// add context references
			compactedObject.put("@context", value.atContext());
			// write the serialized object back to the generator
			gen.writeRaw(JsonUtils.toPrettyString(compactedObject));
		} catch (IOException e) {
			log.error("Was not able to deserialize object", e);
			// bubble to fulfill interface
			throw e;
		}
	}

	private TimeStampType getRequestedTimeProperty() {
		Optional<String> optionalTimeProperty = ServerRequestContext.currentRequest()
				.map(HttpRequest::getParameters)
				.map(params -> params.get(TIME_PROPERTY_KEY));
		if (optionalTimeProperty.isEmpty()) {
			return TimeStampType.OBSERVED_AT;
		}
		return TimeStampType.valueOf(optionalTimeProperty.get());
	}

	private boolean isTemporalValuesOptionSet() {
		Optional<String> optionalOptions = ServerRequestContext.currentRequest()
				.map(HttpRequest::getParameters)
				.map(params -> params.get(OPTIONS_KEY));
		if (optionalOptions.isEmpty()) {
			return false;
		}
		return Arrays.stream(optionalOptions.get().split(",")).anyMatch(TEMPORAL_VALUES_OPTION::equals);
	}

}
