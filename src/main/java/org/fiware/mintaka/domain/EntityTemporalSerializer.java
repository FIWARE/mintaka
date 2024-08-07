package org.fiware.mintaka.domain;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.api.CompactionApi;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.jsonldjava.core.JsonLdOptions;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.context.ServerRequestContext;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.query.temporal.TimeStampType;
import org.fiware.mintaka.exception.JacksonConversionException;
import org.fiware.ngsi.model.EntityTemporalVO;
import org.geojson.GeoJsonObject;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
	public static final String CONTEXT_KEY = "@context";

	private final DocumentLoader documentLoader;
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
			AcceptType acceptType = getAcceptType();
			Object contextObject = value.atContext();
			String jsonString;
			// decide about the representation type.
			if (isTemporalValuesOptionSet()) {
				TemporalValuesEntity tve = temporalValuesMapper.entityTemporalToTemporalValuesEntity(value, getRequestedTimeProperty());
				jsonString = OBJECT_MAPPER.writeValueAsString(tve);
			} else {
				jsonString = OBJECT_MAPPER.writeValueAsString(value);
			}
			Document context = ldContextCache.getContextDocument(contextObject);
			CompactionApi compactionApi = JsonLd.compact(JsonDocument.of(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8))), context);
			JsonObject jsonObject = compactionApi.loader(documentLoader).get();

			// create an builder for the compacted object
			JsonObjectBuilder compactedJsonBuilder = Json.createObjectBuilder(jsonObject);

			switch (acceptType) {
				case JSON_LD:
					// add the context as URL instead of fully embed it.
					if (contextObject instanceof URL) {
						compactedJsonBuilder.add(CONTEXT_KEY, contextObject.toString());
					} else if (contextObject instanceof List) {
						JsonArrayBuilder contextArrayBuilder = Json.createArrayBuilder();
						((List<URL>) contextObject).forEach(contextItem -> contextArrayBuilder.add(contextItem.toString()));
						compactedJsonBuilder.add(CONTEXT_KEY, contextArrayBuilder);
					} else {
						throw new IllegalArgumentException(String.format("Context is invalid: %s", value.atContext()));
					}
					gen.writeRaw(compactedJsonBuilder.build().toString());
					break;
				case JSON:
					// fallthrough, since JSON is the default
				default:
					compactedJsonBuilder.remove(CONTEXT_KEY);
					gen.writeRaw(compactedJsonBuilder.build().toString());
					break;

			}
		} catch (IOException e) {
			log.error("Was not able to deserialize object", e);
			// bubble to fulfill interface
			throw new JacksonConversionException("Was not able to deserialize the retrieved object.", e);
		} catch (JsonLdError jsonLdError) {
			log.error("Was not able to deserialize object", jsonLdError);
 			throw new JacksonConversionException(jsonLdError.getMessage());
		}
	}

	private TimeStampType getRequestedTimeProperty() {
		Optional<String> optionalTimeProperty = ServerRequestContext.currentRequest()
				.map(HttpRequest::getParameters)
				.map(params -> params.get(TIME_PROPERTY_KEY));
		if (optionalTimeProperty.isEmpty()) {
			return TimeStampType.OBSERVED_AT;
		}
		return TimeStampType.getEnum(optionalTimeProperty.get());
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

	private AcceptType getAcceptType() {
		return ServerRequestContext.currentRequest()
				.map(HttpRequest::getHeaders)
				.map(headers -> headers.get("Accept"))
				.map(AcceptType::getEnum)
				// according to NGSI-LD spec 6.3.4 is application/json the default
				.orElse(AcceptType.JSON);
	}

}
