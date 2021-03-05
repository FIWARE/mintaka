package org.fiware.mintaka;

import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.HttpLoader;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.runtime.Micronaut;
import org.fiware.mintaka.context.CacheableDocumentLoader;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.EntityTemporalListVOSerializer;
import org.fiware.mintaka.domain.EntityTemporalSerializer;

import javax.inject.Singleton;
import java.net.http.HttpClient;

/**
 * Base application as starting point
 */
@Factory
public class Application {

	public static void main(String[] args) {
		Micronaut.run(Application.class, args);
	}

	/**
	 * Replacement for the default {@link ObjectMapper} to serialize according to JSON-LD
	 *
	 * @param entityTemporalSerializer json-ld serializer for the temporal entity representation
	 * @return the objectmapper bean
	 */
	@Singleton
	@Replaces(ObjectMapper.class)
	public ObjectMapper objectMapper(EntityTemporalSerializer entityTemporalSerializer, EntityTemporalListVOSerializer entityTemporalListVOSerializer) {
		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule entityTemporalModule = new SimpleModule();
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.registerModule(new JavaTimeModule());
		entityTemporalModule.addSerializer(entityTemporalSerializer);
		entityTemporalModule.addSerializer(entityTemporalListVOSerializer);
		objectMapper.registerModule(entityTemporalModule);
		objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
		return objectMapper;
	}

}
