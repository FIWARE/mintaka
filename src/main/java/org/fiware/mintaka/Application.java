package org.fiware.mintaka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.runtime.Micronaut;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.EntityTemporalSerializer;

import javax.inject.Singleton;

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
	 * @param ldContextCache cache for json-ld context
	 * @return the objectmapper bean
	 */
	@Singleton
	@Replaces(ObjectMapper.class)
	public ObjectMapper objectMapper(LdContextCache ldContextCache) {
		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule entityTemporalModule = new SimpleModule();
		entityTemporalModule.addSerializer(new EntityTemporalSerializer(ldContextCache));
		objectMapper.registerModule(entityTemporalModule);
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		return objectMapper;
	}
}
