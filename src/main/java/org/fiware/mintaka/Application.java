package org.fiware.mintaka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.jackson.JacksonConfiguration;
import io.micronaut.jackson.ObjectMapperFactory;
import io.micronaut.runtime.Micronaut;
import org.fiware.mintaka.context.LdContextCache;
import org.fiware.mintaka.domain.EntityTemporalSerializer;

import javax.inject.Singleton;
import java.util.Optional;

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
	public ObjectMapper objectMapper(EntityTemporalSerializer entityTemporalSerializer) {
		ObjectMapper objectMapper = new ObjectMapper();
		SimpleModule entityTemporalModule = new SimpleModule();
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.registerModule(new JavaTimeModule());
		entityTemporalModule.addSerializer(entityTemporalSerializer);
		objectMapper.registerModule(entityTemporalModule);
		return objectMapper;
	}

}
