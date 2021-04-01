package org.fiware.mintaka;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.runtime.Micronaut;
import org.fiware.mintaka.domain.EntityTemporalListVOSerializer;
import org.fiware.mintaka.domain.EntityTemporalSerializer;
import org.fiware.mintaka.domain.QueryVODeserializer;
import org.fiware.ngsi.model.QueryVO;

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
	 *
	 * @param entityTemporalSerializer json-ld serializer for the temporal entity representation
	 * @return the objectmapper bean
	 */
	@Singleton
	@Replaces(ObjectMapper.class)
	public ObjectMapper objectMapper(EntityTemporalSerializer entityTemporalSerializer, EntityTemporalListVOSerializer entityTemporalListVOSerializer) {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.registerModule(new JavaTimeModule());

		SimpleModule deserializerModifierModule = new SimpleModule();
		deserializerModifierModule.setDeserializerModifier(new BeanDeserializerModifier() {
			@Override
			public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
				if (QueryVO.class.isAssignableFrom(beanDesc.getBeanClass())) {
					return new QueryVODeserializer(deserializer, beanDesc.getBeanClass());
				}
				return deserializer;
			}
		});
		objectMapper.registerModule(deserializerModifierModule);

		SimpleModule entityTemporalModule = new SimpleModule();
		entityTemporalModule.addSerializer(entityTemporalSerializer);
		entityTemporalModule.addSerializer(entityTemporalListVOSerializer);

		objectMapper.registerModule(entityTemporalModule);
		objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
		return objectMapper;
	}

}
