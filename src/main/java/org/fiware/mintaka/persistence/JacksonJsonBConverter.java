package org.fiware.mintaka.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.exception.PersistenceRetrievalException;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.io.IOException;

@Slf4j
@Converter(autoApply = true)
public class JacksonJsonBConverter implements AttributeConverter<Object, String> {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Override
	public String convertToDatabaseColumn(Object attribute) {
		throw new UnsupportedOperationException("Writing to the database is not supported.");
	}

	@Override
	public Object convertToEntityAttribute(String dbData) {
		if (dbData == null) {
			return null;
		}
		try {
			return OBJECT_MAPPER.readValue(dbData, Object.class);
		} catch (IOException ex) {
			log.debug("Was not able to translate db object: %s", dbData);
			throw new PersistenceRetrievalException("Was not able to translate database object.", ex);
		}
	}
}
