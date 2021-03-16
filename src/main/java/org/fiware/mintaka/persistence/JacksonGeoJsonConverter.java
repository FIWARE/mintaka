package org.fiware.mintaka.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.exception.JacksonConversionException;
import org.fiware.mintaka.exception.PersistenceRetrievalException;
import org.geojson.GeoJsonObject;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.io.IOException;

/**
 * Hibernate {@link AttributeConverter} to translate geoJson strings int geojson objects and vice-a-versa
 */
@Slf4j
@Converter(autoApply = true)
public class JacksonGeoJsonConverter implements AttributeConverter<GeoJsonObject, String> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(GeoJsonObject meta) {
        if (meta == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(meta);
        } catch (JsonProcessingException ex) {
            log.debug("Was not able to translate db object: %s", meta);
            throw new JacksonConversionException("Was not able to convert to database column.", ex);
        }
    }

    @Override
    public GeoJsonObject convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(dbData, GeoJsonObject.class);
        } catch (IOException ex) {
            log.debug("Was not able to translate db object: %s", dbData);
            throw new PersistenceRetrievalException("Was not able to translate database object.", ex);
        }
    }

}
