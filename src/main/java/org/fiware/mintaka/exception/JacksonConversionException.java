package org.fiware.mintaka.exception;

import org.fiware.mintaka.persistence.JacksonGeoJsonConverter;

/**
 * Should be thrown when the {@link JacksonGeoJsonConverter} fails to convert the db objects.
 */
public class JacksonConversionException extends RuntimeException {

    public JacksonConversionException(String message) {
        super(message);
    }

    public JacksonConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
