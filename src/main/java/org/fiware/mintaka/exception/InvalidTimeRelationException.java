package org.fiware.mintaka.exception;

/**
 * Should be thrown if a request with an invalid time/timerelation configuration is received.
 */
public class InvalidTimeRelationException extends RuntimeException {
    public InvalidTimeRelationException(String message) {
        super(message);
    }

    public InvalidTimeRelationException(String message, Throwable cause) {
        super(message, cause);
    }
}
