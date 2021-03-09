package org.fiware.mintaka.exception;

/**
 * Should be thrown when a requested context could not have been retrieved.
 */
public class ContextRetrievalException extends RuntimeException {

	public ContextRetrievalException(String message) {
		super(message);
	}

	public ContextRetrievalException(String message, Throwable cause) {
		super(message, cause);
	}
}
