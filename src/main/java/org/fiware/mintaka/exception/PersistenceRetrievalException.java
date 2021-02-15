package org.fiware.mintaka.exception;

/**
 * Should be thrown if something unexpected happens when retrieving data from the persistence layer.
 */
public class PersistenceRetrievalException extends RuntimeException {

	public PersistenceRetrievalException(String message) {
		super(message);
	}

	public PersistenceRetrievalException(String message, Throwable cause) {
		super(message, cause);
	}
}
