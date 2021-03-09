package org.fiware.mintaka.exception;

/**
 * Should be thrown if the given attributes cannot be expanded.
 */
public class StringExpansionException extends RuntimeException{
	public StringExpansionException(String message) {
		super(message);
	}

	public StringExpansionException(String message, Throwable cause) {
		super(message, cause);
	}
}
