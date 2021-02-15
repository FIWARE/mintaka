package org.fiware.mintaka.exception;

/**
 * Should be thrown if the given attributes cannot be expanded.
 */
public class AttributeExpansionException extends RuntimeException{
	public AttributeExpansionException(String message) {
		super(message);
	}

	public AttributeExpansionException(String message, Throwable cause) {
		super(message, cause);
	}
}
