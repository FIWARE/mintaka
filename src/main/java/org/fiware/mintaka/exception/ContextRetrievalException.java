package org.fiware.mintaka.exception;

import lombok.Getter;

/**
 * Should be thrown when a requested context could not have been retrieved.
 */
public class ContextRetrievalException extends RuntimeException {

	@Getter
	private final String contextAddress;

	public ContextRetrievalException(String message, String contextAddress) {
		super(message);
		this.contextAddress = contextAddress;
	}

	public ContextRetrievalException(String message, Throwable cause, String contextAddress) {
		super(message, cause);
		this.contextAddress = contextAddress;
	}
}
