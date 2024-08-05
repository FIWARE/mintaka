package org.fiware.mintaka.exception;

import io.micronaut.http.HttpStatus;
import lombok.Getter;

/**
 * Error types as defined by the NGSI-LD spec.
 */
@Getter
public enum ErrorType {
	INVALID_REQUEST(HttpStatus.BAD_REQUEST, "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest"),
	BAD_REQUEST_DATA(HttpStatus.BAD_REQUEST, "https://uri.etsi.org/ngsi-ld/errors/BadRequestData"),
	OPERATION_NOT_SUPPORTED(HttpStatus.UNPROCESSABLE_ENTITY, "https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported"),
	RESOURCE_NOT_FOUND(HttpStatus.NOT_FOUND, "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound"),
	INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "https://uri.etsi.org/ngsi-ld/errors/InternalError"),
	TOO_COMPLEX_QUERY(HttpStatus.FORBIDDEN, "https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery"),
	TOO_MANY_RESULTS(HttpStatus.FORBIDDEN, "https://uri.etsi.org/ngsi-ld/errors/TooManyResults "),
	LD_CONTEXT_NOT_AVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable"),
	NON_EXISTENT_TENANT(HttpStatus.NOT_FOUND, "https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant ");

	/**
	 * Status to be used with the given error.
	 */
	private final HttpStatus status;
	/**
	 * Error type as defined by the NGSI-LD spec
	 */
	private final String type;

	ErrorType(HttpStatus statusCode, String type) {
		this.status = statusCode;
		this.type = type;
	}
}
