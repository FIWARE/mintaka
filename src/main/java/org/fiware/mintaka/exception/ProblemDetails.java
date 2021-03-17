package org.fiware.mintaka.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Problem details as defined by RFC-7807 {@see https://tools.ietf.org/html/rfc7807} and mandated by the NGSI-LD spec
 * {@see https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.03.01_60/gs_cim009v010301p.pdf} 5.5.2 & 5.5.3
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ProblemDetails {

	/**
	 * Type of the error
	 */
	private String type;
	/**
	 * Title of the error
	 */
	private String title;
	/**
	 * (HTTP) Status code associated with the error
	 */
	private int status;
	/**
	 * (preferably human readable) error details
	 */
	private String detail;
	/**
	 * Id of an instance associated with the problem, null if there is no such instance
	 */
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String instance;

}
