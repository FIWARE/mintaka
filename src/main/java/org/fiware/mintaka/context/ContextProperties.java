package org.fiware.mintaka.context;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("context")
@Data
public class ContextProperties {

	// default needs to be >= 1.4, since the list-of-list representations defined in the aggregated temporal representation responses are flattend in
	// everything below
	private String defaultUrl = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld";

}
