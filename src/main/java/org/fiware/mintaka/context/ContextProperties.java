package org.fiware.mintaka.context;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("context")
@Data
public class ContextProperties {

	private String defaultUrl = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld";

}
