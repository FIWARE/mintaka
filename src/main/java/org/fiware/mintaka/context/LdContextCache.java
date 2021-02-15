package org.fiware.mintaka.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Context;
import lombok.RequiredArgsConstructor;
import org.fiware.mintaka.exception.AttributeExpansionException;
import org.fiware.mintaka.exception.ContextRetrievalException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cache for ld context. Provides additional functionality to work with the context.
 */
@Context
@CacheConfig("contexts")
@RequiredArgsConstructor
public class LdContextCache {

	public static final JsonLdOptions JSON_LD_OPTIONS = new JsonLdOptions();

	private static final URL CORE_CONTEXT_URL;
	private static final Object CORE_CONTEXT;

	static {
		try {
			CORE_CONTEXT_URL = new URL("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld");
			CORE_CONTEXT = JsonUtils.fromURLJavaNet(CORE_CONTEXT_URL);
		} catch (IOException e) {
			throw new ContextRetrievalException("Invalid core context configured.");
		}
	}

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	/**
	 * Get context from the given url. Will be cached.
	 * @param url - url to get the context from
	 * @return the context
	 */
	@Cacheable
	public Object getContextFromURL(URL url) {
		if (url.equals(CORE_CONTEXT_URL)) {
			return CORE_CONTEXT;
		}
		try {
			return JsonUtils.fromURLJavaNet(url);
		} catch (IOException e) {
			throw new ContextRetrievalException(String.format("Was not able to retrieve context from %s.", url), e);
		}
	}

	/**
	 * Expand all given attributes with the given contexts.
	 *
	 * @param attributes - attributes to be expanded
	 * @param contextUrls - urls of contexts to be used for expansion
	 * @return list of expanded attribute-ids
	 */
	public List<String> expandAttributes(List<String> attributes, List<URL> contextUrls) {
		Map contextMap = (Map) getContext(contextUrls);

		return attributes.stream()
				.map(this::getJsonLdAttribute)
				.map(jsonLdAttribute -> {
					try {
						Map jsonLdObject = (Map) JsonUtils.fromString(jsonLdAttribute);
						jsonLdObject.put(JsonLdConsts.CONTEXT, contextMap.get(JsonLdConsts.CONTEXT));
						return jsonLdObject;
					} catch (IOException e) {
						throw new AttributeExpansionException(String.format("Was not able expand %s.", jsonLdAttribute), e);
					}
				})
				.map(this::getIdFromJsonLDObject)
				.collect(Collectors.toList());
	}

	/**
	 * Get the context from the given object. Should either be a (URL)String, a URL or a list of urls/urlstrings.
	 * @param contextURLs - either be a (URL)String, a URL or a list of urls/urlstrings.
	 * @return the context
	 */
	public Object getContext(Object contextURLs) {
		if (contextURLs instanceof List) {
			Object compactedContext = ((List) contextURLs).stream().map(urlObject -> getContext(urlObject))
					.collect(Collectors.reducing((c1, c2) -> JsonLdProcessor.compact(c1, c2, JSON_LD_OPTIONS)));
			if (compactedContext instanceof Optional) {
				return ((Optional<?>) compactedContext).orElseThrow(() -> new ContextRetrievalException("Was not able to get compacted context."));
			}
			return compactedContext;
		} else if (contextURLs instanceof URL) {
			return getContextFromURL((URL) contextURLs);
		} else if (contextURLs instanceof String) {
			return getContextFromURL((String) contextURLs);
		}
		throw new ContextRetrievalException(String.format("Did not receive a valid context: %s.", contextURLs));
	}

	/**
	 * Get the context from the given url
	 * @param urlString - string containing the url
	 * @return the context
	 */
	public Object getContextFromURL(String urlString) {
		try {
			return getContextFromURL(new URL(urlString));
		} catch (MalformedURLException e) {
			throw new ContextRetrievalException(String.format("Was not able to convert %s to URL.", urlString), e);
		}
	}


	/**
	 * Extract the context urls from the link header. CORE_CONTEXT will be automatically added.
	 * @param headerString - content of the link header
	 * @return list of context urls, will either be only the core context or the core-context + the header context
	 */
	public static List<URL> getContextURLsFromLinkHeader(String headerString) {

		Optional<String> linkedContextString = Optional.empty();

		if (headerString != null && !headerString.isEmpty() && !headerString.isBlank()) {
			linkedContextString = Optional.of(headerString.split(";")[0].replace("<", "").replace(">", ""));
		}

		return linkedContextString
				.map(lCS -> {
					try {
						return new URL(lCS);
					} catch (MalformedURLException e) {
						throw new ContextRetrievalException("Was not able to get context url from the Link-header.", e);
					}
				})
				// core-context needs to be first, so that it can be overwritten by the provided context.
				.map(url -> List.of(CORE_CONTEXT_URL, url)).orElse(List.of(CORE_CONTEXT_URL));
	}

	// extract the Id from the expanded object
	private String getIdFromJsonLDObject(Map<String, Object> jsonLdObject) {
		Map<String, Object> expandedObject = (Map<String, Object>) JsonLdProcessor.expand(jsonLdObject)
				.stream()
				.findFirst()
				.orElseThrow(() -> new AttributeExpansionException(String.format("Was not able to get an expanded object for %s.", jsonLdObject)));
		Set<String> expandedKeys = expandedObject.keySet();
		if (expandedKeys.size() != 1) {
			throw new AttributeExpansionException(String.format("Was not able to correctly expand key. Got multiple keys: %s", expandedKeys));
		}
		return expandedKeys.iterator().next();
	}

	// create a json object for json-ld api to be used for extending the key.
	private String getJsonLdAttribute(String attribute) {
		return String.format("{\"%s\":\"\"}", attribute);
	}

}
