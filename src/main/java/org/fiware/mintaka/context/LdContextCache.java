package org.fiware.mintaka.context;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Context;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fiware.mintaka.exception.ContextRetrievalException;
import org.fiware.mintaka.exception.StringExpansionException;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cache for ld context. Provides additional functionality to work with the context.
 */
@Slf4j
@Context
@CacheConfig("contexts")
@RequiredArgsConstructor
public class LdContextCache {

	private final ContextProperties contextProperties;

	private URL coreContextUrl;
	private Object coreContext;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@PostConstruct
	public void initDefaultContext() {
		try {
			coreContextUrl = new URL(contextProperties.getDefaultUrl());
			coreContext = JsonUtils.fromURLJavaNet(coreContextUrl);
		} catch (IOException e) {
			throw new ContextRetrievalException("Invalid core context configured.", e, contextProperties.getDefaultUrl());
		}
	}

	/**
	 * Get context from the given url. Will be cached.
	 *
	 * @param url - url to get the context from
	 * @return the context
	 */
	@Cacheable
	public Object getContextFromURL(URL url) {
		try {
			if (url.toURI().equals(coreContextUrl.toURI())) {
				return coreContext;
			}
			return JsonUtils.fromURLJavaNet(url);
		} catch (IOException e) {
			throw new ContextRetrievalException(String.format("Was not able to retrieve context from %s.", url), e, url.toString());
		} catch (URISyntaxException uriSyntaxException) {
			throw new ContextRetrievalException(String.format("Received an invalid url: %s", url), uriSyntaxException, url.toString());
		}
	}

	/**
	 * Expand all given attributes with the given contexts.
	 *
	 * @param stringsToExpand - strings to be expanded
	 * @param contextUrls     - urls of contexts to be used for expansion
	 * @return list of expanded attribute-ids
	 */
	public List<String> expandStrings(List<String> stringsToExpand, List<URL> contextUrls) {
		Map contextMap = (Map) getContext(contextUrls);

		return stringsToExpand.stream()
				.map(stringToExpand -> expandString(stringToExpand, contextMap))
				.collect(Collectors.toList());
	}

	/**
	 * Expand the given string with the provided contexts.
	 *
	 * @param stringToExpand - string to be expanded
	 * @param contextUrls    - urls of contexts to be used for expansion
	 * @return the expanded attribute-id
	 */
	public String expandString(String stringToExpand, List<URL> contextUrls) {
		return expandString(stringToExpand, (Map) getContext(contextUrls));
	}

	private String expandString(String stringToExpand, Map contextMap) {
		String jsonLdString = getJsonLdString(stringToExpand);
		try {
			Map jsonLdObject = (Map) JsonUtils.fromString(jsonLdString);
			jsonLdObject.put(JsonLdConsts.CONTEXT, contextMap.get(JsonLdConsts.CONTEXT));
			return getIdFromJsonLDObject(jsonLdObject);
		} catch (IOException e) {
			throw new StringExpansionException(String.format("Was not able expand %s.", jsonLdString), e);
		}
	}

	/**
	 * Retrieve the context as a JsonDocument
	 *
	 * @param contextURLs - either be a (URL)String, a URL or a list of urls/urlstrings.
	 * @return the context
	 */
	public Document getContextDocument(Object contextURLs) {
		try {
			return JsonDocument.of(new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsString(getContext(contextURLs)).getBytes(StandardCharsets.UTF_8)));
		} catch (JsonLdError | JsonProcessingException e) {
			throw new IllegalArgumentException(String.format("No valid context available via %s", contextURLs), e);
		}
	}

	/**
	 * Get the context from the given object. Should either be a (URL)String, a URL or a list of urls/urlstrings.
	 * We use the Json-ld-java lib for retrieval, since the titanium lib is not able to combine context objects.
	 *
	 * @param contextURLs - either be a (URL)String, a URL or a list of urls/urlstrings.
	 * @return the context
	 */
	private Object getContext(Object contextURLs) {
		if (contextURLs instanceof List) {
			return Map.of(JsonLdConsts.CONTEXT, ((List) contextURLs).stream()
					.map(this::getContext)
					.map(contextMap -> ((Map<String, Object>) contextMap).get(JsonLdConsts.CONTEXT))
					.flatMap(map -> ((Map<String, Object>) map).entrySet().stream())
					.collect(Collectors.toMap(e -> ((Map.Entry<String, Object>) e).getKey(), e -> ((Map.Entry<String, Object>) e).getValue(), (e1, e2) -> e2)));
		} else if (contextURLs instanceof URL) {
			return getContextFromURL((URL) contextURLs);
		} else if (contextURLs instanceof String) {
			return getContextFromURL((String) contextURLs);
		} else if (contextURLs instanceof URI) {
			return getContextFromURL(contextURLs.toString());
		}
		throw new ContextRetrievalException(String.format("Did not receive a valid context: %s.", contextURLs), contextURLs.toString());
	}

	/**
	 * Get the context from the given url
	 *
	 * @param urlString - string containing the url
	 * @return the context
	 */
	private Object getContextFromURL(String urlString) {
		try {
			return getContextFromURL(new URL(urlString));
		} catch (MalformedURLException e) {
			throw new ContextRetrievalException(String.format("Was not able to convert %s to URL.", urlString), e, urlString);
		}
	}


	/**
	 * Extract the context urls from the link header. CORE_CONTEXT will be automatically added.
	 *
	 * @param headerString - content of the link header
	 * @return list of context urls, will either be only the core context or the core-context + the header context
	 */
	public List<URL> getContextURLsFromLinkHeader(String headerString) {

		Optional<String> linkedContextString = Optional.empty();

		if (headerString != null && !headerString.isEmpty() && !headerString.isBlank()) {
			linkedContextString = Optional.of(headerString.split(";")[0].replace("<", "").replace(">", ""));
		}

		return linkedContextString
				.map(lCS -> {
					try {
						return new URL(lCS);
					} catch (MalformedURLException e) {
						throw new ContextRetrievalException("Was not able to get context url from the Link-header.", e, lCS);
					}
				})
				.map(url -> List.of(url, coreContextUrl)).orElse(List.of(coreContextUrl));
	}

	// extract the Id from the expanded object
	private String getIdFromJsonLDObject(Map<String, Object> jsonLdObject) {
		Map<String, Object> expandedObject = (Map<String, Object>) JsonLdProcessor.expand(jsonLdObject)
				.stream()
				.findFirst()
				.orElseThrow(() -> new StringExpansionException(String.format("Was not able to get an expanded object for %s.", jsonLdObject)));
		Set<String> expandedKeys = expandedObject.keySet();
		if (expandedKeys.size() != 1) {
			throw new StringExpansionException(String.format("Was not able to correctly expand key. Got multiple keys: %s", expandedKeys));
		}
		return expandedKeys.iterator().next();
	}

	// create a json object for json-ld api to be used for extending the key.
	private String getJsonLdString(String string) {
		return String.format("{\"%s\":\"\"}", string);
	}

}
