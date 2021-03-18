package org.fiware.mintaka.context;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.loader.DocumentLoader;
import com.apicatalog.jsonld.loader.DocumentLoaderOptions;
import lombok.RequiredArgsConstructor;

import javax.inject.Singleton;
import java.net.URI;

@Singleton
@RequiredArgsConstructor
public class CacheableDocumentLoader implements DocumentLoader {

	private final LdContextCache ldContextCache;

	@Override
	public Document loadDocument(URI url, DocumentLoaderOptions options) throws JsonLdError {
		return ldContextCache.getContextDocument(url);
	}
}
