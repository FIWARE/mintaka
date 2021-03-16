package org.fiware.mintaka.persistence.tenancy;

import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.multitenancy.tenantresolver.TenantResolver;
import lombok.RequiredArgsConstructor;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;

import javax.inject.Singleton;

/**
 * Tenant provider for datasource selection. Returns a default tenant in case none is given.
 */
@Singleton
@RequiredArgsConstructor
public class MultiTenantResolver implements CurrentTenantIdentifierResolver {

	private final TenantResolver resolver;

	@Override
	public String resolveCurrentTenantIdentifier() {
		try {
			return (String) resolver.resolveTenantIdentifier();
		} catch (TenantNotFoundException notFoundException) {
			return MultiTenantDatasourceConnectionProviderImpl.DEFAULT_TENANT;
		}
	}

	@Override
	public boolean validateExistingCurrentSessions() {
		return true;
	}
}
