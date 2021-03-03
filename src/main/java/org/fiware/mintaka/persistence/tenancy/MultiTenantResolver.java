package org.fiware.mintaka.persistence.tenancy;

import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.multitenancy.tenantresolver.TenantResolver;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Serializable;

@Singleton
public class MultiTenantResolver implements CurrentTenantIdentifierResolver {
	@Inject
	TenantResolver resolver;

	@Override
	public String resolveCurrentTenantIdentifier() {
		try {
			return (String) resolver.resolveTenantIdentifier();
		} catch (TenantNotFoundException notFoundException) {
			return "orion";
		}
	}

	@Override
	public boolean validateExistingCurrentSessions() {
		return true;
	}
}
