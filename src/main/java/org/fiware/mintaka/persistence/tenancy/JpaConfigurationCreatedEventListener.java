package org.fiware.mintaka.persistence.tenancy;

import io.micronaut.configuration.hibernate.jpa.JpaConfiguration;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import lombok.RequiredArgsConstructor;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.cfg.AvailableSettings;

import javax.inject.Singleton;

@Singleton
@RequiredArgsConstructor
public class JpaConfigurationCreatedEventListener implements BeanCreatedEventListener<JpaConfiguration> {

	private final MultiTenantDatasourceConnectionProviderImpl multiTenantDatasourceConnectionProvider;
	private final MultiTenantResolver multiTenantResolver;

	@Override
	public JpaConfiguration onCreated(BeanCreatedEvent<JpaConfiguration> event) {
		JpaConfiguration jpaConfiguration = event.getBean();
		jpaConfiguration.getProperties().put(AvailableSettings.MULTI_TENANT_IDENTIFIER_RESOLVER, multiTenantResolver);
		jpaConfiguration.getProperties().put(AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER, multiTenantDatasourceConnectionProvider);
		jpaConfiguration.getProperties().put(AvailableSettings.MULTI_TENANT, MultiTenancyStrategy.DATABASE);
		return jpaConfiguration;
	}

}
