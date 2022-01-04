package org.fiware.mintaka.persistence.tenancy;

import io.micronaut.configuration.hibernate.jpa.JpaConfiguration;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.hibernate5.MicronautSessionContext;
import io.micronaut.transaction.jdbc.DelegatingDataSource;
import lombok.RequiredArgsConstructor;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.cfg.AvailableSettings;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Entity manager factory to enable the usage of the multitenant datasource provider
 */
@RequiredArgsConstructor
@Factory
public class MultiTenantEntityManagerFactoryBean {

	private final JpaConfiguration jpaConfiguration;
	private final BeanLocator beanLocator;
	private final MultiTenantDatasourceConnectionProviderImpl connectionProvider;
	private final MultiTenantResolver multiTenantResolver;

	@EachBean(DataSource.class)
	@Replaces(
			bean = StandardServiceRegistry.class
	)
	protected StandardServiceRegistry hibernateStandardServiceRegistry(@Parameter String dataSourceName, DataSource dataSource) {
		if (dataSource instanceof DelegatingDataSource) {
			dataSource = ((DelegatingDataSource) dataSource).getTargetDataSource();
		}
		connectionProvider.registerDefaultDatasource(dataSource);
		Map<String, Object> additionalSettings = new LinkedHashMap<>();
		additionalSettings.put("hibernate.connection.datasource", dataSource);
		additionalSettings.put("hibernate.current_session_context_class", MicronautSessionContext.class.getName());
		additionalSettings.put("hibernate.session_factory_name", dataSourceName);
		additionalSettings.put("hibernate.session_factory_name_is_jndi", false);
		additionalSettings.putIfAbsent(AvailableSettings.MULTI_TENANT, MultiTenancyStrategy.DATABASE);
		additionalSettings.putIfAbsent(AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER,
				connectionProvider);
		additionalSettings.putIfAbsent(AvailableSettings.MULTI_TENANT_IDENTIFIER_RESOLVER, multiTenantResolver);
		JpaConfiguration currentJpaConfiguration = this.beanLocator.findBean(JpaConfiguration.class, Qualifiers.byName(dataSourceName)).orElse(this.jpaConfiguration);
		return currentJpaConfiguration.buildStandardServiceRegistry(additionalSettings);
	}
}

