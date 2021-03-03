package org.fiware.mintaka.persistence.tenancy;

import com.zaxxer.hikari.HikariConfig;
import io.micronaut.configuration.jdbc.hikari.HikariUrlDataSource;
import io.micronaut.multitenancy.tenantresolver.TenantResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.jdbc.connections.spi.AbstractDataSourceBasedMultiTenantConnectionProviderImpl;

import javax.inject.Singleton;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Singleton
@RequiredArgsConstructor
public class MultiTenantDatasourceConnectionProviderImpl extends AbstractDataSourceBasedMultiTenantConnectionProviderImpl {

	public static final String DEFAULT_TENANT = "orion";

	private Map<String, DataSource> dataSourceMap = new HashMap<>();

	private final HikariConfig hikariConfig;

	public void registerDefaultDatasource(DataSource dataSource) {
		dataSourceMap.put(DEFAULT_TENANT, dataSource);
	}

	@Override
	protected DataSource selectAnyDataSource() {
		return dataSourceMap.get(DEFAULT_TENANT);
	}

	@Override
	protected DataSource selectDataSource(String tenantIdentifier) {
		if (dataSourceMap.containsKey(tenantIdentifier)) {
			return dataSourceMap.get(tenantIdentifier);
		}
		DataSource tenantDatasource = createDataSource(tenantIdentifier);
		dataSourceMap.put(tenantIdentifier, tenantDatasource);
		return tenantDatasource;
	}

	private DataSource createDataSource(String tenantName) {
		HikariConfig dataSourceConfig = new HikariConfig();
		dataSourceConfig.setConnectionTimeout(hikariConfig.getConnectionTimeout());
		dataSourceConfig.setValidationTimeout(hikariConfig.getValidationTimeout());
		dataSourceConfig.setIdleTimeout(hikariConfig.getIdleTimeout());
		dataSourceConfig.setLeakDetectionThreshold(hikariConfig.getLeakDetectionThreshold());
		dataSourceConfig.setMaxLifetime(hikariConfig.getMaxLifetime());
		dataSourceConfig.setMaximumPoolSize(hikariConfig.getMaximumPoolSize());
		dataSourceConfig.setMinimumIdle(hikariConfig.getMinimumIdle());
		dataSourceConfig.setUsername(hikariConfig.getUsername());
		dataSourceConfig.setPassword(hikariConfig.getPassword());
		dataSourceConfig.setInitializationFailTimeout(hikariConfig.getInitializationFailTimeout());
		dataSourceConfig.setConnectionTestQuery(hikariConfig.getConnectionTestQuery());
		dataSourceConfig.setDriverClassName(hikariConfig.getDriverClassName());
		dataSourceConfig.setAutoCommit(hikariConfig.isAutoCommit());
		dataSourceConfig.setReadOnly(hikariConfig.isReadOnly());
		dataSourceConfig.setIsolateInternalQueries(hikariConfig.isIsolateInternalQueries());
		dataSourceConfig.setRegisterMbeans(hikariConfig.isRegisterMbeans());
		dataSourceConfig.setAllowPoolSuspension(hikariConfig.isAllowPoolSuspension());
		// db name is DEFAULTTENANT_TENANT by definition
		dataSourceConfig.setJdbcUrl(String.format("%s_%s", hikariConfig.getJdbcUrl(), tenantName));
		return new HikariUrlDataSource(dataSourceConfig);
	}
}
