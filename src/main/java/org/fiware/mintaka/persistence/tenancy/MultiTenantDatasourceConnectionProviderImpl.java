package org.fiware.mintaka.persistence.tenancy;

import com.zaxxer.hikari.HikariConfig;
import io.micronaut.configuration.jdbc.hikari.HikariUrlDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.jdbc.connections.spi.AbstractDataSourceBasedMultiTenantConnectionProviderImpl;

import javax.inject.Singleton;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Connection provider to offer DataSources based on the requested tenant.
 */
@Slf4j
@Singleton
@RequiredArgsConstructor
public class MultiTenantDatasourceConnectionProviderImpl extends AbstractDataSourceBasedMultiTenantConnectionProviderImpl {

	public static final String DEFAULT_TENANT = "orion";
	// configuration of the default datasource
	private final HikariConfig hikariConfig;
	// map to hold all tenant specific datasources
	private Map<String, DataSource> dataSourceMap = new HashMap<>();

	/**
	 * Add the datasource for the default tenant
	 *
	 * @param dataSource the datasource to use as default
	 */
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
		return createDataSource(tenantIdentifier);
	}

	/**
	 * Create a datasource for the given tenant, based on the default database configuration.
	 */
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

		DataSource dataSource = new HikariUrlDataSource(dataSourceConfig);
		dataSourceMap.put(tenantName, dataSource);
		return dataSource;
	}
}
