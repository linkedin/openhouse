package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultClientGate;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultCommitPolicy;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans related to tables API controllers. */
@Configuration
public class ApiConfig {
  @Bean
  public TablesApiHandler tablesApiHandler() {
    return new OpenHouseTablesApiHandler();
  }

  /**
   * An unset {@code cluster.read-bridge.column-default.minimum-client-version} defaults to empty,
   * which fails every request to a table with {@code required-capability.column-defaults=true} as a
   * server error until a minimum is configured. A value that is not a release fails startup. Tables
   * without that property are unaffected. {@code cluster.read-bridge.column-default.help-url} is
   * the page rejection messages link to; unset, they carry no link.
   */
  @Bean
  public ColumnDefaultClientGate columnDefaultClientGate(
      @Value("${cluster.read-bridge.column-default.minimum-client-version:}") String minimum,
      @Value("${cluster.read-bridge.column-default.help-url:}") String helpUrl,
      CatalogRequestContext requestContext) {
    return new ColumnDefaultClientGate(minimum, helpUrl, requestContext);
  }

  @Bean
  public ReadBridgeConfigResolver readBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource) {
    return new ReadBridgeConfigResolver(columnDefaultsSource);
  }

  @Bean
  public ReadBridgeStripProtection readBridgeStripProtection(
      ReadBridgeConfigResolver readBridgeConfigResolver) {
    return new ReadBridgeStripProtection(readBridgeConfigResolver);
  }

  /** A pre-commit check: the internal catalog runs it on every table commit. */
  @Bean
  public ColumnDefaultCommitPolicy columnDefaultCommitPolicy(
      CatalogRequestContext requestContext,
      ColumnDefaultClientGate columnDefaultClientGate,
      ReadBridgeStripProtection readBridgeStripProtection) {
    return new ColumnDefaultCommitPolicy(
        requestContext, columnDefaultClientGate, readBridgeStripProtection);
  }
}
