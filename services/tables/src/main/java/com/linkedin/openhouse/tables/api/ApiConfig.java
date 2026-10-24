package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.readbridge.ClientCapabilityGate;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultCommitPolicy;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Beans related to tables API controllers. */
@Configuration
public class ApiConfig {
  @Bean
  public TablesApiHandler tablesApiHandler() {
    return new OpenHouseTablesApiHandler();
  }

  @Bean
  public ClientCapabilityGate clientCapabilityGate(CatalogRequestContext requestContext) {
    return new ClientCapabilityGate(requestContext);
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
      ClientCapabilityGate clientCapabilityGate,
      ReadBridgeStripProtection readBridgeStripProtection) {
    return new ColumnDefaultCommitPolicy(
        requestContext, clientCapabilityGate, readBridgeStripProtection);
  }
}
