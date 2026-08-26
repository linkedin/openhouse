package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import org.springframework.beans.factory.ObjectProvider;
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
   * Prefer {@link ObjectProvider} over a {@code @ConditionalOnMissingBean} noop so a deployment
   * {@code @Bean} source cannot collide with an OSS default.
   */
  @Bean
  public ReadBridgeConfigResolver readBridgeConfigResolver(
      ObjectProvider<ColumnDefaultsSource> columnDefaultsSource, TableFeatureToggle featureToggle) {
    return new ReadBridgeConfigResolver(
        columnDefaultsSource.getIfAvailable(() -> ColumnDefaultsSource.NONE), featureToggle);
  }
}
