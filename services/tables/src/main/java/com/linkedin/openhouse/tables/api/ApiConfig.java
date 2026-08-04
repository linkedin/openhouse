package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import java.util.Collections;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Class that holds all the Beans related to a controller. */
@Configuration
public class ApiConfig {
  @Bean
  public TablesApiHandler tablesApiHandler() {
    return new OpenHouseTablesApiHandler();
  }

  /**
   * Open-source default {@link ColumnDefaultsSource}: supplies none, so read-bridge stays inert.
   */
  @Bean
  @ConditionalOnMissingBean(ColumnDefaultsSource.class)
  public ColumnDefaultsSource columnDefaultsSource() {
    return tableDto -> Collections.emptyMap();
  }

  /**
   * Server-side encoder that stamps the read-bridge {@code config} from {@link
   * ColumnDefaultsSource}.
   */
  @Bean
  @ConditionalOnMissingBean(ReadBridgeConfigResolver.class)
  public ReadBridgeConfigResolver readBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource) {
    return new ReadBridgeConfigResolver(columnDefaultsSource);
  }
}
