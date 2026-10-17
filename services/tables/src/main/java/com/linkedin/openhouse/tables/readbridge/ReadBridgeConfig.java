package com.linkedin.openhouse.tables.readbridge;

import java.util.Collections;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Read-bridge's default providers, each registered only when the application supplies none. Spring
 * Boot loads auto-configuration after the application's own configuration, so each condition sees
 * every provider registered there, in any configuration class.
 */
@AutoConfiguration
public class ReadBridgeConfig {

  /** A {@link ColumnDefaultsSource} with no defaults. */
  @Bean
  @ConditionalOnMissingBean(ColumnDefaultsSource.class)
  public ColumnDefaultsSource noColumnDefaults() {
    return tableDto -> Collections.emptyMap();
  }
}
