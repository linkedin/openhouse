package com.linkedin.openhouse.tables.readbridge;

import java.util.Collections;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Registers a {@link ColumnDefaultsSource} with no defaults when the application has none. Spring
 * Boot loads auto-configuration after the application's own configuration, so the condition sees
 * every source registered there, in any configuration class.
 */
@AutoConfiguration
public class ReadBridgeAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(ColumnDefaultsSource.class)
  public ColumnDefaultsSource noColumnDefaults() {
    return tableDto -> Collections.emptyMap();
  }
}
