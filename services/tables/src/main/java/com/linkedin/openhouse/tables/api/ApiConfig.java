package com.linkedin.openhouse.tables.api;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import org.springframework.beans.factory.ObjectProvider;
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
   * Server-side encoder that stamps the read-bridge {@code config}.
   *
   * <p>{@link ColumnDefaultsSource} is the column-default capability's single extension point, and
   * it is resolved here rather than declared as an overridable default bean. A deployment supplies
   * one; with none present that capability is inert and never consults the feature toggle. Each
   * capability is wired, and rolled out, on its own.
   *
   * <p>Deliberately not a {@code @ConditionalOnMissingBean} default bean. Spring Boot documents
   * that condition as safe only inside auto-configuration, and this is an ordinary
   * {@code @Configuration}: a component-scanned override happens to work, because {@code
   * ConfigurationClassPostProcessor} finishes scanning before it evaluates {@code @Bean}
   * conditions, but a deployment declaring its source with {@code @Bean} in a configuration class
   * parsed after this one would get a competing no-op bean and need {@code @Primary} to avoid a
   * {@code NoUniqueBeanDefinitionException}. With {@link ObjectProvider} no default bean is ever
   * registered, so exactly one bean of the type exists however it was declared.
   */
  @Bean
  public ReadBridgeConfigResolver readBridgeConfigResolver(
      ObjectProvider<ColumnDefaultsSource> columnDefaultsSource, TableFeatureToggle featureToggle) {
    return new ReadBridgeConfigResolver(
        columnDefaultsSource.getIfAvailable(() -> ColumnDefaultsSource.NONE), featureToggle);
  }
}
