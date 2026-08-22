package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultsSource;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@ComponentScan(
    basePackages = {
      "com.linkedin.openhouse.tables.api.validator",
      "com.linkedin.openhouse.tables.authorization",
      "com.linkedin.openhouse.tables.dto.mapper",
      "com.linkedin.openhouse.tables.controller",
      "com.linkedin.openhouse.tables.config",
      "com.linkedin.openhouse.cluster.configs",
      "com.linkedin.openhouse.cluster.storage",
      "com.linkedin.openhouse.tables.services",
      "com.linkedin.openhouse.tables.utils",
      "com.linkedin.openhouse.common.audit",
      "com.linkedin.openhouse.common.exception",
      "com.linkedin.openhouse.tables.audit"
    },
    basePackageClasses = {MockTablesApiHandler.class},
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = "com.linkedin.openhouse.tables.e2e.h2.*"))
@EntityScan(
    basePackages = {
      "com.linkedin.openhouse.tables.model",
      "com.linkedin.openhouse.internal.catalog.model"
    })
@EnableAutoConfiguration(
    exclude = {SecurityAutoConfiguration.class, ManagementWebSecurityAutoConfiguration.class})
public class MockTablesApplication {
  public static void main(String[] args) {
    SpringApplication.run(MockTablesApplication.class, args);
  }

  @MockBean OpenHouseInternalRepository openHouseInternalRepository;

  @MockBean OpenHouseInternalCatalog openHouseInternalCatalog;

  @MockBean HouseTableRepository houseTableRepository;

  /**
   * Mock tests scan {@code tables.api.validator}, not {@code tables.api}, so {@code ApiConfig} is
   * not loaded. Wire a no-op strip guard rather than component-scanning {@code tables.readbridge}.
   */
  @Bean
  public ReadBridgeStripProtection readBridgeStripProtection() {
    TableFeatureToggle unused =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            return false;
          }
        };
    return new ReadBridgeStripProtection(
        new ReadBridgeConfigResolver(ColumnDefaultsSource.NONE, unused));
  }
}
