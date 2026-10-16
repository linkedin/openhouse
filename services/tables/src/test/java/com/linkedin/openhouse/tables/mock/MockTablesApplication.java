package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import com.linkedin.openhouse.tables.readbridge.ClientCapabilityGate;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeConfigResolver;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
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
   * The views service graph in {@code tables.services} requires these seams. Mock tests do not scan
   * {@code tables.toggle} (which would add FeatureToggleAspect and the HTS-backed toggle store) or
   * {@code internal.catalog} (which owns the view commit engine, FileIO and metadata codec), so
   * mock them as for the table repository above. The views service reuses the House Table mock
   * above and the real {@code cluster.storage} selector/manager beans. The mocked toggle reports
   * every feature inactive, i.e. views stay disabled.
   */
  @MockBean TableFeatureToggle tableFeatureToggle;

  @MockBean ViewCommitEngine viewCommitEngine;

  @MockBean FileIOManager fileIOManager;

  @MockBean ViewMetadataCodec viewMetadataCodec;

  /**
   * Mock tests scan {@code tables.api.validator}, not {@code tables.api}, so {@code ApiConfig} is
   * not loaded. Wire a no-op strip guard rather than component-scanning {@code tables.readbridge}.
   */
  @Bean
  public ReadBridgeStripProtection readBridgeStripProtection() {
    return new ReadBridgeStripProtection(
        new ReadBridgeConfigResolver(tableDto -> Collections.emptyMap()));
  }

  /**
   * Services require the client gate from {@code ApiConfig}; no mock table requires a capability.
   */
  @Bean
  public ClientCapabilityGate clientCapabilityGate(CatalogRequestContext requestContext) {
    return new ClientCapabilityGate(requestContext);
  }
}
