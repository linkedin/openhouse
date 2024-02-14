package com.linkedin.openhouse.tables.settable;

import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.impl.SettableInternalRepositoryForTest;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class SettableTestConfig {
  @Primary
  @Bean("SettableCatalog")
  public Catalog provideTestCatalog() {
    return new SettableCatalogForTest();
  }

  @Primary
  @Bean("SettableInternalRepo")
  public OpenHouseInternalRepository provideTestInternalRepo() {
    return new SettableInternalRepositoryForTest();
  }
}
