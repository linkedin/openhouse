package com.linkedin.openhouse.tables.settable;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.SnapshotDiffApplier;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.impl.SettableInternalRepositoryForTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

  @Bean
  public SnapshotDiffApplier snapshotDiffApplier(MeterRegistry meterRegistry) {
    MetricsReporter metricsReporter =
        new MetricsReporter(meterRegistry, "test", Lists.newArrayList());
    return new SnapshotDiffApplier(metricsReporter);
  }
}
