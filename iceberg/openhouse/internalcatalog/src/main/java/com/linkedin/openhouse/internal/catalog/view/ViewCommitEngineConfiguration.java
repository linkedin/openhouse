package com.linkedin.openhouse.internal.catalog.view;

import static com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant.METRICS_PREFIX;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collections;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the view commit engine and codec only when the Iceberg view API is present. The string
 * form of {@link ConditionalOnClass} lets Spring evaluate it without resolving a 1.2-absent type.
 */
@Configuration
@ConditionalOnClass(name = "org.apache.iceberg.view.ViewMetadata")
public class ViewCommitEngineConfiguration {

  @Bean
  public ViewMetadataCodec viewMetadataCodec() {
    return new IcebergViewMetadataCodec();
  }

  @Bean
  public ViewCommitEngine viewCommitEngine(
      HouseTableRepository houseTableRepository,
      FileIOManager fileIOManager,
      ViewMetadataCodec viewMetadataCodec,
      StorageType storageType,
      MeterRegistry meterRegistry) {
    return new ViewCommitEngineImpl(
        houseTableRepository,
        fileIOManager,
        viewMetadataCodec,
        storageType,
        new MetricsReporter(meterRegistry, METRICS_PREFIX, Collections.emptyList()));
  }
}
