package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the view commit engine and codec only when the Iceberg view API is present. The string
 * form of {@link ConditionalOnClass} is required so Spring can evaluate it without resolving a type
 * that is absent under Iceberg 1.2.
 *
 * <p>No {@code StorageSelector}: storage selection belongs to the service layer that will call this
 * engine, so the engine only converts an already selected storage type into a {@code FileIO}.
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
      HouseTableMapper houseTableMapper) {
    return new ViewCommitEngineImpl(
        houseTableRepository, fileIOManager, viewMetadataCodec, storageType, houseTableMapper);
  }
}
