package com.linkedin.openhouse.housetables.config;

import com.linkedin.openhouse.housetables.repository.impl.jdbc.StorageLocationJdbcRepository;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.TableStorageLocationJdbcRepository;
import com.linkedin.openhouse.housetables.service.StorageLocationService;
import com.linkedin.openhouse.housetables.service.impl.StorageLocationServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Explicit bean declarations for StorageLocation infrastructure. */
@Configuration
public class StorageLocationConfig {

  @Bean
  public StorageLocationService storageLocationService(
      StorageLocationJdbcRepository storageLocationRepo,
      TableStorageLocationJdbcRepository tableStorageLocationRepo) {
    return new StorageLocationServiceImpl(storageLocationRepo, tableStorageLocationRepo);
  }
}
