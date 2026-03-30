package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.internal.catalog.repository.StorageLocationRepository;
import com.linkedin.openhouse.internal.catalog.repository.StorageLocationRepositoryImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/** Explicit bean declaration for {@link StorageLocationRepository}. */
@Configuration
public class StorageLocationConfig {

  @Bean
  public StorageLocationRepository storageLocationRepository(
      @Qualifier("htsWebClient") WebClient htsWebClient) {
    return new StorageLocationRepositoryImpl(htsWebClient);
  }
}
