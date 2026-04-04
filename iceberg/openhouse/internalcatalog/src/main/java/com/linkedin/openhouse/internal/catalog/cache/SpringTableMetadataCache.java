package com.linkedin.openhouse.internal.catalog.cache;

import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component
@CacheConfig(
    cacheManager = TableMetadataCaches.CACHE_MANAGER,
    cacheNames = TableMetadataCaches.TABLE_METADATA)
public class SpringTableMetadataCache implements TableMetadataCache {

  @Override
  @Cacheable(key = "#metadataLocation")
  public TableMetadata load(String metadataLocation, Supplier<TableMetadata> metadataLoader) {
    return metadataLoader.get();
  }

  @Override
  @CachePut(key = "#metadataLocation")
  public TableMetadata seed(String metadataLocation, TableMetadata tableMetadata) {
    return tableMetadata;
  }
}
