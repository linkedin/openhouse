package com.linkedin.openhouse.internal.catalog.cache;

import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component
public class SpringTableMetadataCache implements TableMetadataCache {

  @Override
  @Cacheable(
      cacheManager = "internalCatalogCacheManager",
      cacheNames = "tableMetadata",
      key = "#metadataLocation")
  public TableMetadata load(String metadataLocation, Supplier<TableMetadata> metadataLoader) {
    return metadataLoader.get();
  }

  @Override
  @CachePut(
      cacheManager = "internalCatalogCacheManager",
      cacheNames = "tableMetadata",
      key = "#metadataLocation")
  public TableMetadata seed(String metadataLocation, TableMetadata tableMetadata) {
    return tableMetadata;
  }
}
