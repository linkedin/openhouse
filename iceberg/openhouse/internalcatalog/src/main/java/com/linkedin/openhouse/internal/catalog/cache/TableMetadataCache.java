package com.linkedin.openhouse.internal.catalog.cache;

import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;

public interface TableMetadataCache {

  TableMetadata load(String metadataLocation, Supplier<TableMetadata> metadataLoader);

  TableMetadata seed(String metadataLocation, TableMetadata tableMetadata);
}
