package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.FEATURE_TOGGLE_STOP_CREATE;
import static com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant.METRICS_PREFIX;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryException;
import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Iceberg Catalog Implementation for OpenHouse User Table persisted as Iceberg tables. Built on-top
 * of HouseTableService where the Iceberg table root pointer is persisted. A custom implementation
 * can be built on top of this by extending this class and making that bean the primary.
 */
@Slf4j
@Component
public class OpenHouseInternalCatalog extends BaseMetastoreCatalog {

  @Autowired HouseTableRepository houseTableRepository;

  @Autowired FileIOManager fileIOManager;

  @Autowired StorageManager storageManager;

  @Autowired SnapshotInspector snapshotInspector;

  @Autowired HouseTableMapper houseTableMapper;

  @Autowired MeterRegistry meterRegistry;

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new OpenHouseInternalTableOperations(
        houseTableRepository,
        fileIOManager.getFileIO(storageManager.getDefaultStorage().getType()),
        snapshotInspector,
        houseTableMapper,
        tableIdentifier,
        new MetricsReporter(this.meterRegistry, METRICS_PREFIX, Lists.newArrayList()));
  }

  /** Overwritten for annotation purpose. */
  @Override
  @IcebergFeatureGate(value = FEATURE_TOGGLE_STOP_CREATE)
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return super.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public String name() {
    return getClass().getSimpleName();
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Location will be provided explicitly");
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }
    if (namespace.isEmpty()) {
      return StreamSupport.stream(houseTableRepository.findAll().spliterator(), false)
          .map(
              houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), houseTable.getTableId()))
          .collect(Collectors.toList());
    }
    return houseTableRepository.findAllByDatabaseId(namespace.toString()).stream()
        .map(houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), houseTable.getTableId()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tableLocation = loadTable(identifier).location();
    log.debug("Dropping table {}, purge:{}", tableLocation, purge);
    try {
      houseTableRepository.deleteById(
          HouseTablePrimaryKey.builder()
              .databaseId(identifier.namespace().toString())
              .tableId(identifier.name())
              .build());
    } catch (HouseTableRepositoryException houseTableRepositoryException) {
      throw new RuntimeException(
          String.format("The table %s cannot be dropped due to the server side error:", identifier),
          houseTableRepositoryException);
    }
    if (purge) {
      // Delete data and metadata files from storage.
      FileIO fileIO = fileIOManager.getFileIO(storageManager.getDefaultStorage().getType());
      if (fileIO instanceof SupportsPrefixOperations) {
        log.debug("Deleting files for table {}", tableLocation);
        ((SupportsPrefixOperations) fileIO).deletePrefix(tableLocation);
      } else {
        log.debug(
            "Failed to delete files for table {}. fileIO does not support prefix operations.",
            tableLocation);
        throw new UnsupportedOperationException(
            "Drop table is supported only with a fileIO instance that SupportsPrefixOperations");
      }
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Rename Tables not implemented yet");
  }
}
