package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant.METRICS_PREFIX;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreCatalog;
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

  @Autowired StorageSelector storageSelector;

  @Autowired StorageType storageType;

  @Autowired SnapshotInspector snapshotInspector;

  @Autowired HouseTableMapper houseTableMapper;

  @Autowired MeterRegistry meterRegistry;

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new OpenHouseInternalTableOperations(
        houseTableRepository,
        resolveFileIO(tableIdentifier),
        snapshotInspector,
        houseTableMapper,
        tableIdentifier,
        new MetricsReporter(this.meterRegistry, METRICS_PREFIX, Lists.newArrayList()));
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
    // TODO: Implement SupportsNamespace interface and listNamespaces() method to remove this
    //  branch. This is anti-pattern and only a temporary solution.
    if (namespace.isEmpty()) {
      return StreamSupport.stream(houseTableRepository.findAll().spliterator(), false)
          .map(houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), "Unused"))
          .collect(Collectors.toList());
    }
    return houseTableRepository.findAllByDatabaseId(namespace.toString()).stream()
        .map(houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), houseTable.getTableId()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tableLocation = loadTable(identifier).location();
    FileIO fileIO = resolveFileIO(identifier);
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
    try {
      houseTableRepository
          .findById(
              HouseTablePrimaryKey.builder()
                  .databaseId(from.namespace().toString())
                  .tableId(from.name())
                  .build())
          .ifPresent(
              houseTable -> {
                houseTableRepository.rename(
                    houseTable.getDatabaseId(),
                    houseTable.getTableId(),
                    to.namespace().toString(),
                    to.name());
              });
    } catch (HouseTableRepositoryException e) {
      throw new RuntimeException(
          String.format("The table %s cannot be renamed due to the server side error:", from), e);
    }
  }

  /**
   * Get the file IO for a table. if table exists, return the fileIO for the storageType in hts else
   * return the fileio for storageType returned by storage selector
   *
   * @param tableIdentifier
   * @return fileIO
   */
  protected FileIO resolveFileIO(TableIdentifier tableIdentifier) {
    Optional<HouseTable> houseTable = Optional.empty();
    try {
      houseTable =
          houseTableRepository.findById(
              HouseTablePrimaryKey.builder()
                  .databaseId(tableIdentifier.namespace().toString())
                  .tableId(tableIdentifier.name())
                  .build());
    } catch (HouseTableNotFoundException e) {
      log.info(
          "House table entry not found {}.{}",
          tableIdentifier.namespace().toString(),
          tableIdentifier.name());
    }
    StorageType.Type type =
        houseTable.isPresent()
            ? storageType.fromString(houseTable.get().getStorageType())
            : storageSelector
                .selectStorage(tableIdentifier.namespace().toString(), tableIdentifier.name())
                .getType();

    return fileIOManager.getFileIO(type);
  }
}
