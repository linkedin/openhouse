package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant.METRICS_PREFIX;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryException;
import com.linkedin.openhouse.tables.model.TableDto;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
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

  public Page<TableIdentifier> listTables(Namespace namespace, Pageable pageable) {
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }
    if (namespace.isEmpty()) {
      return houseTableRepository
          .findAll(pageable)
          .map(houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), "Unused"));
    }
    return houseTableRepository
        .findAllByDatabaseId(namespace.toString(), pageable)
        .map(houseTable -> TableIdentifier.of(houseTable.getDatabaseId(), houseTable.getTableId()));
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tableLocation = loadTable(identifier).location();
    FileIO fileIO = resolveFileIO(identifier);
    log.debug("Dropping table {}, purge:{}", tableLocation, purge);
    try {
      HouseTablePrimaryKey primaryKey =
          HouseTablePrimaryKey.builder()
              .databaseId(identifier.namespace().toString())
              .tableId(identifier.name())
              .build();
      houseTableRepository.deleteById(primaryKey, purge);
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
    Table fromTable = loadTable(from);
    String tableClusterId = fromTable.properties().get(CatalogConstants.OPENHOUSE_CLUSTERID_KEY);

    // Preserve existing case if databases are the same
    String toDatabaseName =
        from.namespace().toString().equalsIgnoreCase(to.namespace().toString())
            ? from.namespace().toString()
            : to.namespace().toString();

    TableUri tableUri =
        TableUri.builder()
            .clusterId(tableClusterId)
            .databaseId(toDatabaseName)
            .tableId(to.name())
            .build();

    Transaction transaction = fromTable.newTransaction();
    UpdateProperties updateProperties = transaction.updateProperties();
    log.info(
        "Setting preserved table properties {} to {}, {} to {}, and {} to {} for table rename",
        CatalogConstants.OPENHOUSE_TABLEID_KEY,
        to.name(),
        CatalogConstants.OPENHOUSE_DATABASEID_KEY,
        toDatabaseName,
        CatalogConstants.OPENHOUSE_TABLEURI_KEY,
        tableUri.toString());
    updateProperties.set(CatalogConstants.OPENHOUSE_TABLEID_KEY, to.name());
    updateProperties.set(CatalogConstants.OPENHOUSE_DATABASEID_KEY, toDatabaseName);
    updateProperties.set(CatalogConstants.OPENHOUSE_TABLEURI_KEY, tableUri.toString());
    updateProperties.commit();
    transaction.commitTransaction();
  }

  public Page<TableDto> searchSoftDeletedTablesByDatabase(Namespace namespace, Pageable pageable) {
    if (namespace.levels().length > 1) {
      throw new ValidationException(
          "Input namespace has more than one levels " + String.join(".", namespace.levels()));
    }

    List<TableDto> softDeletedTables =
        houseTableRepository
            .findAllSoftDeletedTablesByDatabaseId(
                namespace.toString(), pageable.getPageNumber(), pageable.getPageSize())
            .stream()
            .map(
                houseTable ->
                    TableDto.builder()
                        .tableId(houseTable.getTableId())
                        .databaseId(houseTable.getDatabaseId())
                        .tableLocation(houseTable.getTableLocation())
                        .deletedAtMs(houseTable.getDeletedAtMs())
                        .build())
            .collect(Collectors.toList());

    return new PageImpl<>(softDeletedTables, pageable, softDeletedTables.size());
  }

  public void purgeSoftDeletedTables(String databaseId, String tableId, long purgeAfterMs) {
    log.info(
        "Purging soft deleted tables for databaseId: {}, tableId: {}, purgeAfterMs: {}",
        databaseId,
        tableId,
        purgeAfterMs);

    houseTableRepository.purgeSoftDeletedTables(databaseId, tableId, purgeAfterMs);
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
