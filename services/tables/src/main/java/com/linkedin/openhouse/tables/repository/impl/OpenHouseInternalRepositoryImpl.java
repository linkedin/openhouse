package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.*;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.exception.InvalidSchemaEvolutionException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.common.schema.IcebergSchemaHelper;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.internal.catalog.SnapshotsUtil;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PartitionSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.PoliciesSpecMapper;
import com.linkedin.openhouse.tables.dto.mapper.iceberg.TableTypeMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.repository.PreservedKeyChecker;
import com.linkedin.openhouse.tables.repository.SchemaValidator;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Implementation for Repository that uses Iceberg Catalog to store or retrieve Iceberg table TODO:
 * Unit test for this class
 */
@Component
@Slf4j
public class OpenHouseInternalRepositoryImpl implements OpenHouseInternalRepository {

  private static final String TABLE_TYPE_KEY = "tableType";
  private static final String CLUSTER_ID = "clusterId";

  @Autowired Catalog catalog;

  @Autowired TablesMapper mapper;

  @Autowired PoliciesSpecMapper policiesMapper;

  @Autowired PartitionSpecMapper partitionSpecMapper;

  @Autowired TableTypeMapper tableTypeMapper;

  @Autowired FileIOManager fileIOManager;

  @Autowired StorageManager storageManager;

  @Autowired StorageSelector storageSelector;

  @Autowired MeterRegistry meterRegistry;

  @Autowired SchemaValidator schemaValidator;

  @Autowired ClusterProperties clusterProperties;

  @Autowired PreservedKeyChecker preservedKeyChecker;

  @Timed(metricKey = MetricsConstant.REPO_TABLE_SAVE_TIME)
  @Override
  public TableDto save(TableDto tableDto) {
    long startTime = System.currentTimeMillis();
    TableIdentifier tableIdentifier =
        TableIdentifier.of(tableDto.getDatabaseId(), tableDto.getTableId());
    Table table;
    Schema writeSchema = IcebergSchemaHelper.getSchemaFromSchemaJson(tableDto.getSchema());
    boolean existed =
        existsById(
            TableDtoPrimaryKey.builder()
                .tableId(tableDto.getTableId())
                .databaseId(tableDto.getDatabaseId())
                .build());
    if (!existed) {
      creationEligibilityCheck(tableDto);
      PartitionSpec partitionSpec = partitionSpecMapper.toPartitionSpec(tableDto);
      log.info(
          "Creating a new user table: {} with schema: {} and partitionSpec: {}",
          tableIdentifier,
          writeSchema,
          partitionSpec);
      Map<String, String> tableProps = computePropsForTableCreation(tableDto);
      table =
          catalog.createTable(
              tableIdentifier,
              writeSchema,
              partitionSpec,
              storageSelector
                  .selectStorage(tableDto.getDatabaseId(), tableDto.getTableId())
                  .allocateTableLocation(
                      tableDto.getDatabaseId(),
                      tableDto.getTableId(),
                      tableDto.getTableUUID(),
                      tableDto.getTableCreator(),
                      tableProps),
              tableProps);
      meterRegistry.counter(MetricsConstant.REPO_TABLE_CREATED_CTR).increment();
      log.info(
          "create for table {} took {} ms",
          tableIdentifier,
          System.currentTimeMillis() - startTime);
    } else {
      table = catalog.loadTable(tableIdentifier);
      Transaction transaction = table.newTransaction();
      updateEligibilityCheck(table, tableDto);

      UpdateProperties updateProperties = transaction.updateProperties();
      boolean schemaUpdated =
          doUpdateSchemaIfNeeded(writeSchema, table.schema(), tableDto, updateProperties);
      boolean propsUpdated = doUpdateUserPropsIfNeeded(updateProperties, tableDto, table);
      boolean snapshotsUpdated = doUpdateSnapshotsIfNeeded(updateProperties, tableDto);
      boolean policiesUpdated =
          doUpdatePoliciesIfNeeded(updateProperties, tableDto, table.properties());
      // TODO remove tableTypeAdded after all existing tables have been back-filled to have a
      // tableType
      boolean tableTypeAdded = checkIfTableTypeAdded(updateProperties, table.properties());
      updateProperties.set(COMMIT_KEY, tableDto.getTableVersion());
      updateProperties.commit();

      // No new metadata.json shall be generated if nothing changed.
      if (schemaUpdated || propsUpdated || snapshotsUpdated || policiesUpdated || tableTypeAdded) {
        transaction.commitTransaction();
        meterRegistry.counter(MetricsConstant.REPO_TABLE_UPDATED_CTR).increment();
      }
      log.info(
          "update for table {} took {} ms",
          tableIdentifier,
          System.currentTimeMillis() - startTime);
    }
    return convertToTableDto(
        table, fileIOManager, partitionSpecMapper, policiesMapper, tableTypeMapper);
  }

  private boolean skipEligibilityCheck(
      Map<String, String> existingTableProps, Map<String, String> newTableprops) {
    TableType existingTableType =
        TableType.valueOf(existingTableProps.get(getCanonicalFieldName(TABLE_TYPE_KEY)));
    TableType newTableType =
        TableType.valueOf(newTableprops.get(getCanonicalFieldName(TABLE_TYPE_KEY)));
    return existingTableType == TableType.REPLICA_TABLE
        && newTableType == TableType.PRIMARY_TABLE
        && !existingTableProps
            .get(getCanonicalFieldName(CLUSTER_ID))
            .equals(newTableprops.get(getCanonicalFieldName(CLUSTER_ID)));
  }

  /**
   * Check the eligibility of table creation. Throw exceptions when invalidate behaviors detected
   * for {@link com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler} to deal
   * with.
   */
  protected void creationEligibilityCheck(TableDto tableDto) {
    versionCheck(null, tableDto);
  }

  /**
   * Check the eligibility of table updates. Throw exceptions when invalidate behaviors detected for
   * {@link com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler} to deal with
   */
  protected void updateEligibilityCheck(Table existingTable, TableDto tableDto) {
    if (!skipEligibilityCheck(existingTable.properties(), tableDto.getTableProperties())) {
      // eligibility check is relaxed for request from replication flow since preserved properties
      // & tableType will differ in tableDto and existing table.
      PartitionSpec partitionSpec = partitionSpecMapper.toPartitionSpec(tableDto);
      versionCheck(existingTable, tableDto);
      checkIfPreservedTblPropsModified(tableDto, existingTable);
      checkIfTableTypeModified(tableDto, existingTable);
      checkPartitionSpecEvolution(partitionSpec, existingTable.spec());
    }
  }

  /**
   * Ensure existing table's tableLocation (path to metadata.json) matches user provided baseVersion
   * (path to metadata.json of the table where the updates are based upon)
   */
  void versionCheck(Table existingTable, TableDto mergedTableDto) {
    String baseTableVersion = mergedTableDto.getTableVersion();

    if (existingTable != null) {
      String head = existingTable.properties().get(getCanonicalFieldName("tableLocation"));
      if (!getSchemeLessPath(baseTableVersion).equals(getSchemeLessPath(head))) {
        throw new CommitFailedException(
            String.format(
                "Conflict detected for databaseId: %s, tableId: %s, expected version: %s actual version %s: %s",
                mergedTableDto.getDatabaseId(),
                mergedTableDto.getTableId(),
                head,
                baseTableVersion,
                "The requested user table has been modified/created by other processes."));
      }
    } else {
      if (!ValidatorConstants.INITIAL_TABLE_VERSION.equals(baseTableVersion)) {
        throw new RequestValidationFailureException(
            String.format(
                "Request body contains incorrect version :%s, when request table doesn't exist and requested to be created. "
                    + "This might occur when updating a table that has been deleted already.",
                baseTableVersion));
      }
    }
  }

  /**
   * Validate that user is not attempting to add, alter or drop table properties under openhouse
   * namespace.
   *
   * @param tableDto Container of provided table metadata
   * @param table Container of existing table metadata
   */
  private void checkIfPreservedTblPropsModified(TableDto tableDto, Table table) {
    Map<String, String> existingTableProps = table == null ? Maps.newHashMap() : table.properties();
    Map<String, String> providedTableProps =
        tableDto.getTableProperties() == null ? Maps.newHashMap() : tableDto.getTableProperties();

    Map<String, String> extractedExistingProps =
        extractPreservedProps(existingTableProps, tableDto, preservedKeyChecker);
    Map<String, String> extractedProvidedProps =
        extractPreservedProps(providedTableProps, tableDto, preservedKeyChecker);
    if (!extractedExistingProps.equals(extractedProvidedProps)) {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.ALTER_RESERVED_TBLPROPS,
          String.format(
              "Bad tblproperties provided: Can't add, alter or drop table properties due to the restriction: [%s], "
                  + "diff in existing & provided table properties: %s",
              preservedKeyChecker.describePreservedSpace(),
              (new GsonBuilder().setPrettyPrinting().create())
                  .toJson(Maps.difference(extractedExistingProps, extractedProvidedProps))));
    }
  }

  private void checkIfTableTypeModified(TableDto tableDto, Table table) {
    Map<String, String> existingTableProps = table == null ? Maps.newHashMap() : table.properties();
    if (existingTableProps.containsKey(getCanonicalFieldName(TABLE_TYPE_KEY))
        && !existingTableProps
            .get(getCanonicalFieldName(TABLE_TYPE_KEY))
            .equals(tableDto.getTableType().toString())) {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.ALTER_TABLE_TYPE,
          String.format(
              "Bad tableType provided: Can't update tableType to [%s], "
                  + "existing table is of type [%s]",
              tableDto.getTableType(),
              existingTableProps.get(getCanonicalFieldName(TABLE_TYPE_KEY))));
    }
  }

  /**
   * computePropForNewTbl takes a tableDTO and returns a Map<String, String> representing {@link
   * Table} properties. policies from tableDTO is massaged into the properties map so that it can be
   * stored as part of properties in metadata.json
   *
   * @param tableDto
   * @return
   */
  private Map<String, String> computePropsForTableCreation(TableDto tableDto) {
    // Populate non-preserved keys, mainly user defined properties.
    Map<String, String> propertiesMap =
        tableDto.getTableProperties().entrySet().stream()
            .filter(entry -> preservedKeyChecker.allowKeyInCreation(entry.getKey(), tableDto))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Populate server reserved properties
    Map<String, String> dtoMap = tableDto.convertToMap();
    for (String htsFieldName : HTS_FIELD_NAMES) {
      if (dtoMap.get(htsFieldName) != null) {
        propertiesMap.put(getCanonicalFieldName(htsFieldName), dtoMap.get(htsFieldName));
      }
    }

    // Populate policies
    String policiesString = policiesMapper.toPoliciesJsonString(tableDto);
    propertiesMap.put(InternalRepositoryUtils.POLICIES_KEY, policiesString);

    if (!CollectionUtils.isEmpty(tableDto.getJsonSnapshots())) {
      meterRegistry.counter(MetricsConstant.REPO_TABLE_CREATED_WITH_DATA_CTR).increment();
      propertiesMap.put(
          SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializeList(tableDto.getJsonSnapshots()));
      if (!MapUtils.isEmpty(tableDto.getSnapshotRefs())) {
        propertiesMap.put(
            SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(tableDto.getSnapshotRefs()));
      }
    }
    if (tableDto.getTableType() != null) {
      propertiesMap.put(getCanonicalFieldName(TABLE_TYPE_KEY), tableDto.getTableType().toString());
    }

    if (tableDto.isStageCreate()) {
      meterRegistry.counter(MetricsConstant.REPO_TABLE_CREATED_CTR_STAGED).increment();
      propertiesMap.put(IS_STAGE_CREATE_KEY, String.valueOf(tableDto.isStageCreate()));
    }

    propertiesMap.put(
        TableProperties.DEFAULT_FILE_FORMAT,
        clusterProperties.getClusterIcebergWriteFormatDefault());
    propertiesMap.put(
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
        Boolean.toString(
            clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled()));
    propertiesMap.put(
        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
        Integer.toString(clusterProperties.getClusterIcebergWriteMetadataPreviousVersionsMax()));
    propertiesMap.put(
        TableProperties.FORMAT_VERSION,
        Integer.toString(clusterProperties.getClusterIcebergFormatVersion()));

    return propertiesMap;
  }

  /**
   * Helper method to encapsulate the check for partitionSpec's evolution, mainly for readability of
   * main {@link #save(TableDto)} method.
   */
  private void checkPartitionSpecEvolution(PartitionSpec newSpec, PartitionSpec oldSpec) {
    if (!arePartitionColumnNamesSame(newSpec, oldSpec)) {
      meterRegistry
          .counter(MetricsConstant.REPO_TABLE_UNSUPPORTED_PARTITIONSPEC_EVOLUTION)
          .increment();
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.PARTITION_EVOLUTION,
          "Evolution of table partitioning and clustering columns are not supported, recreate the table with "
              + "new partition spec.");
    }
  }

  /** @return Whether there are any schema-updates actually materialized. */
  private boolean doUpdateSchemaIfNeeded(
      Schema writeSchema,
      Schema tableSchema,
      TableDto tableDto,
      UpdateProperties updateProperties) {
    if (!writeSchema.sameSchema(tableSchema)) {
      try {
        schemaValidator.validateWriteSchema(tableSchema, writeSchema, tableDto.getTableUri());
        updateProperties.set(CatalogConstants.EVOLVED_SCHEMA_KEY, SchemaParser.toJson(writeSchema));
        return true;
      } catch (Exception e) {
        // TODO: Make upstream change to have explicit SchemaEvolutionFailureException
        // Currently(0.12.1) org.apache.iceberg.SchemaUpdate.updateColumn doesn't throw specific
        // exception.
        meterRegistry.counter(MetricsConstant.REPO_TABLE_INVALID_SCHEMA_EVOLUTION).increment();
        throw new InvalidSchemaEvolutionException(
            tableDto.getTableUri(), tableDto.getSchema(), tableSchema.toString(), e);
      }
    } else {
      return false;
    }
  }

  /**
   * @param updateProperties
   * @param providedTableDto
   * @return Whether there are any properties-updates actually materialized.
   */
  private boolean doUpdateUserPropsIfNeeded(
      UpdateProperties updateProperties, TableDto providedTableDto, Table existingTable) {
    // Only check user-defined props.
    Map<String, String> existingTableProps =
        getUserTblProps(existingTable.properties(), preservedKeyChecker, providedTableDto);
    Map<String, String> providedTableProps =
        providedTableDto.getTableProperties() == null
            ? null
            : getUserTblProps(
                providedTableDto.getTableProperties(), preservedKeyChecker, providedTableDto);

    return InternalRepositoryUtils.alterPropIfNeeded(
        updateProperties, existingTableProps, providedTableProps);
  }

  private boolean doUpdateSnapshotsIfNeeded(
      UpdateProperties updateProperties, TableDto providedTableDto) {
    if (CollectionUtils.isNotEmpty(providedTableDto.getJsonSnapshots())) {
      updateProperties.set(
          SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializeList(providedTableDto.getJsonSnapshots()));
      if (MapUtils.isNotEmpty(providedTableDto.getSnapshotRefs())) {
        updateProperties.set(
            SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(providedTableDto.getSnapshotRefs()));
      }
      return true;
    }
    return false;
  }

  /**
   * @param updateProperties
   * @param tableDto
   * @param existingTableProps
   * @return Whether there are any policies-updates actually materialized in properties.
   */
  private boolean doUpdatePoliciesIfNeeded(
      UpdateProperties updateProperties,
      TableDto tableDto,
      Map<String, String> existingTableProps) {
    boolean policiesUpdated;
    String tableDtoPolicyString = policiesMapper.toPoliciesJsonString(tableDto);

    if (!existingTableProps.containsKey(InternalRepositoryUtils.POLICIES_KEY)) {
      updateProperties.set(InternalRepositoryUtils.POLICIES_KEY, tableDtoPolicyString);
      policiesUpdated = true;
    } else {
      String policiesJsonString = existingTableProps.get(InternalRepositoryUtils.POLICIES_KEY);
      policiesUpdated =
          InternalRepositoryUtils.alterPoliciesIfNeeded(
              updateProperties, tableDtoPolicyString, policiesJsonString);
    }

    return policiesUpdated;
  }

  /**
   * check if an existing table has tableType set in table properties, if not the put operation
   * should add an explicit entry in tableProperties for openhouse.tableType = "PRIMARY_TABLE"
   */
  // TODO remove after all existing tables have been back-filled to have a tableType
  private boolean checkIfTableTypeAdded(
      UpdateProperties updateProperties, Map<String, String> existingTableProps) {
    boolean tableTypeAdded;
    if (existingTableProps.containsKey(getCanonicalFieldName(TABLE_TYPE_KEY))) {
      tableTypeAdded = false;
    } else {
      updateProperties.set(
          getCanonicalFieldName(TABLE_TYPE_KEY), TableType.PRIMARY_TABLE.toString());
      tableTypeAdded = true;
    }
    return tableTypeAdded;
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_FIND_TIME)
  @Override
  public Optional<TableDto> findById(TableDtoPrimaryKey tableDtoPrimaryKey) {
    Table table;
    TableIdentifier tableId =
        TableIdentifier.of(tableDtoPrimaryKey.getDatabaseId(), tableDtoPrimaryKey.getTableId());
    try {
      table = catalog.loadTable(tableId);
    } catch (NoSuchTableException exception) {
      log.debug("User table does not exist:  " + tableId + " is required.");
      return Optional.empty();
    }
    return Optional.of(
        convertToTableDto(
            table, fileIOManager, partitionSpecMapper, policiesMapper, tableTypeMapper));
  }

  // FIXME: Likely need a cache layer to avoid expensive tableScan.
  @Timed(metricKey = MetricsConstant.REPO_TABLE_EXISTS_TIME)
  @Override
  public boolean existsById(TableDtoPrimaryKey tableDtoPrimaryKey) {
    return catalog.tableExists(
        TableIdentifier.of(tableDtoPrimaryKey.getDatabaseId(), tableDtoPrimaryKey.getTableId()));
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_DELETE_TIME)
  @Override
  public void deleteById(TableDtoPrimaryKey tableDtoPrimaryKey) {
    catalog.dropTable(
        TableIdentifier.of(tableDtoPrimaryKey.getDatabaseId(), tableDtoPrimaryKey.getTableId()),
        true);
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLES_SEARCH_BY_DATABASE_TIME)
  @Override
  public List<TableDto> searchTables(String databaseId) {
    return catalog.listTables(Namespace.of(databaseId)).stream()
        .map(tablesIdentifier -> mapper.toTableDto(tablesIdentifier))
        .collect(Collectors.toList());
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_IDS_FIND_ALL_TIME)
  @Override
  public List<TableDtoPrimaryKey> findAllIds() {
    return catalog.listTables(Namespace.empty()).stream()
        .map(key -> mapper.toTableDtoPrimaryKey(key))
        .collect(Collectors.toList());
  }

  /* IMPLEMENT AS NEEDED */
  @Timed(metricKey = MetricsConstant.REPO_TABLES_FIND_ALL_TIME)
  @Override
  public Iterable<TableDto> findAll() {
    List<Table> tables =
        catalog.listTables(Namespace.empty()).stream()
            .map(tableIdentifier -> catalog.loadTable(tableIdentifier))
            .collect(Collectors.toList());
    return tables.stream()
        .map(
            table ->
                convertToTableDto(
                    table, fileIOManager, partitionSpecMapper, policiesMapper, tableTypeMapper))
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<TableDto> findAllById(Iterable<TableDtoPrimaryKey> tablePrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public <S extends TableDto> Iterable<S> saveAll(Iterable<S> entities) {
    throw getUnsupportedException();
  }

  @Override
  public long count() {
    throw getUnsupportedException();
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_DELETE_TIME)
  @Override
  public void delete(TableDto entity) {
    /** Temporarily implemented for testing purposes. Need further work before productionization. */
    catalog.dropTable(TableIdentifier.of(entity.getDatabaseId(), entity.getTableId()));
  }

  @Override
  public void deleteAllById(Iterable<? extends TableDtoPrimaryKey> tablePrimaryKeys) {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAll(Iterable<? extends TableDto> entities) {
    throw getUnsupportedException();
  }

  @Override
  public void deleteAll() {
    throw getUnsupportedException();
  }

  private UnsupportedOperationException getUnsupportedException() {
    return new UnsupportedOperationException(
        "Only save, findById, existsById supported for OpenHouseCatalog");
  }

  private Boolean arePartitionColumnNamesSame(PartitionSpec before, PartitionSpec newSpec) {
    return Iterators.elementsEqual(
        before.fields().stream().map(PartitionField::name).iterator(),
        newSpec.fields().stream().map(PartitionField::name).iterator());
  }
}
