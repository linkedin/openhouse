package com.linkedin.openhouse.tables.repository.impl;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.*;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static com.linkedin.openhouse.tables.repository.impl.InternalRepositoryUtils.*;

import com.google.common.annotations.VisibleForTesting;
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
import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.SnapshotsUtil;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
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
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
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

  @Autowired private PoliciesSpecMapper policiesMapper;

  @Autowired private TablePolicyUpdater tablePolicyUpdater;

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
      String tableLocation =
          storageSelector
              .selectStorage(tableDto.getDatabaseId(), tableDto.getTableId())
              .allocateTableLocation(
                  tableDto.getDatabaseId(),
                  tableDto.getTableId(),
                  tableDto.getTableUUID(),
                  tableDto.getTableCreator(),
                  tableProps);
      SortOrder sortOrder = getIcebergSortOrder(tableDto, writeSchema);
      table =
          createTable(
              tableIdentifier, writeSchema, partitionSpec, tableLocation, tableProps, sortOrder);
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
          tablePolicyUpdater.updatePoliciesIfNeeded(updateProperties, tableDto, table.properties());
      boolean sortOrderUpdated =
          doUpdateSortOrderIfNeeded(updateProperties, tableDto, table, writeSchema);
      // TODO remove tableTypeAdded after all existing tables have been back-filled to have a
      // tableType
      boolean tableTypeAdded = checkIfTableTypeAdded(updateProperties, table.properties());
      updateProperties.set(COMMIT_KEY, tableDto.getTableVersion()).commit();
      // this relies on forked iceberg-core to use this property for building the base transaction
      // retryer
      // default iceberg behavior will use the hdfs base metadata to derive the base transaction
      // retryer
      String desiredCommitRetries =
          tableDto.getTableProperties() != null
                  && tableDto.getTableProperties().containsKey(TableProperties.COMMIT_NUM_RETRIES)
              ? tableDto.getTableProperties().get(TableProperties.COMMIT_NUM_RETRIES)
              : table.properties().get(TableProperties.COMMIT_NUM_RETRIES);
      overrideProperty(
          updateProperties, desiredCommitRetries, TableProperties.COMMIT_NUM_RETRIES, "0");

      // No new metadata.json shall be generated if nothing changed.
      if (schemaUpdated
          || propsUpdated
          || snapshotsUpdated
          || policiesUpdated
          || sortOrderUpdated
          || tableTypeAdded) {
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

  protected Table createTable(
      TableIdentifier tableIdentifier,
      Schema writeSchema,
      PartitionSpec partitionSpec,
      String tableLocation,
      Map<String, String> tableProps,
      SortOrder sortOrder) {
    return catalog
        .buildTable(tableIdentifier, writeSchema)
        .withPartitionSpec(partitionSpec)
        .withLocation(tableLocation)
        .withProperties(tableProps)
        .withSortOrder(sortOrder)
        .create();
  }

  private boolean doUpdateSortOrderIfNeeded(
      UpdateProperties updateProperties,
      TableDto providedTableDto,
      Table existingTable,
      Schema writeSchema) {
    SortOrder sortOrder = getIcebergSortOrder(providedTableDto, writeSchema);
    if (sortOrder.equals(existingTable.sortOrder())) {
      return false;
    }
    updateProperties.set(SORT_ORDER_KEY, SortOrderParser.toJson(sortOrder));
    return true;
  }

  private SortOrder getIcebergSortOrder(TableDto tableDto, Schema writeSchema) {
    return tableDto.getSortOrder() == null
        ? SortOrder.unsorted()
        : SortOrderParser.fromJson(writeSchema, tableDto.getSortOrder());
  }

  private boolean skipEligibilityCheck(
      Map<String, String> existingTableProps, Map<String, String> newTableProps) {
    // If on the same cluster, table update is primary -> primary and must check all keys, so don't
    // skip validation
    if (existingTableProps
        .get(getCanonicalFieldName(CLUSTER_ID))
        .equals(newTableProps.get(getCanonicalFieldName(CLUSTER_ID)))) {
      return false;
    }

    // If the table is explicitly marked as replicated, skip eligibility check
    if (Boolean.parseBoolean(
        existingTableProps.getOrDefault(OPENHOUSE_IS_TABLE_REPLICATED_KEY, "false"))) {
      return true;
    }

    // For backwards compatibility check table types for a primary -> replica update
    TableType existingTableType =
        TableType.valueOf(existingTableProps.get(getCanonicalFieldName(TABLE_TYPE_KEY)));
    TableType newTableType =
        TableType.valueOf(newTableProps.get(getCanonicalFieldName(TABLE_TYPE_KEY)));

    // Legacy check to skip eligibility check for a primary -> replica update
    return existingTableType == TableType.REPLICA_TABLE && newTableType == TableType.PRIMARY_TABLE;
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
  @VisibleForTesting
  Map<String, String> computePropsForTableCreation(TableDto tableDto) {
    // Populate non-preserved keys, mainly user defined properties.
    Map<String, String> propertiesMap =
        tableDto.getTableProperties().entrySet().stream()
            .filter(entry -> preservedKeyChecker.allowKeyInCreation(entry.getKey(), tableDto))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Only set cluster default for DEFAULT_FILE_FORMAT if user hasn't provided a value
    // (which means either they didn't specify it, or the feature toggle filtered it out)
    if (!propertiesMap.containsKey(TableProperties.DEFAULT_FILE_FORMAT)) {
      propertiesMap.put(
          TableProperties.DEFAULT_FILE_FORMAT,
          clusterProperties.getClusterIcebergWriteFormatDefault());
    }

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
    if (!CollectionUtils.isEmpty(tableDto.getNewIntermediateSchemas())) {
      propertiesMap.put(
          INTERMEDIATE_SCHEMAS_KEY,
          new GsonBuilder().create().toJson(tableDto.getNewIntermediateSchemas()));
    }
    if (tableDto.getTableType() != null) {
      propertiesMap.put(getCanonicalFieldName(TABLE_TYPE_KEY), tableDto.getTableType().toString());
    }

    // add a default property to indicate replicationState for table.
    // Required in addition to tableType to indicate if primary table is replicated or not
    propertiesMap.put(
        OPENHOUSE_IS_TABLE_REPLICATED_KEY,
        tableDto.getTableProperties() != null
            ? tableDto.getTableProperties().getOrDefault(OPENHOUSE_IS_TABLE_REPLICATED_KEY, "false")
            : "false");

    if (tableDto.isStageCreate()) {
      meterRegistry.counter(MetricsConstant.REPO_TABLE_CREATED_CTR_STAGED).increment();
      propertiesMap.put(IS_STAGE_CREATE_KEY, String.valueOf(tableDto.isStageCreate()));
    }

    propertiesMap.put(
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
        Boolean.toString(
            clusterProperties.isClusterIcebergWriteMetadataDeleteAfterCommitEnabled()));
    if (!propertiesMap.containsKey(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX)) {
      propertiesMap.put(
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
          Integer.toString(clusterProperties.getClusterIcebergWriteMetadataPreviousVersionsMax()));
      log.info(
          "Setting the table property: {} to default value: {}.",
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
          Integer.toString(clusterProperties.getClusterIcebergWriteMetadataPreviousVersionsMax()));
    } else {
      log.info(
          "Using the value: {} for table property: {}.",
          propertiesMap.get(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX),
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX);
    }
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
        doSetNewIntermediateSchemasIfNeeded(updateProperties, tableDto);
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
   * Sets intermediate schemas in table properties if they are provided in the TableDto.
   * Intermediate schemas are used during replication when multiple schema updates occur in a single
   * commit, allowing the full schema evolution history to be preserved.
   *
   * @param updateProperties The properties to update
   * @param tableDto The table DTO containing potential intermediate schemas
   */
  private void doSetNewIntermediateSchemasIfNeeded(
      UpdateProperties updateProperties, TableDto tableDto) {
    if (CollectionUtils.isNotEmpty(tableDto.getNewIntermediateSchemas())) {
      updateProperties.set(
          INTERMEDIATE_SCHEMAS_KEY,
          new GsonBuilder().create().toJson(tableDto.getNewIntermediateSchemas()));
      log.info(
          "Setting {} intermediate schemas for table {}.{}",
          tableDto.getNewIntermediateSchemas().size(),
          tableDto.getDatabaseId(),
          tableDto.getTableId());
    }
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

  private void overrideProperty(
      UpdateProperties updateProperties,
      String desiredFinalValue,
      String propertyKey,
      String overrideValue) {
    if (desiredFinalValue != null) {
      String desiredFinalValueForLog =
          desiredFinalValue.length() > 256
              ? desiredFinalValue.substring(0, 256) + "...(truncated)"
              : desiredFinalValue;
      log.info(
          "overrideProperty: stashing desiredFinalValue for {} into {}{} (desiredFinalValue={}), then overriding {} -> {}",
          propertyKey,
          CatalogConstants.TRANSIENT_RESTORE_PREFIX,
          propertyKey,
          desiredFinalValueForLog,
          propertyKey,
          overrideValue);
      updateProperties.set(
          CatalogConstants.TRANSIENT_RESTORE_PREFIX + propertyKey, desiredFinalValue);
    } else {
      log.info(
          "overrideProperty: desiredFinalValue is null for {}; setting {}{} as transient-added marker, then overriding {} -> {}",
          propertyKey,
          CatalogConstants.TRANSIENT_ADDED_PREFIX,
          propertyKey,
          propertyKey,
          overrideValue);
      updateProperties.set(CatalogConstants.TRANSIENT_ADDED_PREFIX + propertyKey, "");
    }
    updateProperties.set(propertyKey, overrideValue).commit();
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

  @Timed(metricKey = MetricsConstant.REPO_TABLES_SEARCH_BY_DATABASE_PAGINATED_TIME)
  @Override
  public Page<TableDto> searchTables(String databaseId, Pageable pageable) {
    if (!(catalog instanceof OpenHouseInternalCatalog)) {
      throw new UnsupportedOperationException(
          "Does not support paginated search for getting all tables in a database");
    }
    return ((OpenHouseInternalCatalog) catalog)
        .listTables(Namespace.of(databaseId), pageable)
        .map(tablesIdentifier -> mapper.toTableDto(tablesIdentifier));
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_IDS_FIND_ALL_TIME)
  @Override
  public List<TableDtoPrimaryKey> findAllIds() {
    return catalog.listTables(Namespace.empty()).stream()
        .map(key -> mapper.toTableDtoPrimaryKey(key))
        .collect(Collectors.toList());
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_IDS_FIND_ALL_TIME)
  @Override
  public Page<TableDtoPrimaryKey> findAllIds(Pageable pageable) {
    if (!(catalog instanceof OpenHouseInternalCatalog)) {
      throw new UnsupportedOperationException(
          "Does not support paginated search for getting all databases");
    }
    return ((OpenHouseInternalCatalog) catalog)
        .listTables(Namespace.empty(), pageable)
        .map(key -> mapper.toTableDtoPrimaryKey(key));
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

  @Override
  public Iterable<TableDto> findAll(Sort sort) {
    throw getUnsupportedException();
  }

  @Override
  public Page<TableDto> findAll(Pageable pageable) {
    throw getUnsupportedException();
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_RENAME_TIME)
  @Override
  public void rename(TableDtoPrimaryKey from, TableDtoPrimaryKey to) {
    catalog.renameTable(
        TableIdentifier.of(from.getDatabaseId(), from.getTableId()),
        TableIdentifier.of(to.getDatabaseId(), to.getTableId()));
  }

  @Timed(metricKey = MetricsConstant.REPO_TABLE_SEARCH_SOFT_DELETED_TIME)
  @Override
  public Page<SoftDeletedTableDto> searchSoftDeletedTables(
      String databaseId, String tableId, Pageable pageable) {
    if (catalog instanceof OpenHouseInternalCatalog) {
      return ((OpenHouseInternalCatalog) catalog)
          .searchSoftDeletedTables(Namespace.of(databaseId), tableId, pageable);
    } else {
      throw new UnsupportedOperationException(
          "SearchSoftDeletedTables is not supported for this catalog type: "
              + catalog.getClass().getSimpleName());
    }
  }

  @Timed(metricKey = MetricsConstant.REPO_PURGE_SOFT_DELETED_TIME)
  @Override
  public void purgeSoftDeletedTableById(TableDtoPrimaryKey tableDtoPrimaryKey, long purgeAfterMs) {
    if (catalog instanceof OpenHouseInternalCatalog) {
      ((OpenHouseInternalCatalog) catalog)
          .purgeSoftDeletedTables(
              tableDtoPrimaryKey.getDatabaseId(), tableDtoPrimaryKey.getTableId(), purgeAfterMs);
    } else {
      throw new UnsupportedOperationException(
          "purgeSoftDeletedTableById is not supported for this catalog type: "
              + catalog.getClass().getName());
    }
  }

  @Timed(metricKey = MetricsConstant.REPO_RESTORE_TABLE_TIME)
  @Override
  public void restoreTable(SoftDeletedTablePrimaryKey softDeletedTablePrimaryKey) {
    if (catalog instanceof OpenHouseInternalCatalog) {
      ((OpenHouseInternalCatalog) catalog)
          .restoreTable(
              softDeletedTablePrimaryKey.getDatabaseId(),
              softDeletedTablePrimaryKey.getTableId(),
              softDeletedTablePrimaryKey.getDeletedAtMs());
    } else {
      throw new UnsupportedOperationException(
          "restoreTable is not supported for this catalog type: " + catalog.getClass().getName());
    }
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
