package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import com.linkedin.openhouse.cluster.storage.local.LocalStorageClient;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.utils.MetadataUpdateUtils;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

@AllArgsConstructor
@Slf4j
public class OpenHouseInternalTableOperations extends BaseMetastoreTableOperations {

  HouseTableRepository houseTableRepository;

  FileIO fileIO;

  HouseTableMapper houseTableMapper;

  TableIdentifier tableIdentifier;

  MetricsReporter metricsReporter;

  FileIOManager fileIOManager;

  private static final Gson GSON = new Gson();

  private static final Cache<String, Integer> CACHE =
      CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(1000).build();

  @Override
  protected String tableName() {
    return this.tableIdentifier.toString();
  }

  @Override
  public FileIO io() {
    return this.fileIO;
  }

  @Override
  protected void doRefresh() {
    Optional<HouseTable> houseTable = Optional.empty();
    try {
      houseTable =
          houseTableRepository.findById(
              HouseTablePrimaryKey.builder()
                  .databaseId(tableIdentifier.namespace().toString())
                  .tableId(tableIdentifier.name())
                  .build());
    } catch (HouseTableNotFoundException ne) {
      // This path is only expected during table-creation, where refresh() before creation is not
      // avoidable.
      log.debug(
          "Currently there's no entry that exists in House table for the key {}.{}",
          tableIdentifier.namespace().toString(),
          tableIdentifier.name());
      metricsReporter.count(InternalCatalogMetricsConstant.NO_TABLE_WHEN_REFRESH);
    }
    if (!houseTable.isPresent() && currentMetadataLocation() != null) {
      throw new IllegalStateException(
          String.format(
              "Cannot find table %s after refresh, maybe another process deleted it", tableName()));
    }
    refreshMetadata(houseTable.map(HouseTable::getTableLocation).orElse(null));
  }

  /** A wrapper function to encapsulate timer logic for loading metadata. */
  protected void refreshMetadata(final String metadataLoc) {
    long startTime = System.currentTimeMillis();
    boolean needToReload = !Objects.equal(currentMetadataLocation(), metadataLoc);
    Runnable r = () -> super.refreshFromMetadataLocation(metadataLoc);
    try {
      if (needToReload) {
        metricsReporter.executeWithStats(
            r, InternalCatalogMetricsConstant.METADATA_RETRIEVAL_LATENCY, getCatalogMetricTags());
      } else {
        r.run();
      }
      log.info(
          "refreshMetadata from location {} succeeded, took {} ms",
          metadataLoc,
          System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      log.error(
          "refreshMetadata from location {} failed after {} ms",
          metadataLoc,
          System.currentTimeMillis() - startTime,
          e);
      throw e;
    }
  }

  /**
   * File location for the root table metadata location. OpenHouse Service writes the table metadata
   * in the root table directory. The path has a random UUID which allows concurrent writes at the
   * same version to safe write their local uncommitted versions.
   *
   * <p>The directory layout for a given OH table directory looks like below.
   *
   * <p>The root ./table_directory holds all the Table Metadata JSON files and sub-directories /data
   * and /metadata. The metadata sub-directory ./table_directory/metadata holds all the Manifest
   * List Files and Manifest Files. Finally, the data sub-directory ./table_directory/data holds all
   * the Data Files.
   *
   * @param metadata {@link TableMetadata} for which the metadata file location needs to be derived.
   * @param newVersion new table version.
   * @return path to the root table metadata location.
   */
  private static String rootMetadataFileLocation(TableMetadata metadata, int newVersion) {
    String codecName =
        metadata.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    return String.format(
        "%s/%s",
        metadata.location(),
        String.format(
            "%05d-%s%s",
            newVersion, UUID.randomUUID(), TableMetadataParser.getFileExtension(codecName)));
  }

  /**
   * {@link BaseMetastoreTableOperations#commit(TableMetadata, TableMetadata)} operation forces
   * doRefresh() after a doCommit() operation succeeds. This workflow is problematic for
   * isStageCreate=true tables, for which metadata.json is created but not persisted in hts.
   *
   * <p>We override the default behavior and disable forced refresh for newly committed staged
   * tables.
   */
  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    boolean isStageCreate =
        Boolean.parseBoolean(metadata.properties().get(CatalogConstants.IS_STAGE_CREATE_KEY));
    super.commit(base, metadata);
    if (isStageCreate) {
      disableRefresh(); /* disable forced refresh */
    }
  }

  /**
   * An internal helper method to rebuild the {@link TableMetadata} object with a parsed schema.
   *
   * @param newMetadata The current table metadata
   * @param schemaJson The parsed schema object
   * @param reuseMetadata Whether to reuse existing metadata or build from empty
   * @return Table metadata builder with the new schema set as current
   */
  private TableMetadata.Builder rebuildTblMetaWithSchemaBuilder(
      TableMetadata newMetadata, String schemaJson, boolean reuseMetadata) {
    Schema writerSchema = SchemaParser.fromJson(schemaJson);

    if (reuseMetadata) {
      return TableMetadata.buildFrom(newMetadata)
          .setCurrentSchema(writerSchema, writerSchema.highestFieldId());
    } else {
      return TableMetadata.buildFromEmpty()
          .setLocation(newMetadata.location())
          .setCurrentSchema(writerSchema, newMetadata.lastColumnId())
          .addPartitionSpec(
              rebuildPartitionSpec(newMetadata.spec(), newMetadata.schema(), writerSchema))
          .addSortOrder(rebuildSortOrder(newMetadata.sortOrder(), writerSchema))
          .setProperties(newMetadata.properties());
    }
  }

  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {

    // Handle all schema-related processing (client schema, evolved schema, intermediate schemas)
    metadata = processSchemas(base, metadata);

    int version = currentVersion() + 1;
    CommitStatus commitStatus = CommitStatus.FAILURE;

    /* This method adds no fs scheme, and it persists in HTS that way. */
    final String newMetadataLocation = rootMetadataFileLocation(metadata, version);

    HouseTable houseTable = HouseTable.builder().build();
    try {
      // Now that we have metadataLocation we stamp it in metadata property.
      Map<String, String> properties = new HashMap<>(metadata.properties());
      failIfRetryUpdate(properties);
      restoreOverriddenProperties(properties);

      properties.put(
          getCanonicalFieldName("tableVersion"),
          properties.getOrDefault(
              getCanonicalFieldName("tableLocation"), CatalogConstants.INITIAL_VERSION));
      properties.put(getCanonicalFieldName("tableLocation"), newMetadataLocation);

      String currentTsString = String.valueOf(Instant.now(Clock.systemUTC()).toEpochMilli());
      if (isReplicatedTableCreate(properties)) {
        currentTsString =
            metadata.properties().getOrDefault(CatalogConstants.LAST_UPDATED_MS, currentTsString);
      }
      properties.put(getCanonicalFieldName("lastModifiedTime"), currentTsString);
      if (base == null) {
        properties.put(getCanonicalFieldName("creationTime"), currentTsString);
      }

      if (properties.containsKey(CatalogConstants.EVOLVED_SCHEMA_KEY)) {
        properties.remove(CatalogConstants.EVOLVED_SCHEMA_KEY);
      }

      if (properties.containsKey(CatalogConstants.INTERMEDIATE_SCHEMAS_KEY)) {
        properties.remove(CatalogConstants.INTERMEDIATE_SCHEMAS_KEY);
      }

      String serializedSnapshotsToPut = properties.remove(CatalogConstants.SNAPSHOTS_JSON_KEY);
      String serializedSnapshotRefs = properties.remove(CatalogConstants.SNAPSHOTS_REFS_KEY);
      boolean isStageCreate =
          Boolean.parseBoolean(properties.remove(CatalogConstants.IS_STAGE_CREATE_KEY));
      String sortOrderJson = properties.remove(CatalogConstants.SORT_ORDER_KEY);
      logPropertiesMap(properties);

      TableMetadata metadataToCommit = metadata.replaceProperties(properties);

      if (sortOrderJson != null) {
        SortOrder sortOrder = SortOrderParser.fromJson(metadataToCommit.schema(), sortOrderJson);
        metadataToCommit = metadataToCommit.replaceSortOrder(sortOrder);
      }

      if (serializedSnapshotsToPut != null) {
        List<Snapshot> snapshotsToPut =
            SnapshotsUtil.parseSnapshots(fileIO, serializedSnapshotsToPut);
        Map<String, SnapshotRef> snapshotRefs =
            serializedSnapshotRefs == null
                ? new HashMap<>()
                : SnapshotsUtil.parseSnapshotRefs(serializedSnapshotRefs);

        TableMetadata.Builder builder = TableMetadata.buildFrom(metadataToCommit);

        // 1. Identify which snapshots are new vs existing
        Set<Long> existingSnapshotIds =
            metadataToCommit.snapshots().stream()
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());
        Set<Long> newSnapshotIds =
            snapshotsToPut.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

        // 2. Add new snapshots
        snapshotsToPut.stream()
            .filter(s -> !existingSnapshotIds.contains(s.snapshotId()))
            .forEach(builder::addSnapshot);

        // 3. Remove snapshots that are no longer present in the client payload
        List<Long> toRemove =
            existingSnapshotIds.stream()
                .filter(id -> !newSnapshotIds.contains(id))
                .collect(Collectors.toList());
        if (!toRemove.isEmpty()) {
          builder.removeSnapshots(toRemove);
        }

        // 4. Sync Refs: Remove refs not in payload, Set/Update refs from payload
        metadataToCommit.refs().keySet().stream()
            .filter(ref -> !snapshotRefs.containsKey(ref))
            .forEach(builder::removeRef);

        snapshotRefs.forEach(builder::setRef);

        metadataToCommit = builder.build();
      }

      final TableMetadata updatedMtDataRef = metadataToCommit;
      long metadataUpdateStartTime = System.currentTimeMillis();
      try {
        metricsReporter.executeWithStats(
            () ->
                TableMetadataParser.write(
                    updatedMtDataRef, io().newOutputFile(newMetadataLocation)),
            InternalCatalogMetricsConstant.METADATA_UPDATE_LATENCY,
            getCatalogMetricTags());
        log.info(
            "updateMetadata to location {} succeeded, took {} ms",
            newMetadataLocation,
            System.currentTimeMillis() - metadataUpdateStartTime);
      } catch (Exception e) {
        log.error(
            "updateMetadata to location {} failed after {} ms",
            newMetadataLocation,
            System.currentTimeMillis() - metadataUpdateStartTime,
            e);
        throw e;
      }

      houseTable = houseTableMapper.toHouseTable(metadataToCommit, fileIO);
      if (base != null
          && (properties.containsKey(CatalogConstants.OPENHOUSE_TABLEID_KEY)
                  && !properties
                      .get(CatalogConstants.OPENHOUSE_TABLEID_KEY)
                      .equalsIgnoreCase(this.tableIdentifier.name())
              || properties.containsKey(CatalogConstants.OPENHOUSE_DATABASEID_KEY)
                  && !properties
                      .get(CatalogConstants.OPENHOUSE_DATABASEID_KEY)
                      .equalsIgnoreCase(this.tableIdentifier.namespace().toString()))) {
        houseTableRepository.rename(
            this.tableIdentifier.namespace().toString(),
            this.tableIdentifier.name(),
            properties.get(CatalogConstants.OPENHOUSE_DATABASEID_KEY),
            properties.get(CatalogConstants.OPENHOUSE_TABLEID_KEY),
            newMetadataLocation);
      } else if (!isStageCreate) {
        houseTableRepository.save(houseTable);
      } else {
        /**
         * Refresh current metadata for staged tables from newly created metadata file and disable
         * "forced refresh" in {@link OpenHouseInternalTableOperations#commit(TableMetadata,
         * TableMetadata)}
         */
        refreshFromMetadataLocation(newMetadataLocation);
      }
      if (isReplicatedTableCreate(properties)) {
        updateMetadataFieldForTable(metadata, newMetadataLocation);
      }
      commitStatus = CommitStatus.SUCCESS;
    } catch (IOException ioe) {
      commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      // clean up the HTS entry
      try {
        houseTableRepository.delete(houseTable);
      } catch (HouseTableCallerException
          | HouseTableNotFoundException
          | HouseTableConcurrentUpdateException e) {
        log.warn(
            "Failed to delete house table during IOException cleanup for table: {}",
            tableIdentifier,
            e);
      }
      throw new CommitFailedException(ioe);
    } catch (InvalidIcebergSnapshotException | IllegalArgumentException e) {
      throw new BadRequestException(e, e.getMessage());
    } catch (ValidationException e) {
      // Stale snapshot errors are retryable - client should refresh and retry
      if (isStaleSnapshotError(e)) {
        throw new CommitFailedException(e);
      }
      throw new BadRequestException(e, e.getMessage());
    } catch (CommitFailedException e) {
      throw e;
    } catch (HouseTableCallerException
        | HouseTableNotFoundException
        | HouseTableConcurrentUpdateException e) {
      throw new CommitFailedException(e);
    } catch (Throwable persistFailure) {
      // Try to reconnect and determine the commit status for unknown exception
      log.error(
          "Encounter unexpected error while updating metadata.json for table:" + tableIdentifier,
          persistFailure);
      commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      switch (commitStatus) {
        case SUCCESS:
          log.debug("Calling doCommit succeeded");
          break;
        case FAILURE:
          // logging error and exception-throwing co-existence is needed, given the exception
          // handler in
          // org.apache.iceberg.BaseMetastoreCatalog.BaseMetastoreCatalogTableBuilder.create swallow
          // the
          // nested exception information.
          log.error("Exception details:", persistFailure);
          throw new CommitFailedException(
              persistFailure,
              String.format(
                  "Persisting metadata file %s at version %s for table %s failed while persisting to house table",
                  newMetadataLocation, version, GSON.toJson(houseTable)));
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      switch (commitStatus) {
        case FAILURE:
          metricsReporter.count(InternalCatalogMetricsConstant.COMMIT_FAILED_CTR);
          break;
        case UNKNOWN:
          metricsReporter.count(InternalCatalogMetricsConstant.COMMIT_STATE_UNKNOWN);
          break;
        default:
          break; /*should never happen, kept to silence SpotBugs*/
      }
    }
  }

  /**
   * Build a new partition spec with new schema from original pspec. The new pspec has the same
   * partition fields as the original pspec with source ids from the new schema
   *
   * @param originalPspec
   * @param originalSchema
   * @param newSchema
   * @return new partition spec
   */
  static PartitionSpec rebuildPartitionSpec(
      PartitionSpec originalPspec, Schema originalSchema, Schema newSchema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(newSchema);

    for (PartitionField field : originalPspec.fields()) {
      // get field name from original schema using source id of partition field
      // because Pspec appends _bucket and _trunc to field name for bucket and truncate fields
      String fieldName = originalSchema.findField(field.sourceId()).name();
      // Check if the partition field is present in new schema
      if (newSchema.findField(fieldName) == null) {
        throw new IllegalArgumentException(
            "Field " + fieldName + " does not exist in the new schema");
      }
      // build the pspec from transform string representation
      buildPspecFromTransform(builder, field, fieldName);
    }

    return builder.build();
  }

  static void buildPspecFromTransform(
      PartitionSpec.Builder builder, PartitionField field, String fieldName) {
    // Recreate the transform using the string representation
    String transformString = field.transform().toString();

    // Add the field to the new PartitionSpec based on the transform type
    if ("identity".equalsIgnoreCase(transformString)) {
      builder.identity(fieldName);
    } else if (transformString.startsWith("bucket[")) {
      // Extract bucket number from the string (e.g., bucket[16])
      int numBuckets =
          Integer.parseInt(
              transformString.substring(
                  transformString.indexOf('[') + 1, transformString.indexOf(']')));
      builder.bucket(fieldName, numBuckets);
    } else if (transformString.startsWith("truncate[")) {
      // Extract width from the string (e.g., truncate[10])
      int width =
          Integer.parseInt(
              transformString.substring(
                  transformString.indexOf('[') + 1, transformString.indexOf(']')));
      builder.truncate(fieldName, width);
    } else if ("year".equalsIgnoreCase(transformString)) {
      builder.year(fieldName);
    } else if ("month".equalsIgnoreCase(transformString)) {
      builder.month(fieldName);
    } else if ("day".equalsIgnoreCase(transformString)) {
      builder.day(fieldName);
    } else if ("hour".equalsIgnoreCase(transformString)) {
      builder.hour(fieldName);
    } else {
      throw new UnsupportedOperationException("Unsupported transform: " + transformString);
    }
  }

  /**
   * Build a new sort order with new schema from original sort order. The new sort order has the
   * same fields as the original sort order with source ids from the new schema
   *
   * @param originalSortOrder
   * @param newSchema
   * @return new SortOrder
   */
  static SortOrder rebuildSortOrder(SortOrder originalSortOrder, Schema newSchema) {
    SortOrder.Builder builder = SortOrder.builderFor(newSchema);

    for (SortField field : originalSortOrder.fields()) {
      // Find the field name in the original schema based on the sourceId
      String fieldName = originalSortOrder.schema().findField(field.sourceId()).name();
      // Check if the sortorder field is present in new schema
      if (newSchema.findField(fieldName) == null) {
        throw new IllegalArgumentException(
            "Field " + fieldName + " does not exist in the new schema");
      }
      // Create a new SortField with the updated sourceId and original direction and null order
      Term term = Expressions.ref(fieldName);

      // Apply sort direction and null ordering with the updated sourceId
      if (field.direction() == SortDirection.ASC) {
        builder.asc(term, field.nullOrder());
      } else {
        builder.desc(term, field.nullOrder());
      }
    }

    return builder.build();
  }

  /**
   * If this commit comes from Iceberg built-in retry in
   * org.apache.iceberg.PropertiesUpdate#commit() Then throw fatal {@link CommitFailedException} to
   * inform users.
   */
  private void failIfRetryUpdate(Map<String, String> properties) {
    if (properties.containsKey(CatalogConstants.COMMIT_KEY)) {
      String userProvidedTblVer = properties.get(CatalogConstants.COMMIT_KEY);

      // If the commit is ever seen in the past, that indicates this commit is a retry and should
      // abort
      if (CACHE.getIfPresent(userProvidedTblVer) != null) {
        throw new CommitFailedException(
            String.format(
                "The user provided table version [%s] for table [%s] is stale, please consider retry from application",
                userProvidedTblVer, tableIdentifier));
      } else {
        CACHE.put(userProvidedTblVer, 1);
      }

      properties.remove(CatalogConstants.COMMIT_KEY);
    } else {
      // This should never occur except table-creation. However, when table-creation hits
      // concurrency issue
      // it throw AlreadyExistsException and will not trigger retry.
      metricsReporter.count(InternalCatalogMetricsConstant.MISSING_COMMIT_KEY);
    }
  }

  /**
   * Checks if a ValidationException is due to a stale snapshot (sequence number conflict). This
   * happens during concurrent modifications and should be retryable (409), not a bad request (400).
   */
  private boolean isStaleSnapshotError(ValidationException e) {
    String msg = e.getMessage();
    return msg != null
        && msg.contains("Cannot add snapshot with sequence number")
        && msg.contains("older than last sequence number");
  }

  /**
   * Process all schema-related operations including client schema (for new tables), evolved schema
   * (for updates), and intermediate schemas (for replication scenarios). This consolidates all
   * schema handling logic into a single method.
   *
   * @param base The base table metadata (null for new tables)
   * @param metadata The current table metadata
   * @return Updated table metadata with all schema changes applied
   */
  private TableMetadata processSchemas(TableMetadata base, TableMetadata metadata) {
    boolean isNewTable = (base == null);
    String finalSchemaUpdate =
        isNewTable && metadata.properties().get(CatalogConstants.CLIENT_TABLE_SCHEMA) != null
            ? metadata.properties().get(CatalogConstants.CLIENT_TABLE_SCHEMA)
            : metadata.properties().get(CatalogConstants.EVOLVED_SCHEMA_KEY);
    // If there is no schema update, return the original metadata
    if (finalSchemaUpdate == null) {
      return metadata;
    }
    List<String> newSchemas = getIntermediateSchemasFromProps(metadata);
    newSchemas.add(finalSchemaUpdate);
    TableMetadata.Builder updatedMetadataBuilder;

    // Process intermediate schemas first if present
    updatedMetadataBuilder =
        rebuildTblMetaWithSchemaBuilder(metadata, newSchemas.get(0), !isNewTable);

    newSchemas.stream()
        .skip(1) // Skip the initialization schema
        .forEach(
            schemaJson -> {
              try {
                Schema schema = SchemaParser.fromJson(schemaJson);
                updatedMetadataBuilder.setCurrentSchema(schema, schema.highestFieldId());
              } catch (Exception e) {
                log.error(
                    "Failed to process schema: {} for table {}", schemaJson, tableIdentifier, e);
              }
            });

    return updatedMetadataBuilder.build();
  }

  /** Helper function to dump contents for map in debugging mode. */
  private void logPropertiesMap(Map<String, String> map) {
    log.debug(" === Printing the table properties within doCommit method === ");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      log.debug(entry.getKey() + ":" + entry.getValue());
    }
  }

  /**
   * Restores table properties that were temporarily overridden by the repository layer. Properties
   * marked with {@code TRANSIENT_RESTORE_PREFIX} have their original values reinstated, while those
   * marked with {@code TRANSIENT_ADDED_PREFIX} are removed entirely.
   *
   * @param properties mutable map of table properties
   */
  private void restoreOverriddenProperties(Map<String, String> properties) {
    final String restorePrefix = CatalogConstants.TRANSIENT_RESTORE_PREFIX;
    final String addedPrefix = CatalogConstants.TRANSIENT_ADDED_PREFIX;

    Map<String, String> toRestore = new HashMap<>();
    Set<String> toRemove = new java.util.HashSet<>();

    for (String key : new java.util.ArrayList<>(properties.keySet())) {
      if (key.startsWith(restorePrefix)) {
        String propertyKey = key.substring(restorePrefix.length());
        toRestore.put(propertyKey, properties.get(key));
        properties.remove(key);
      } else if (key.startsWith(addedPrefix)) {
        String propertyKey = key.substring(addedPrefix.length());
        toRemove.add(propertyKey);
        properties.remove(key);
      }
    }

    log.info(
        "restoreOverriddenProperties: restoring {} properties, removing {} transient-added properties",
        toRestore.size(),
        toRemove.size());

    toRestore.forEach(
        (propertyKey, originalValue) -> {
          if (originalValue == null) {
            log.info(
                "restoreOverriddenProperties: removing property {} because originalValue is null",
                propertyKey);
            properties.remove(propertyKey);
          } else {
            String originalValueForLog =
                originalValue.length() > 256
                    ? originalValue.substring(0, 256) + "...(truncated)"
                    : originalValue;
            log.info(
                "restoreOverriddenProperties: restoring property {} to {}",
                propertyKey,
                originalValueForLog);
            properties.put(propertyKey, originalValue);
          }
        });

    toRemove.forEach(
        propertyKey -> {
          log.info(
              "restoreOverriddenProperties: removing transient-added property {}", propertyKey);
          properties.remove(propertyKey);
        });
  }

  /**
   * Returns consistent metric tags for catalog metadata operations. This ensures both
   * METADATA_UPDATE_LATENCY and METADATA_RETRIEVAL_LATENCY are always emitted with the same tag
   * dimensions.
   *
   * @return Array of tag key-value pairs for catalog metadata metrics
   */
  private String[] getCatalogMetricTags() {
    return new String[] {};
  }

  /**
   * Updates metadata field for staged tables by extracting updateTimeStamp from metadata.properties
   * and updating the metadata file. Should be used only for replicated table.
   *
   * @param metadata The table metadata containing properties
   */
  private void updateMetadataFieldForTable(TableMetadata metadata, String tableLocation)
      throws IOException {
    String updateTimeStamp = metadata.properties().get(CatalogConstants.LAST_UPDATED_MS);
    if (updateTimeStamp != null) {
      Storage storage = fileIOManager.getStorage(fileIO);
      // Support only for HDFS Storage and local storage clients
      if (storage != null
          && (storage.getClient() instanceof HdfsStorageClient
              || storage.getClient() instanceof LocalStorageClient)) {
        StorageClient<?> client = storage.getClient();
        FileSystem fs = (FileSystem) client.getNativeClient();
        if (tableLocation != null) {
          MetadataUpdateUtils.updateMetadataField(
              fs, tableLocation, CatalogConstants.LAST_UPDATED_MS, Long.valueOf(updateTimeStamp));
        }
      }
    }
  }

  /**
   * Check if the properties have field values indicating a replicated table create request
   *
   * @param properties
   * @return
   */
  private boolean isReplicatedTableCreate(Map<String, String> properties) {
    return Boolean.parseBoolean(
            properties.getOrDefault(CatalogConstants.OPENHOUSE_IS_TABLE_REPLICATED_KEY, "false"))
        && properties
            .getOrDefault(
                CatalogConstants.OPENHOUSE_TABLE_VERSION, CatalogConstants.INITIAL_VERSION)
            .equals(CatalogConstants.INITIAL_VERSION);
  }

  private List<String> getIntermediateSchemasFromProps(TableMetadata metadata) {
    String serializedNewIntermediateSchemas =
        metadata.properties().get(CatalogConstants.INTERMEDIATE_SCHEMAS_KEY);
    if (serializedNewIntermediateSchemas == null) {
      return new ArrayList<>();
    }
    return new GsonBuilder()
        .create()
        .fromJson(serializedNewIntermediateSchemas, new TypeToken<List<String>>() {}.getType());
  }
}
