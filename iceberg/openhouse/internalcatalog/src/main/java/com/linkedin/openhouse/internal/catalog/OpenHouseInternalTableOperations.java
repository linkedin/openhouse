package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
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
import java.util.Collections;
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
import org.apache.iceberg.SnapshotSummary;
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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Objects;

@AllArgsConstructor
@Slf4j
public class OpenHouseInternalTableOperations extends BaseMetastoreTableOperations {

  HouseTableRepository houseTableRepository;

  FileIO fileIO;

  SnapshotInspector snapshotInspector;

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

  /** An internal helper method to rebuild the {@link TableMetadata} object. */
  private TableMetadata rebuildTblMetaWithSchema(
      TableMetadata newMetadata, String schemaKey, boolean reuseMetadata) {
    Schema writerSchema = SchemaParser.fromJson(newMetadata.properties().get(schemaKey));
    if (reuseMetadata) {
      return TableMetadata.buildFrom(newMetadata)
          .setCurrentSchema(writerSchema, writerSchema.highestFieldId())
          .build();
    } else {
      return TableMetadata.buildFromEmpty()
          .setLocation(newMetadata.location())
          .setCurrentSchema(writerSchema, newMetadata.lastColumnId())
          .addPartitionSpec(
              rebuildPartitionSpec(newMetadata.spec(), newMetadata.schema(), writerSchema))
          .addSortOrder(rebuildSortOrder(newMetadata.sortOrder(), writerSchema))
          .setProperties(newMetadata.properties())
          .build();
    }
  }

  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {

    /**
     * During table creation, the table metadata object that arrives here has the field-ids
     * reassigned from the client supplied schema.This code block creates a new table metadata
     * object using the client supplied schema by preserving its field-ids.
     */
    if (base == null && metadata.properties().get(CatalogConstants.CLIENT_TABLE_SCHEMA) != null) {
      metadata = rebuildTblMetaWithSchema(metadata, CatalogConstants.CLIENT_TABLE_SCHEMA, false);
    } else if (metadata.properties().get(CatalogConstants.EVOLVED_SCHEMA_KEY) != null) {
      metadata = rebuildTblMetaWithSchema(metadata, CatalogConstants.EVOLVED_SCHEMA_KEY, true);
    }

    metadata = applySnapshots(base, metadata);

    int version = currentVersion() + 1;
    CommitStatus commitStatus = CommitStatus.FAILURE;

    /* This method adds no fs scheme, and it persists in HTS that way. */
    final String newMetadataLocation = rootMetadataFileLocation(metadata, version);

    HouseTable houseTable = HouseTable.builder().build();
    try {
      // Now that we have metadataLocation we stamp it in metadata property.
      Map<String, String> properties = new HashMap<>(metadata.properties());
      failIfRetryUpdate(properties);

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
      boolean isStageCreate =
          Boolean.parseBoolean(properties.remove(CatalogConstants.IS_STAGE_CREATE_KEY));
      String sortOrderJson = properties.remove(CatalogConstants.SORT_ORDER_KEY);
      logPropertiesMap(properties);

      TableMetadata updatedMetadata = metadata.replaceProperties(properties);

      if (sortOrderJson != null) {
        SortOrder sortOrder = SortOrderParser.fromJson(updatedMetadata.schema(), sortOrderJson);
        updatedMetadata = updatedMetadata.replaceSortOrder(sortOrder);
      }

      final TableMetadata updatedMtDataRef = updatedMetadata;
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

      houseTable = houseTableMapper.toHouseTable(updatedMetadata, fileIO);
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
    } catch (InvalidIcebergSnapshotException e) {
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

  // ==================== Functional Snapshot Application Pipeline ====================

  /**
   * Immutable state object representing the complete snapshot diff and categorization. All fields
   * are final and collections are unmodifiable.
   */
  @lombok.Value
  @lombok.Builder
  private static class SnapshotState {
    List<Snapshot> providedSnapshots;
    Map<String, SnapshotRef> providedRefs;
    List<Snapshot> existingSnapshots;
    Map<String, SnapshotRef> existingRefs;

    // Categorization
    List<Snapshot> wapSnapshots;
    List<Snapshot> cherryPickedSnapshots;
    List<Snapshot> regularSnapshots;

    // Diff results
    List<Snapshot> newSnapshots;
    List<Snapshot> existingRetainedSnapshots;
    List<Snapshot> deletedSnapshots;

    // Branch updates
    Map<String, SnapshotRef> branchUpdates;

    // Metrics for recording
    int appendedCount;
    int stagedCount;
    int cherryPickedCount;
    int deletedCount;
  }

  /**
   * Applies snapshot updates from metadata properties using a functional pipeline. This method
   * follows principles: immutability, pure functions, and composition.
   *
   * <p>Pipeline stages: 1. Extract snapshots from properties 2. Parse snapshots from JSON 3. Parse
   * references from JSON 4. Compute complete state diff (categorize, identify changes) 5. Validate
   * entire operation 6. Apply state changes 7. Record metrics/properties
   *
   * @param base The base table metadata (may be null for table creation)
   * @param metadata The new metadata with properties containing snapshot updates
   * @return Updated metadata with snapshots applied
   */
  TableMetadata applySnapshots(TableMetadata base, TableMetadata metadata) {
    // Check if snapshots update is requested
    if (!metadata.properties().containsKey(CatalogConstants.SNAPSHOTS_JSON_KEY)) {
      // No snapshot updates requested, return unchanged
      return metadata;
    }

    return Optional.ofNullable(metadata.properties().get(CatalogConstants.SNAPSHOTS_JSON_KEY))
        .map(
            snapshotsJson -> {
              // Stage 1-3: Extract and parse
              SnapshotState.SnapshotStateBuilder stateBuilder = SnapshotState.builder();

              // Extract and parse snapshots (Stage 1-2)
              List<Snapshot> providedSnapshots = parseSnapshotsFromJson(snapshotsJson);
              stateBuilder.providedSnapshots(Collections.unmodifiableList(providedSnapshots));

              // Extract and parse references (Stage 3)
              Map<String, SnapshotRef> providedRefs =
                  Optional.ofNullable(
                          metadata.properties().get(CatalogConstants.SNAPSHOTS_REFS_KEY))
                      .map(this::parseReferencesFromJson)
                      .orElse(Collections.emptyMap());
              stateBuilder.providedRefs(Collections.unmodifiableMap(providedRefs));

              // Get existing state from base
              List<Snapshot> existingSnapshots =
                  Optional.ofNullable(base)
                      .map(TableMetadata::snapshots)
                      .orElse(Collections.emptyList());
              stateBuilder.existingSnapshots(Collections.unmodifiableList(existingSnapshots));

              Map<String, SnapshotRef> existingRefs =
                  Optional.ofNullable(base).map(TableMetadata::refs).orElse(Collections.emptyMap());
              stateBuilder.existingRefs(Collections.unmodifiableMap(existingRefs));

              // Stage 4: Compute complete state diff
              SnapshotState state = computeStateDiff(stateBuilder);

              // Stage 5: Validate entire operation
              validateOperation(state, base);

              // Stage 6: Apply state changes
              TableMetadata updated = applyStateChanges(metadata, state);

              // Stage 7: Record metrics/properties
              return recordMetrics(updated, state);
            })
        .orElse(metadata); // No snapshot updates if key not present
  }

  /** Stage 2: Parse snapshots from JSON string. Pure function - no side effects. */
  private List<Snapshot> parseSnapshotsFromJson(String snapshotsJson) {
    return SnapshotsUtil.parseSnapshots(fileIO, snapshotsJson);
  }

  /** Stage 3: Parse references from JSON string. Pure function - no side effects. */
  private Map<String, SnapshotRef> parseReferencesFromJson(String refsJson) {
    return SnapshotsUtil.parseSnapshotRefs(refsJson);
  }

  /**
   * Stage 4: Compute complete state diff. Pure function that categorizes snapshots and identifies
   * changes.
   */
  private SnapshotState computeStateDiff(SnapshotState.SnapshotStateBuilder builder) {
    SnapshotState partial = builder.build();

    Map<Long, Snapshot> providedById =
        partial.getProvidedSnapshots().stream()
            .collect(Collectors.toMap(Snapshot::snapshotId, s -> s));
    Map<Long, Snapshot> existingById =
        partial.getExistingSnapshots().stream()
            .collect(Collectors.toMap(Snapshot::snapshotId, s -> s));

    // Categorize all snapshots by type
    SnapshotCategories categories =
        categorizeAllSnapshots(partial.getProvidedSnapshots(), existingById);

    // Identify snapshot changes (new, retained, deleted)
    SnapshotChanges changes =
        identifySnapshotChanges(
            partial.getProvidedSnapshots(),
            partial.getExistingSnapshots(),
            providedById,
            existingById);

    // Identify branch updates
    Map<String, SnapshotRef> branchUpdates =
        computeBranchUpdates(partial.getProvidedRefs(), partial.getExistingRefs());

    // Compute metrics
    SnapshotMetrics metrics = computeSnapshotMetrics(categories, changes, existingById);

    // Build complete state
    return builder
        .wapSnapshots(Collections.unmodifiableList(categories.wapSnapshots))
        .cherryPickedSnapshots(Collections.unmodifiableList(categories.cherryPickedSnapshots))
        .regularSnapshots(Collections.unmodifiableList(categories.regularSnapshots))
        .newSnapshots(Collections.unmodifiableList(changes.newSnapshots))
        .existingRetainedSnapshots(Collections.unmodifiableList(changes.existingRetainedSnapshots))
        .deletedSnapshots(Collections.unmodifiableList(changes.deletedSnapshots))
        .branchUpdates(Collections.unmodifiableMap(branchUpdates))
        .appendedCount(metrics.appendedCount)
        .stagedCount(metrics.stagedCount)
        .cherryPickedCount(metrics.cherryPickedCount)
        .deletedCount(metrics.deletedCount)
        .build();
  }

  /** Container for categorized snapshots. */
  @lombok.Value
  private static class SnapshotCategories {
    List<Snapshot> wapSnapshots;
    List<Snapshot> cherryPickedSnapshots;
    List<Snapshot> regularSnapshots;
  }

  /** Categorize all snapshots into WAP, cherry-picked, and regular. */
  private SnapshotCategories categorizeAllSnapshots(
      List<Snapshot> providedSnapshots, Map<Long, Snapshot> existingById) {
    List<Snapshot> wapSnapshots = categorizeWapSnapshots(providedSnapshots);
    List<Snapshot> cherryPickedSnapshots =
        categorizeCherryPickedSnapshots(providedSnapshots, existingById);
    List<Snapshot> regularSnapshots =
        categorizeRegularSnapshots(providedSnapshots, wapSnapshots, cherryPickedSnapshots);

    return new SnapshotCategories(wapSnapshots, cherryPickedSnapshots, regularSnapshots);
  }

  /** Container for snapshot changes. */
  @lombok.Value
  private static class SnapshotChanges {
    List<Snapshot> newSnapshots;
    List<Snapshot> existingRetainedSnapshots;
    List<Snapshot> deletedSnapshots;
  }

  /** Identify which snapshots are new, retained, or deleted. */
  private SnapshotChanges identifySnapshotChanges(
      List<Snapshot> providedSnapshots,
      List<Snapshot> existingSnapshots,
      Map<Long, Snapshot> providedById,
      Map<Long, Snapshot> existingById) {

    List<Snapshot> newSnapshots =
        providedSnapshots.stream()
            .filter(s -> !existingById.containsKey(s.snapshotId()))
            .collect(Collectors.toList());

    List<Snapshot> existingRetainedSnapshots =
        providedSnapshots.stream()
            .filter(s -> existingById.containsKey(s.snapshotId()))
            .collect(Collectors.toList());

    List<Snapshot> deletedSnapshots =
        existingSnapshots.stream()
            .filter(s -> !providedById.containsKey(s.snapshotId()))
            .collect(Collectors.toList());

    return new SnapshotChanges(newSnapshots, existingRetainedSnapshots, deletedSnapshots);
  }

  /** Container for snapshot metrics. */
  @lombok.Value
  private static class SnapshotMetrics {
    int appendedCount;
    int stagedCount;
    int cherryPickedCount;
    int deletedCount;
  }

  /** Compute metrics based on categorized snapshots and changes. */
  private SnapshotMetrics computeSnapshotMetrics(
      SnapshotCategories categories, SnapshotChanges changes, Map<Long, Snapshot> existingById) {

    int appendedCount =
        (int)
            categories.regularSnapshots.stream()
                .filter(s -> !existingById.containsKey(s.snapshotId()))
                .count();
    int stagedCount = categories.wapSnapshots.size();
    int cherryPickedCount = categories.cherryPickedSnapshots.size();
    int deletedCount = changes.deletedSnapshots.size();

    return new SnapshotMetrics(appendedCount, stagedCount, cherryPickedCount, deletedCount);
  }

  /**
   * Categorize WAP (Write-Audit-Publish) snapshots. A snapshot is WAP if it has the WAP ID in its
   * summary.
   */
  private List<Snapshot> categorizeWapSnapshots(List<Snapshot> snapshots) {
    return snapshots.stream()
        .filter(
            s -> s.summary() != null && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP))
        .collect(Collectors.toList());
  }

  /**
   * Categorize cherry-picked snapshots. A snapshot is cherry-picked if it exists in the current
   * metadata but has a different parent than in the provided snapshots (indicating it was moved to
   * a different branch).
   */
  private List<Snapshot> categorizeCherryPickedSnapshots(
      List<Snapshot> providedSnapshots, Map<Long, Snapshot> existingById) {

    return providedSnapshots.stream()
        .filter(
            provided -> {
              Snapshot existing = existingById.get(provided.snapshotId());
              if (existing == null) {
                return false; // New snapshot, not cherry-picked
              }
              // Check if parent changed (indicating cherry-pick to different branch)
              Long providedParent = provided.parentId();
              Long existingParent = existing.parentId();
              return !Objects.equal(providedParent, existingParent);
            })
        .collect(Collectors.toList());
  }

  /**
   * Categorize regular (appended) snapshots. Regular snapshots are those that are not WAP or
   * cherry-picked.
   */
  private List<Snapshot> categorizeRegularSnapshots(
      List<Snapshot> allSnapshots,
      List<Snapshot> wapSnapshots,
      List<Snapshot> cherryPickedSnapshots) {

    Set<Long> wapIds = wapSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Set<Long> cherryPickedIds =
        cherryPickedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    return allSnapshots.stream()
        .filter(s -> !wapIds.contains(s.snapshotId()) && !cherryPickedIds.contains(s.snapshotId()))
        .collect(Collectors.toList());
  }

  /** Compute branch updates by comparing provided and existing refs. */
  private Map<String, SnapshotRef> computeBranchUpdates(
      Map<String, SnapshotRef> providedRefs, Map<String, SnapshotRef> existingRefs) {

    return providedRefs.entrySet().stream()
        .filter(
            entry -> {
              SnapshotRef existing = existingRefs.get(entry.getKey());
              return existing == null || existing.snapshotId() != entry.getValue().snapshotId();
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /** Stage 5: Validate entire operation. Throws exceptions for invalid operations. */
  private void validateOperation(SnapshotState state, TableMetadata base) {
    // Validation 1: Current snapshot not deleted without replacements
    validateCurrentSnapshotNotDeleted(state, base);

    // Validation 2: No ambiguous commits (multiple branches → same snapshot)
    validateNoAmbiguousCommits(state);

    // Validation 3: Deleted snapshots not referenced by branches/tags
    validateDeletedSnapshotsNotReferenced(state);

    // Validation 4: Individual snapshot validation using SnapshotInspector
    validateIndividualSnapshots(state);
  }

  /**
   * Validate that current snapshot is not deleted without replacements. Package-private for
   * testing.
   */
  void validateCurrentSnapshotNotDeleted(SnapshotState state, TableMetadata base) {
    if (base == null || base.currentSnapshot() == null) {
      return; // No current snapshot to validate
    }

    long currentSnapshotId = base.currentSnapshot().snapshotId();
    boolean currentDeleted =
        state.getDeletedSnapshots().stream().anyMatch(s -> s.snapshotId() == currentSnapshotId);

    if (currentDeleted && state.getNewSnapshots().isEmpty()) {
      throw new InvalidIcebergSnapshotException(
          String.format(
              "Cannot delete the current snapshot %s without adding replacement snapshots. "
                  + "Deleted: [%s], New: [%s]",
              currentSnapshotId,
              state.getDeletedSnapshots().stream()
                  .map(s -> Long.toString(s.snapshotId()))
                  .collect(Collectors.joining(", ")),
              state.getNewSnapshots().stream()
                  .map(s -> Long.toString(s.snapshotId()))
                  .collect(Collectors.joining(", "))));
    }
  }

  /**
   * Validate no ambiguous commits (multiple branches pointing to same snapshot in one commit).
   * Package-private for testing.
   */
  void validateNoAmbiguousCommits(SnapshotState state) {
    Map<Long, List<String>> snapshotToBranches =
        state.getBranchUpdates().entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getValue().snapshotId(),
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    snapshotToBranches.forEach(
        (snapshotId, branches) -> {
          if (branches.size() > 1) {
            throw new InvalidIcebergSnapshotException(
                String.format(
                    "Ambiguous commit: snapshot %s is referenced by multiple branches [%s] in a single commit. "
                        + "Each snapshot can only be referenced by one branch per commit.",
                    snapshotId, String.join(", ", branches)));
          }
        });
  }

  /**
   * Validate that deleted snapshots are not referenced by any branches or tags. Package-private for
   * testing.
   */
  void validateDeletedSnapshotsNotReferenced(SnapshotState state) {
    Set<Long> deletedIds =
        state.getDeletedSnapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    Map<Long, List<String>> referencedIdsToRefs =
        state.getProvidedRefs().entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getValue().snapshotId(),
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    Map<Long, List<String>> invalidDeletes =
        deletedIds.stream()
            .filter(referencedIdsToRefs::containsKey)
            .collect(Collectors.toMap(id -> id, referencedIdsToRefs::get));

    if (!invalidDeletes.isEmpty()) {
      String details =
          invalidDeletes.entrySet().stream()
              .map(
                  e ->
                      String.format(
                          "snapshot %s (referenced by: %s)",
                          e.getKey(), String.join(", ", e.getValue())))
              .collect(Collectors.joining("; "));
      throw new InvalidIcebergSnapshotException(
          String.format(
              "Cannot delete snapshots that are still referenced by branches/tags: %s", details));
    }
  }

  /**
   * Validate individual snapshots using existing SnapshotInspector. Package-private for testing.
   */
  void validateIndividualSnapshots(SnapshotState state) {
    state
        .getNewSnapshots()
        .forEach(
            snapshot -> {
              if (snapshotInspector != null) {
                snapshotInspector.validateSnapshot(snapshot);
              }
            });
  }

  /**
   * Stage 6: Apply state changes to create new TableMetadata. Pure function - creates new metadata
   * without mutating existing.
   *
   * <p>This method uses Iceberg's proper APIs: - removeSnapshots() to delete snapshots -
   * addSnapshot() to add new snapshots - setBranchSnapshot() to set branch references
   *
   * <p>The order of operations matters: 1. Start with base metadata (buildFrom copies all existing
   * state) 2. Remove deleted snapshots first (using proper removeSnapshots API) 3. Remove stale
   * branch references 4. Add new snapshots and set branch pointers
   */
  private TableMetadata applyStateChanges(TableMetadata metadata, SnapshotState state) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);

    // Step 1: Remove deleted snapshots using proper Iceberg API
    if (!state.getDeletedSnapshots().isEmpty()) {
      Set<Long> deletedIds =
          state.getDeletedSnapshots().stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());
      builder.removeSnapshots(deletedIds);
    }

    // Step 2: Remove stale branch references (branches that are no longer in provided refs)
    Set<String> providedRefNames = state.getProvidedRefs().keySet();
    metadata.refs().keySet().stream()
        .filter(refName -> !providedRefNames.contains(refName))
        .forEach(builder::removeRef);

    // Step 3: Identify existing snapshots (after deletions)
    Set<Long> existingSnapshotIds =
        metadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Set<Long> deletedIds =
        state.getDeletedSnapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    existingSnapshotIds.removeAll(deletedIds);

    // Step 4: Identify snapshots referenced by branches
    Set<Long> referencedByBranches =
        state.getProvidedRefs().values().stream()
            .map(SnapshotRef::snapshotId)
            .collect(Collectors.toSet());

    // Step 5: Add unreferenced new snapshots (referenced ones are added via setBranchSnapshot)
    state.getProvidedSnapshots().stream()
        .filter(s -> !existingSnapshotIds.contains(s.snapshotId()))
        .filter(s -> !referencedByBranches.contains(s.snapshotId()))
        .forEach(builder::addSnapshot);

    // Step 6: Set branch pointers for all provided refs
    state
        .getProvidedRefs()
        .forEach(
            (branchName, ref) -> {
              Snapshot snapshot =
                  state.getProvidedSnapshots().stream()
                      .filter(s -> s.snapshotId() == ref.snapshotId())
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new InvalidIcebergSnapshotException(
                                  String.format(
                                      "Branch %s references non-existent snapshot %s",
                                      branchName, ref.snapshotId())));

              if (existingSnapshotIds.contains(snapshot.snapshotId())) {
                // Snapshot already exists - just update the branch pointer if needed
                SnapshotRef existingRef = metadata.refs().get(branchName);
                if (existingRef == null || existingRef.snapshotId() != ref.snapshotId()) {
                  builder.setRef(branchName, ref);
                }
              } else {
                // Snapshot is new - setBranchSnapshot will add it and set the branch pointer
                builder.setBranchSnapshot(snapshot, branchName);
              }
            });

    return builder.build();
  }

  /**
   * Stage 7: Record metrics and add properties to metadata. Returns new metadata with updated
   * properties.
   */
  private TableMetadata recordMetrics(TableMetadata metadata, SnapshotState state) {
    Map<String, String> newProperties = new HashMap<>(metadata.properties());

    // Helper to format snapshot IDs as comma-separated string
    java.util.function.Function<List<Snapshot>, String> formatIds =
        snapshots ->
            snapshots.stream()
                .map(s -> Long.toString(s.snapshotId()))
                .collect(Collectors.joining(","));

    // Record categorization metrics as comma-separated snapshot IDs
    if (!state.getRegularSnapshots().isEmpty()) {
      List<Snapshot> newRegularSnapshots =
          state.getRegularSnapshots().stream()
              .filter(s -> state.getNewSnapshots().contains(s))
              .collect(Collectors.toList());
      if (!newRegularSnapshots.isEmpty()) {
        newProperties.put(
            getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
            formatIds.apply(newRegularSnapshots));
      }
    }
    if (!state.getWapSnapshots().isEmpty()) {
      newProperties.put(
          getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
          formatIds.apply(state.getWapSnapshots()));
    }
    if (!state.getCherryPickedSnapshots().isEmpty()) {
      newProperties.put(
          getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
          formatIds.apply(state.getCherryPickedSnapshots()));
    }
    if (!state.getDeletedSnapshots().isEmpty()) {
      newProperties.put(
          getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
          formatIds.apply(state.getDeletedSnapshots()));
    }

    // Remove the transient snapshot keys from properties
    newProperties.remove(CatalogConstants.SNAPSHOTS_JSON_KEY);
    newProperties.remove(CatalogConstants.SNAPSHOTS_REFS_KEY);

    return metadata.replaceProperties(newProperties);
  }

  // ==================== End Functional Snapshot Application Pipeline ====================

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

  /** Helper function to dump contents for map in debugging mode. */
  private void logPropertiesMap(Map<String, String> map) {
    log.debug(" === Printing the table properties within doCommit method === ");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      log.debug(entry.getKey() + ":" + entry.getValue());
    }
  }

  /**
   * Returns consistent metric tags for catalog metadata operations. This ensures both
   * METADATA_UPDATE_LATENCY and METADATA_RETRIEVAL_LATENCY are always emitted with the same tag
   * dimensions.
   *
   * @return Array of tag key-value pairs for catalog metadata metrics
   */
  private String[] getCatalogMetricTags() {
    return new String[] {
      InternalCatalogMetricsConstant.DATABASE_TAG, tableIdentifier.namespace().toString()
    };
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
}
