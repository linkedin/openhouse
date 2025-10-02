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
import java.util.ArrayList;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
import org.springframework.data.util.Pair;

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
      String serializedSnapshotsToPut = properties.remove(CatalogConstants.SNAPSHOTS_JSON_KEY);
      String serializedSnapshotRefs = properties.remove(CatalogConstants.SNAPSHOTS_REFS_KEY);
      boolean isStageCreate =
          Boolean.parseBoolean(properties.remove(CatalogConstants.IS_STAGE_CREATE_KEY));
      String sortOrderJson = properties.remove(CatalogConstants.SORT_ORDER_KEY);
      logPropertiesMap(properties);

      TableMetadata updatedMetadata = metadata.replaceProperties(properties);

      if (sortOrderJson != null) {
        SortOrder sortOrder = SortOrderParser.fromJson(updatedMetadata.schema(), sortOrderJson);
        updatedMetadata = updatedMetadata.replaceSortOrder(sortOrder);
      }

      if (serializedSnapshotsToPut != null) {
        List<Snapshot> snapshotsToPut =
            SnapshotsUtil.parseSnapshots(fileIO, serializedSnapshotsToPut);
        Pair<List<Snapshot>, List<Snapshot>> snapshotsDiff =
            SnapshotsUtil.symmetricDifferenceSplit(snapshotsToPut, updatedMetadata.snapshots());
        List<Snapshot> appendedSnapshots = snapshotsDiff.getFirst();
        List<Snapshot> deletedSnapshots = snapshotsDiff.getSecond();
        snapshotInspector.validateSnapshotsUpdate(
            updatedMetadata, appendedSnapshots, deletedSnapshots);
        Map<String, SnapshotRef> snapshotRefs =
            serializedSnapshotRefs == null
                ? new HashMap<>()
                : SnapshotsUtil.parseSnapshotRefs(serializedSnapshotRefs);

        // Multi-branch support is now enabled with snapshot ID matching

        updatedMetadata =
            applySnapshotOperations(updatedMetadata, appendedSnapshots, snapshotRefs, true);
        updatedMetadata = maybeDeleteSnapshots(updatedMetadata, deletedSnapshots);
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

  public TableMetadata maybeDeleteSnapshots(
      TableMetadata metadata, List<Snapshot> snapshotsToDelete) {
    TableMetadata result = metadata;
    if (CollectionUtils.isNotEmpty(snapshotsToDelete)) {
      Set<Long> snapshotIds =
          snapshotsToDelete.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      Map<String, String> updatedProperties = new HashMap<>(result.properties());
      updatedProperties.put(
          getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
          snapshotsToDelete.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")));
      result =
          TableMetadata.buildFrom(result)
              .setProperties(updatedProperties)
              .build()
              .removeSnapshotsIf(s -> snapshotIds.contains(s.snapshotId()));
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR, snapshotsToDelete.size());
    }
    return result;
  }

  /**
   * Determines the target branch for a snapshot commit based on the provided snapshotRefs.
   *
   * @param snapshotRefs map of branch names to snapshot references
   * @param defaultBranch default branch to use if no specific branch can be determined
   * @return target branch name for the snapshot commit
   */
  private String determineTargetBranch(
      Map<String, SnapshotRef> snapshotRefs, String defaultBranch) {
    return determineTargetBranch(snapshotRefs, Collections.emptyList(), defaultBranch);
  }

  /**
   * Returns the single target branch when only one branch is explicitly specified. This is the most
   * common case - client explicitly specified which branch to commit to.
   */
  private String getSingleTargetBranch(Map<String, SnapshotRef> snapshotRefs) {
    String targetBranch = snapshotRefs.keySet().iterator().next();
    log.debug("Using explicit target branch from commit context: {}", targetBranch);
    return targetBranch;
  }

  /**
   * Finds branches that exactly match the given snapshot ID. Returns the single matching branch, or
   * null if there are zero or multiple matches.
   */
  private String findExactSnapshotMatch(Map<String, SnapshotRef> snapshotRefs, long snapshotId) {
    List<String> exactMatches = new ArrayList<>();
    for (Map.Entry<String, SnapshotRef> entry : snapshotRefs.entrySet()) {
      String branchName = entry.getKey();
      long branchSnapshotId = entry.getValue().snapshotId();

      if (branchSnapshotId == snapshotId) {
        exactMatches.add(branchName);
      }
    }

    if (exactMatches.size() == 1) {
      String targetBranch = exactMatches.get(0);
      log.info(
          "Determined target branch '{}' by exact snapshot ID match within commit context: {}",
          targetBranch,
          snapshotId);
      return targetBranch;
    } else if (exactMatches.size() > 1) {
      log.error("Multiple branches point to same snapshot {}: {}", snapshotId, exactMatches);
      throw new IllegalStateException(
          String.format(
              "Multiple explicitly targeted branches point to the same snapshot %s: %s. "
                  + "This indicates an invalid commit state.",
              snapshotId, exactMatches));
    }

    // No exact match or zero matches
    return null;
  }

  /**
   * Finds branches that match parent-child relationship with the given snapshot. Returns the single
   * matching branch, or null if there are zero or multiple matches.
   */
  private String findParentChildMatch(
      Map<String, SnapshotRef> snapshotRefs, long parentSnapshotId, long childSnapshotId) {
    List<String> parentMatches = new ArrayList<>();
    for (Map.Entry<String, SnapshotRef> entry : snapshotRefs.entrySet()) {
      String branchName = entry.getKey();
      long branchSnapshotId = entry.getValue().snapshotId();

      if (branchSnapshotId == parentSnapshotId) {
        parentMatches.add(branchName);
        log.info("Branch '{}' matches parent snapshot {}", branchName, parentSnapshotId);
      }
    }

    if (parentMatches.size() == 1) {
      String targetBranch = parentMatches.get(0);
      log.info(
          "Determined target branch '{}' by parent-child relationship within commit context: new snapshot {} is child of branch snapshot {}",
          targetBranch,
          childSnapshotId,
          parentSnapshotId);
      return targetBranch;
    } else if (parentMatches.size() > 1) {
      log.error(
          "Multiple branches point to parent snapshot {}: {}", parentSnapshotId, parentMatches);
      throw new IllegalStateException(
          String.format(
              "Multiple explicitly targeted branches point to parent snapshot %s: %s. "
                  + "Cannot determine which branch should receive child snapshot %s. "
                  + "This indicates ambiguous commit targeting - the client should specify a single target branch.",
              parentSnapshotId, parentMatches, childSnapshotId));
    }

    // No parent match or zero matches - could happen in cherry-pick or other non-linear operations
    return null;
  }

  /**
   * Determines target branch when multiple branches are specified by analyzing snapshot
   * relationships.
   */
  private String determineTargetFromMultipleBranches(
      Map<String, SnapshotRef> snapshotRefs, List<Snapshot> newSnapshots) {

    log.info(
        "Multiple branches in snapshotRefs ({}), analyzing snapshot relationships",
        snapshotRefs.size());

    if (!newSnapshots.isEmpty()) {
      Snapshot latestSnapshot = newSnapshots.get(newSnapshots.size() - 1);
      long latestSnapshotId = latestSnapshot.snapshotId();
      log.info("Latest snapshot ID: {}", latestSnapshotId);

      // First try: exact snapshot ID match
      String exactMatch = findExactSnapshotMatch(snapshotRefs, latestSnapshotId);
      if (exactMatch != null) {
        return exactMatch;
      }

      // Second try: parent-child relationship match
      Long parentSnapshotId = latestSnapshot.parentId();
      log.info("Parent snapshot ID: {}", parentSnapshotId);
      if (parentSnapshotId != null) {
        String parentMatch = findParentChildMatch(snapshotRefs, parentSnapshotId, latestSnapshotId);
        if (parentMatch != null) {
          return parentMatch;
        }
      }
    }

    // If we reach here, we have multiple explicitly targeted branches but couldn't determine
    // the target based on snapshot relationships. This suggests the commit operation itself
    // is ambiguous or invalid.
    log.error(
        "Cannot determine target branch from explicitly targeted branches: {}",
        snapshotRefs.keySet());
    throw new IllegalStateException(
        String.format(
            "Cannot determine target branch from explicitly targeted branches: %s. "
                + "The commit specifies multiple target branches but snapshot relationships "
                + "don't clearly indicate which branch should receive the new snapshots. "
                + "This suggests an invalid or ambiguous commit operation.",
            snapshotRefs.keySet()));
  }

  /**
   * Determines the target branch for snapshot commits using explicit branch targeting information.
   * The snapshotRefs parameter contains the explicit branch targeting from the client commit
   * operation.
   */
  private String determineTargetBranch(
      Map<String, SnapshotRef> snapshotRefs, List<Snapshot> newSnapshots, String defaultBranch) {

    // Handle simple case: no explicit branch targeting
    if (MapUtils.isEmpty(snapshotRefs)) {
      return defaultBranch;
    }

    // Handle simple case: single branch explicitly specified
    if (snapshotRefs.size() == 1) {
      return getSingleTargetBranch(snapshotRefs);
    }

    // Handle complex case: multiple branches with snapshot relationship analysis
    return determineTargetFromMultipleBranches(snapshotRefs, newSnapshots);
  }

  /**
   * Applies a regular (non-WAP, non-cherry-picked) snapshot by assigning it to a branch or staging
   * it.
   */
  private void applyRegularSnapshot(
      Snapshot snapshot,
      Map<String, SnapshotRef> snapshotRefs,
      TableMetadata.Builder metadataBuilder) {

    if (MapUtils.isNotEmpty(snapshotRefs)) {
      // We have explicit branch information, use it to assign snapshot
      String targetBranch =
          determineTargetBranch(
              snapshotRefs, Collections.singletonList(snapshot), SnapshotRef.MAIN_BRANCH);
      metadataBuilder.setBranchSnapshot(snapshot, targetBranch);
    } else {
      // No branch information provided - add snapshot without assigning to any branch
      // The snapshot will exist in metadata but won't be the HEAD of any branch
      // Branch refs can be updated later via separate calls to applySnapshotOperations with
      // snapshotRefs
      metadataBuilder.addSnapshot(snapshot);
    }
  }

  /** Applies a WAP staged snapshot - not committed to any branch. */
  private void applyStagedSnapshot(Snapshot snapshot, TableMetadata.Builder metadataBuilder) {
    metadataBuilder.addSnapshot(snapshot);
  }

  /** Applies a cherry-picked snapshot - non fast-forward cherry pick. */
  private void applyCherryPickedSnapshot(
      Snapshot snapshot,
      Map<String, SnapshotRef> snapshotRefs,
      TableMetadata.Builder metadataBuilder) {
    String targetBranch =
        determineTargetBranch(
            snapshotRefs, Collections.singletonList(snapshot), SnapshotRef.MAIN_BRANCH);
    metadataBuilder.setBranchSnapshot(snapshot, targetBranch);
  }

  /** Result of categorizing and applying snapshots. */
  private static class SnapshotOperationResult {
    final List<String> appendedSnapshots;
    final List<String> stagedSnapshots;
    final List<String> cherryPickedSnapshots;

    SnapshotOperationResult(
        List<String> appendedSnapshots,
        List<String> stagedSnapshots,
        List<String> cherryPickedSnapshots) {
      this.appendedSnapshots = new ArrayList<>(appendedSnapshots);
      this.stagedSnapshots = new ArrayList<>(stagedSnapshots);
      this.cherryPickedSnapshots = new ArrayList<>(cherryPickedSnapshots);
    }
  }

  /** Categorizes snapshots by type and applies them to the metadata builder. */
  private SnapshotOperationResult categorizeAndApplySnapshots(
      List<Snapshot> snapshots,
      Map<String, SnapshotRef> snapshotRefs,
      TableMetadata.Builder metadataBuilder) {

    List<String> appendedSnapshots = new ArrayList<>();
    List<String> stagedSnapshots = new ArrayList<>();
    List<String> cherryPickedSnapshots = new ArrayList<>();

    for (Snapshot snapshot : snapshots) {
      snapshotInspector.validateSnapshot(snapshot);

      if (snapshot.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)) {
        applyStagedSnapshot(snapshot, metadataBuilder);
        stagedSnapshots.add(String.valueOf(snapshot.snapshotId()));

      } else if (snapshot.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)) {
        applyCherryPickedSnapshot(snapshot, snapshotRefs, metadataBuilder);
        appendedSnapshots.add(String.valueOf(snapshot.snapshotId()));
        cherryPickedSnapshots.add(
            String.valueOf(snapshot.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)));

      } else {
        applyRegularSnapshot(snapshot, snapshotRefs, metadataBuilder);
        appendedSnapshots.add(String.valueOf(snapshot.snapshotId()));
      }
    }

    return new SnapshotOperationResult(appendedSnapshots, stagedSnapshots, cherryPickedSnapshots);
  }

  /**
   * Updates branch references for fast-forward cherry-pick or rollback operations. Returns list of
   * cherry-picked snapshot IDs.
   */
  private List<String> updateBranchReferences(
      TableMetadata metadata,
      Map<String, SnapshotRef> snapshotRefs,
      TableMetadata.Builder metadataBuilder) {

    List<String> cherryPickedSnapshots = new ArrayList<>();

    for (Map.Entry<String, SnapshotRef> entry : snapshotRefs.entrySet()) {
      String branchName = entry.getKey();
      long newSnapshotId = entry.getValue().snapshotId();

      if (needsBranchUpdate(metadata, branchName, newSnapshotId)) {
        metadataBuilder.setBranchSnapshot(newSnapshotId, branchName);
        cherryPickedSnapshots.add(String.valueOf(newSnapshotId));
      }
    }

    return cherryPickedSnapshots;
  }

  /** Checks if a branch needs to be updated based on current refs and new snapshot ID. */
  private boolean needsBranchUpdate(TableMetadata metadata, String branchName, long newSnapshotId) {
    if (MapUtils.isEmpty(metadata.refs())) {
      // No refs exist yet, this is a new branch
      return true;
    }

    SnapshotRef currentRef = metadata.refs().get(branchName);
    return currentRef == null || currentRef.snapshotId() != newSnapshotId;
  }

  /** Records snapshot actions in table properties and reports metrics. */
  private void recordSnapshotActions(
      TableMetadata metadata,
      TableMetadata.Builder metadataBuilder,
      List<String> appendedSnapshots,
      List<String> stagedSnapshots,
      List<String> cherryPickedSnapshots) {

    Map<String, String> updatedProperties = new HashMap<>(metadata.properties());

    if (CollectionUtils.isNotEmpty(appendedSnapshots)) {
      updatedProperties.put(
          getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
          appendedSnapshots.stream().collect(Collectors.joining(",")));
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR, appendedSnapshots.size());
    }

    if (CollectionUtils.isNotEmpty(stagedSnapshots)) {
      updatedProperties.put(
          getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
          stagedSnapshots.stream().collect(Collectors.joining(",")));
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR, stagedSnapshots.size());
    }

    if (CollectionUtils.isNotEmpty(cherryPickedSnapshots)) {
      updatedProperties.put(
          getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
          cherryPickedSnapshots.stream().collect(Collectors.joining(",")));
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR, cherryPickedSnapshots.size());
    }

    metadataBuilder.setProperties(updatedProperties);
  }

  public TableMetadata applySnapshotOperations(
      TableMetadata metadata,
      List<Snapshot> snapshots,
      Map<String, SnapshotRef> snapshotRefs,
      boolean recordAction) {
    TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(metadata);

    /**
     * Apply snapshots to current TableMetadata. The following cases are handled:
     *
     * <p>[1] A regular (non-wap) snapshot is being added to any branch.
     *
     * <p>[2] A staged (wap) snapshot is being created on top of current snapshot as its base.
     * Recognized by STAGED_WAP_ID_PROP. These are stage-only and not committed to any branch.
     *
     * <p>[3] A staged (wap) snapshot is being cherry picked to any branch wherein current snapshot
     * in the target branch is not the same as the base snapshot the staged (wap) snapshot was
     * created on. Recognized by SOURCE_SNAPSHOT_ID_PROP. This case is called non-fast forward
     * cherry pick.
     *
     * <p>Additionally, branch ref updates can occur independently for fast-forward cherry-pick or
     * rollback operations where existing snapshots are assigned to branches.
     */
    SnapshotOperationResult snapshotResult =
        CollectionUtils.isNotEmpty(snapshots)
            ? categorizeAndApplySnapshots(snapshots, snapshotRefs, metadataBuilder)
            : new SnapshotOperationResult(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    // Handle ref updates (this can happen independently of snapshot processing operations)
    List<String> refUpdateResults =
        MapUtils.isNotEmpty(snapshotRefs)
            ? updateBranchReferences(metadata, snapshotRefs, metadataBuilder)
            : Collections.emptyList();

    if (recordAction) {
      // Combine cherry-picked snapshots from both operations
      List<String> allCherryPickedSnapshots = new ArrayList<>(snapshotResult.cherryPickedSnapshots);
      allCherryPickedSnapshots.addAll(refUpdateResults);

      recordSnapshotActions(
          metadata,
          metadataBuilder,
          snapshotResult.appendedSnapshots,
          snapshotResult.stagedSnapshots,
          allCherryPickedSnapshots);
    }
    return metadataBuilder.build();
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
