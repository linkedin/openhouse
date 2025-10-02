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

  /** Represents the semantic difference between current server state and client-desired state. */
  private static class StateDiff {
    final List<Snapshot> newSnapshots;
    final Map<String, Long> branchUpdates; // branch -> snapshotId
    final Map<String, Snapshot> snapshotLookup; // snapshotId -> Snapshot for efficiency

    StateDiff(
        List<Snapshot> newSnapshots,
        Map<String, Long> branchUpdates,
        List<Snapshot> allClientSnapshots) {
      this.newSnapshots = List.copyOf(newSnapshots);
      this.branchUpdates = Map.copyOf(branchUpdates);
      this.snapshotLookup =
          allClientSnapshots.stream()
              .collect(Collectors.toMap(s -> String.valueOf(s.snapshotId()), s -> s));
    }
  }

  /**
   * Combines cherry-picked snapshot IDs from both snapshot processing and standalone ref
   * operations.
   */
  private List<String> combineCherryPickedSnapshots(
      List<String> fromSnapshotProcessing, List<String> fromStandaloneRefUpdates) {
    List<String> allCherryPicks = new ArrayList<>(fromSnapshotProcessing);
    allCherryPicks.addAll(fromStandaloneRefUpdates);
    return allCherryPicks;
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

  /**
   * Applies client-requested changes to server state using functional approach.
   *
   * <p>Contract: currentState + clientDesiredState -> newState + metrics
   *
   * <p>Client sends desired final state, server computes semantic diff and applies it.
   */
  public TableMetadata applySnapshotOperations(
      TableMetadata currentMetadata,
      List<Snapshot> clientSnapshots,
      Map<String, SnapshotRef> clientRefs,
      boolean recordAction) {

    return computeStateDiff(currentMetadata, clientSnapshots, clientRefs)
        .map(
            diff -> {
              TableMetadata newMetadata = applyStateDiff(currentMetadata, diff);
              return recordAction
                  ? recordTransition(currentMetadata, newMetadata, diff)
                  : newMetadata;
            })
        .orElse(currentMetadata);
  }

  /** Computes semantic difference between current server state and client-desired state. */
  private Optional<StateDiff> computeStateDiff(
      TableMetadata currentMetadata,
      List<Snapshot> clientSnapshots,
      Map<String, SnapshotRef> clientRefs) {

    if (CollectionUtils.isEmpty(clientSnapshots) && MapUtils.isEmpty(clientRefs)) {
      return Optional.empty(); // No changes requested
    }

    Set<Long> currentSnapshotIds =
        currentMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    // Find truly new snapshots (not in current metadata)
    List<Snapshot> newSnapshots =
        Optional.ofNullable(clientSnapshots).orElse(Collections.emptyList()).stream()
            .filter(s -> !currentSnapshotIds.contains(s.snapshotId()))
            .collect(Collectors.toList());

    // Find branch updates needed
    Map<String, Long> branchUpdates =
        Optional.ofNullable(clientRefs).orElse(Collections.emptyMap()).entrySet().stream()
            .filter(
                entry ->
                    needsBranchUpdate(
                        currentMetadata, entry.getKey(), entry.getValue().snapshotId()))
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().snapshotId()));

    return Optional.of(
        new StateDiff(
            newSnapshots,
            branchUpdates,
            Optional.ofNullable(clientSnapshots).orElse(Collections.emptyList())));
  }

  /** Applies the computed state diff to create new metadata. */
  private TableMetadata applyStateDiff(TableMetadata currentMetadata, StateDiff diff) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(currentMetadata);

    // Add new snapshots (respecting Iceberg semantics)
    diff.newSnapshots.forEach(
        snapshot -> {
          snapshotInspector.validateSnapshot(snapshot);

          if (isWapStaged(snapshot)) {
            // WAP snapshots are always staged (never assigned to branches initially)
            builder.addSnapshot(snapshot);
          } else {
            // All other snapshots: assign to branch if specified, otherwise stage
            findTargetBranchForSnapshot(snapshot, diff.branchUpdates)
                .ifPresentOrElse(
                    targetBranch -> builder.setBranchSnapshot(snapshot, targetBranch),
                    () -> builder.addSnapshot(snapshot));
          }
        });

    // Update branch pointers to existing snapshots
    diff.branchUpdates.entrySet().stream()
        .filter(entry -> !isNewSnapshot(entry.getValue(), diff.newSnapshots))
        .forEach(entry -> builder.setBranchSnapshot(entry.getValue(), entry.getKey()));

    return builder.build();
  }

  /** Checks if snapshot is WAP staged (should not be assigned to any branch). */
  private boolean isWapStaged(Snapshot snapshot) {
    return snapshot.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP);
  }

  /** Checks if snapshot is cherry-picked (should go directly to target branch). */
  private boolean isCherryPicked(Snapshot snapshot) {
    return snapshot.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
  }

  /**
   * Finds which branch this snapshot should be assigned to based on branch updates. Fails fast if
   * multiple branches want the same snapshot (ambiguous commit).
   */
  private Optional<String> findTargetBranchForSnapshot(
      Snapshot snapshot, Map<String, Long> branchUpdates) {
    List<String> matchingBranches =
        branchUpdates.entrySet().stream()
            .filter(entry -> entry.getValue() == snapshot.snapshotId())
            .map(Map.Entry::getKey)
            .toList();

    if (matchingBranches.size() > 1) {
      throw new IllegalStateException(
          "Multiple branches (%s) specify the same target snapshot %d. "
              + "This indicates an ambiguous commit operation - each snapshot can only be assigned to one branch."
                  .formatted(matchingBranches, snapshot.snapshotId()));
    }

    return matchingBranches.stream().findFirst();
  }

  /** Checks if this snapshot ID is in the list of new snapshots being added. */
  private boolean isNewSnapshot(Long snapshotId, List<Snapshot> newSnapshots) {
    return newSnapshots.stream().anyMatch(s -> s.snapshotId() == snapshotId);
  }

  /** Records metrics and properties about the state transition that occurred. */
  private TableMetadata recordTransition(
      TableMetadata originalMetadata, TableMetadata newMetadata, StateDiff diff) {

    Map<String, String> properties = new HashMap<>(newMetadata.properties());

    // Categorize new snapshots by their semantic type for metrics
    Map<String, List<String>> snapshotsByType =
        diff.newSnapshots.stream()
            .collect(
                Collectors.groupingBy(
                    this::getSnapshotCategory,
                    Collectors.mapping(s -> String.valueOf(s.snapshotId()), Collectors.toList())));

    // Record snapshot metrics by type
    recordIfPresent(
        properties,
        snapshotsByType,
        "appended",
        CatalogConstants.APPENDED_SNAPSHOTS,
        InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR);
    recordIfPresent(
        properties,
        snapshotsByType,
        "staged",
        CatalogConstants.STAGED_SNAPSHOTS,
        InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR);

    // For cherry-picked snapshots, record the SOURCE snapshot IDs that were cherry-picked
    List<String> cherryPickSourceIds =
        diff.newSnapshots.stream()
            .filter(this::isCherryPicked)
            .map(this::getCherryPickSourceId)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    if (!cherryPickSourceIds.isEmpty()) {
      properties.put(
          getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
          String.join(",", cherryPickSourceIds));
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR, cherryPickSourceIds.size());
    }

    // Record branch updates that don't involve new snapshots (pure ref moves)
    List<String> refOnlyCherryPicks =
        diff.branchUpdates.entrySet().stream()
            .filter(entry -> !isNewSnapshot(entry.getValue(), diff.newSnapshots))
            .map(entry -> String.valueOf(entry.getValue()))
            .collect(Collectors.toList());

    if (!refOnlyCherryPicks.isEmpty()) {
      String existing =
          properties.get(getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS));
      String combined =
          existing != null
              ? existing + "," + String.join(",", refOnlyCherryPicks)
              : String.join(",", refOnlyCherryPicks);
      properties.put(getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS), combined);
      metricsReporter.count(
          InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR, refOnlyCherryPicks.size());
    }

    return TableMetadata.buildFrom(newMetadata).setProperties(properties).build();
  }

  /** Categorizes snapshot for metrics based on its semantic type. */
  private String getSnapshotCategory(Snapshot snapshot) {
    if (isWapStaged(snapshot)) return "staged";
    if (isCherryPicked(snapshot))
      return "appended"; // Cherry-picked snapshots are NEW, so they're "appended"
    return "appended";
  }

  /** Extracts the source snapshot ID for cherry-picked snapshots. */
  private Optional<String> getCherryPickSourceId(Snapshot snapshot) {
    return Optional.ofNullable(snapshot.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP));
  }

  /** Records snapshot category in properties if snapshots exist. */
  private void recordIfPresent(
      Map<String, String> properties,
      Map<String, List<String>> categorized,
      String category,
      String propertyKey,
      String metricKey) {

    Optional.ofNullable(categorized.get(category))
        .filter(CollectionUtils::isNotEmpty)
        .ifPresent(
            snapshots -> {
              properties.put(getCanonicalFieldName(propertyKey), String.join(",", snapshots));
              metricsReporter.count(metricKey, snapshots.size());
            });
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
