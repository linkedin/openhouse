package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.CatalogConstants.COMMIT_KEY;
import static com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant.*;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.Gson;
import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryUnavailableException;
import java.time.Clock;
import java.time.Instant;
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
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.springframework.data.util.Pair;

@AllArgsConstructor
@Slf4j
public class OpenHouseInternalTableOperations extends BaseMetastoreTableOperations {

  private static final String INITIAL_VERSION = "INITIAL_VERSION";

  public static final String SNAPSHOTS_JSON_KEY = "snapshotsJsonToBePut";

  public static final String IS_STAGE_CREATE_KEY = "isStageCreate";

  HouseTableRepository houseTableRepository;

  FileIO fileIO;

  SnapshotInspector snapshotInspector;

  HouseTableMapper houseTableMapper;

  TableIdentifier tableIdentifier;

  MetricsReporter metricsReporter;

  private static final Gson GSON = new Gson();

  private static final Cache<String, Integer> CACHE =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).maximumSize(100).build();

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
      metricsReporter.count(NO_TABLE_WHEN_REFRESH);
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
    boolean needToReload = !Objects.equal(currentMetadataLocation(), metadataLoc);

    Runnable r = () -> super.refreshFromMetadataLocation(metadataLoc);
    if (needToReload) {
      metricsReporter.executeWithStats(r, METADATA_RETRIEVAL_LATENCY);
    } else {
      r.run();
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
    boolean isStageCreate = Boolean.parseBoolean(metadata.properties().get(IS_STAGE_CREATE_KEY));
    super.commit(base, metadata);
    if (isStageCreate) {
      disableRefresh(); /* disable forced refresh */
    }
  }

  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    int version = currentVersion() + 1;
    CommitStatus commitStatus = CommitStatus.FAILURE;

    /* This method adds no fs scheme, and it persists in HTS that way. */
    final String newMetadataLocation = rootMetadataFileLocation(metadata, version);

    HouseTable houseTable = HouseTable.builder().build();
    try {
      // Now that we have metadataLocation we stamp it in metadata property.
      Map<String, String> properties = new HashMap<>(metadata.properties());
      failIfRetryUpdate(properties);

      String currentTsString = String.valueOf(Instant.now(Clock.systemUTC()).toEpochMilli());
      properties.put(getCanonicalFieldName("lastModifiedTime"), currentTsString);
      if (base == null) {
        properties.put(getCanonicalFieldName("creationTime"), currentTsString);
      }
      properties.put(
          getCanonicalFieldName("tableVersion"),
          properties.getOrDefault(getCanonicalFieldName("tableLocation"), INITIAL_VERSION));
      properties.put(getCanonicalFieldName("tableLocation"), newMetadataLocation);

      String serializedSnapshotsToPut = properties.remove(SNAPSHOTS_JSON_KEY);
      boolean isStageCreate = Boolean.parseBoolean(properties.remove(IS_STAGE_CREATE_KEY));
      logPropertiesMap(properties);

      TableMetadata updatedMetadata = metadata.replaceProperties(properties);

      if (serializedSnapshotsToPut != null) {
        List<Snapshot> snapshotsToPut =
            SnapshotsUtil.parseSnapshots(fileIO, serializedSnapshotsToPut);
        Pair<List<Snapshot>, List<Snapshot>> diff =
            SnapshotsUtil.symmetricDifferenceSplit(snapshotsToPut, updatedMetadata.snapshots());
        List<Snapshot> appendedSnapshots = diff.getFirst();
        List<Snapshot> deletedSnapshots = diff.getSecond();
        snapshotInspector.validateSnapshotsUpdate(
            updatedMetadata, appendedSnapshots, deletedSnapshots);
        updatedMetadata = maybeAppendSnapshots(updatedMetadata, appendedSnapshots, true);
        updatedMetadata = maybeDeleteSnapshots(updatedMetadata, deletedSnapshots);
      }

      final TableMetadata updatedMtDataRef = updatedMetadata;
      metricsReporter.executeWithStats(
          () ->
              TableMetadataParser.write(updatedMtDataRef, io().newOutputFile(newMetadataLocation)),
          METADATA_UPDATE_LATENCY);

      houseTable = houseTableMapper.toHouseTable(updatedMetadata);

      if (!isStageCreate) {
        houseTableRepository.save(houseTable);
      } else {
        /**
         * Refresh current metadata for staged tables from newly created metadata file and disable
         * "forced refresh" in {@link OpenHouseInternalTableOperations#commit(TableMetadata,
         * TableMetadata)}
         */
        refreshFromMetadataLocation(newMetadataLocation);
      }
      commitStatus = CommitStatus.SUCCESS;
    } catch (InvalidIcebergSnapshotException e) {
      throw new BadRequestException(e, e.getMessage());
    } catch (CommitFailedException e) {
      throw e;
    } catch (HouseTableCallerException
        | HouseTableRepositoryUnavailableException
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
          metricsReporter.count(COMMIT_FAILED_CTR);
          break;
        case UNKNOWN:
          metricsReporter.count(COMMIT_STATE_UNKNOWN);
          break;
        default:
          break; /*should never happen, kept to silence SpotBugs*/
      }
    }
  }

  /**
   * If this commit comes from Iceberg built-in retry in
   * org.apache.iceberg.PropertiesUpdate#commit() Then throw fatal {@link CommitFailedException} to
   * inform users.
   */
  private void failIfRetryUpdate(Map<String, String> properties) {
    if (properties.containsKey(COMMIT_KEY)) {
      String userProvidedTblVer = properties.get(COMMIT_KEY);

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

      properties.remove(COMMIT_KEY);
    } else {
      // This should never occur except table-creation. However, when table-creation hits
      // concurrency issue
      // it throw AlreadyExistsException and will not trigger retry.
      metricsReporter.count(MISSING_COMMIT_KEY);
    }
  }

  public TableMetadata maybeDeleteSnapshots(TableMetadata metadata, List<Snapshot> snapshots) {
    TableMetadata result = metadata;
    if (!snapshots.isEmpty()) {
      Set<Long> snapshotIds =
          snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      Map<String, String> updatedProperties = new HashMap<>(result.properties());
      updatedProperties.put(
          getCanonicalFieldName("deleted_snapshots"),
          snapshots.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")));
      result =
          TableMetadata.buildFrom(result)
              .setProperties(updatedProperties)
              .build()
              .removeSnapshotsIf(s -> snapshotIds.contains(s.snapshotId()));
      metricsReporter.count(SNAPSHOTS_DELETED_CTR, snapshots.size());
    }
    return result;
  }

  public TableMetadata maybeAppendSnapshots(
      TableMetadata metadata, List<Snapshot> snapshots, boolean recordAction) {
    TableMetadata result = metadata;
    if (!snapshots.isEmpty()) {
      for (Snapshot snapshot : snapshots) {
        snapshotInspector.validateSnapshot(snapshot);
        result =
            TableMetadata.buildFrom(result)
                .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
                .build();
      }
      if (recordAction) {
        Map<String, String> updatedProperties = new HashMap<>(result.properties());
        updatedProperties.put(
            getCanonicalFieldName("appended_snapshots"),
            snapshots.stream()
                .map(s -> Long.toString(s.snapshotId()))
                .collect(Collectors.joining(",")));
        result = TableMetadata.buildFrom(result).setProperties(updatedProperties).build();
        metricsReporter.count(SNAPSHOTS_ADDED_CTR, snapshots.size());
      }
    }
    return result;
  }

  /** Helper function to dump contents for map in debugging mode. */
  private void logPropertiesMap(Map<String, String> map) {
    log.debug(" === Printing the table properties within doCommit method === ");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      log.debug(entry.getKey() + ":" + entry.getValue());
    }
  }
}
