package com.linkedin.openhouse.jobs.util;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static org.apache.spark.sql.functions.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.stats.model.BaseEventModels;
import com.linkedin.openhouse.common.stats.model.ColumnData;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.CommitMetadata;
import com.linkedin.openhouse.common.stats.model.CommitOperation;
import com.linkedin.openhouse.common.stats.model.HistoryPolicyStatsSchema;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.common.stats.model.PolicyStats;
import com.linkedin.openhouse.common.stats.model.RetentionStatsSchema;
import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;

/** Utility class to collect stats for a given table. */
@Slf4j
public final class TableStatsCollectorUtil {

  private TableStatsCollectorUtil() {}
  /** Collect stats about referenced files in a given table. */
  public static IcebergTableStats populateStatsOfAllReferencedFiles(
      Table table, SparkSession spark, IcebergTableStats stats) {
    long referencedManifestFilesCount =
        getManifestFilesCount(table, spark, MetadataTableType.ALL_MANIFESTS);

    long referencedManifestListFilesCount = ReachableFileUtil.manifestListLocations(table).size();
    long metadataFilesCount = ReachableFileUtil.metadataFileLocations(table, true).size();

    long totalMetadataFilesCount =
        referencedManifestFilesCount + referencedManifestListFilesCount + metadataFilesCount;

    Map<Integer, FilesSummary> allFilesSummary =
        getFileMetadataTable(table, spark, MetadataTableType.ALL_FILES);

    long countOfDataFiles =
        Optional.ofNullable(allFilesSummary.get(FileContent.DATA.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfDataFileSizeBytes =
        Optional.ofNullable(allFilesSummary.get(FileContent.DATA.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    long countOfPositionDeleteFiles =
        Optional.ofNullable(allFilesSummary.get(FileContent.POSITION_DELETES.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfPositionDeleteFileSizeBytes =
        Optional.ofNullable(allFilesSummary.get(FileContent.POSITION_DELETES.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    long countOfEqualityDeleteFiles =
        Optional.ofNullable(allFilesSummary.get(FileContent.EQUALITY_DELETES.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfEqualityDeleteFilesSizeBytes =
        Optional.ofNullable(allFilesSummary.get(FileContent.EQUALITY_DELETES.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    log.info(
        "Table: {}, Count of metadata files: {}, Manifest files: {}, Manifest list files: {}, Metadata files: {},"
            + "Data files: {}, Sum of file sizes in bytes: {}"
            + "Position delete files: {}, Sum of position delete file sizes in bytes: {}"
            + "Equality delete files: {}, Sum of equality delete file sizes in bytes: {}",
        table.name(),
        totalMetadataFilesCount,
        referencedManifestFilesCount,
        referencedManifestListFilesCount,
        metadataFilesCount,
        countOfDataFiles,
        sumOfDataFileSizeBytes,
        countOfPositionDeleteFiles,
        sumOfPositionDeleteFileSizeBytes,
        countOfEqualityDeleteFiles,
        sumOfEqualityDeleteFilesSizeBytes);

    return stats
        .toBuilder()
        .numReferencedDataFiles(countOfDataFiles)
        .totalReferencedDataFilesSizeInBytes(sumOfDataFileSizeBytes)
        .numPositionDeleteFiles(countOfPositionDeleteFiles)
        .totalPositionDeleteFileSizeInBytes(sumOfPositionDeleteFileSizeBytes)
        .numEqualityDeleteFiles(countOfEqualityDeleteFiles)
        .totalEqualityDeleteFileSizeInBytes(sumOfEqualityDeleteFilesSizeBytes)
        .numReferencedManifestFiles(referencedManifestFilesCount)
        .numReferencedManifestLists(referencedManifestListFilesCount)
        .numExistingMetadataJsonFiles(metadataFilesCount)
        .build();
  }

  /** Collect stats for snapshots of a given table. */
  public static IcebergTableStats populateStatsForSnapshots(
      Table table, SparkSession spark, IcebergTableStats stats) {

    Map<Integer, FilesSummary> currentSnapshotFilesSummary =
        getFileMetadataTable(table, spark, MetadataTableType.FILES);

    long countOfDataFiles =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.DATA.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfDataFileSizeBytes =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.DATA.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    long countOfPositionDeleteFiles =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.POSITION_DELETES.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfPositionDeleteFileSizeBytes =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.POSITION_DELETES.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    long countOfEqualityDeleteFiles =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.EQUALITY_DELETES.id()))
            .map(FilesSummary::getTotalFileCount)
            .orElse(0L);
    long sumOfEqualityDeleteFilesSizeBytes =
        Optional.ofNullable(currentSnapshotFilesSummary.get(FileContent.EQUALITY_DELETES.id()))
            .map(FilesSummary::getSumOfFileSizeBytes)
            .orElse(0L);

    Long currentSnapshotId =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null);

    Long currentSnapshotTimestamp =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::timestampMillis).orElse(null);
    PolicyStats policyStats = getTablePolicies(table);
    String earliestPartitionDate =
        getEarliestPartitionDate(table, spark, policyStats.getRetentionPolicy());

    log.info(
        "Table: {}, Count of total Data files in snapshot: {}, Sum of file sizes in bytes: {}"
            + ", Position delete files: {}, Sum of position delete file sizes in bytes: {}"
            + ", Equality delete files: {}, Sum of equality delete file sizes in bytes: {}"
            + ", Earliest partition date: {}, for snapshot: {}",
        table.name(),
        countOfDataFiles,
        sumOfDataFileSizeBytes,
        countOfPositionDeleteFiles,
        sumOfPositionDeleteFileSizeBytes,
        countOfEqualityDeleteFiles,
        sumOfEqualityDeleteFilesSizeBytes,
        currentSnapshotId,
        earliestPartitionDate);

    List<Long> snapshotTimestamps =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(Snapshot::timestampMillis)
            .sorted(Long::compareTo)
            .collect(Collectors.toList());

    Integer numSnapshots = snapshotTimestamps.size();
    Long oldestSnapshotTimestamp = numSnapshots > 0 ? snapshotTimestamps.get(0) : null;
    Long secondOldestSnapshotTimestamp = numSnapshots > 1 ? snapshotTimestamps.get(1) : null;

    return stats
        .toBuilder()
        .currentSnapshotId(currentSnapshotId)
        .currentSnapshotTimestamp(currentSnapshotTimestamp)
        .oldestSnapshotTimestamp(oldestSnapshotTimestamp)
        .secondOldestSnapshotTimestamp(secondOldestSnapshotTimestamp)
        .numCurrentSnapshotReferencedDataFiles(countOfDataFiles)
        .totalCurrentSnapshotReferencedDataFilesSizeInBytes(sumOfDataFileSizeBytes)
        .numCurrentSnapshotPositionDeleteFiles(countOfPositionDeleteFiles)
        .totalCurrentSnapshotPositionDeleteFileSizeInBytes(sumOfPositionDeleteFileSizeBytes)
        .numCurrentSnapshotEqualityDeleteFiles(countOfEqualityDeleteFiles)
        .totalCurrentSnapshotEqualityDeleteFileSizeInBytes(sumOfEqualityDeleteFilesSizeBytes)
        .earliestPartitionDate(earliestPartitionDate)
        .numSnapshots(numSnapshots)
        .historyPolicy(policyStats.getHistoryPolicy())
        .build();
  }

  /** Collect storage stats for a given table. */
  public static IcebergTableStats populateStorageStats(
      Table table, FileSystem fs, IcebergTableStats stats) {
    // Find the sum of file size in bytes on HDFS by listing recursively all files in the table
    // location using filesystem call. This just replicates hdfs dfs -count and hdfs dfs -du -s.
    long sumOfTotalDirectorySizeInBytes = 0;
    long numOfObjectsInDirectory = 0;
    try {
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(table.location()), true);
      while (it.hasNext()) {
        LocatedFileStatus status = it.next();
        numOfObjectsInDirectory++;
        sumOfTotalDirectorySizeInBytes += status.getLen();
      }
    } catch (IOException e) {
      log.error("Error while listing files in HDFS directory for table: {}", table.name(), e);
      return stats;
    }

    log.info(
        "Table: {}, Count of objects in HDFS directory: {}, Sum of file sizes in bytes on HDFS: {}",
        table.name(),
        numOfObjectsInDirectory,
        sumOfTotalDirectorySizeInBytes);
    return stats
        .toBuilder()
        .numObjectsInDirectory(numOfObjectsInDirectory)
        .totalDirectorySizeInBytes(sumOfTotalDirectorySizeInBytes)
        .build();
  }

  /** Collect table metadata for a given table. */
  public static IcebergTableStats populateTableMetadata(Table table, IcebergTableStats stats) {
    PolicyStats policyStats = getTablePolicies(table);
    return stats
        .toBuilder()
        .recordTimestamp(System.currentTimeMillis())
        .clusterName(table.properties().get(getCanonicalFieldName("clusterId")))
        .databaseName(table.properties().get(getCanonicalFieldName("databaseId")))
        .tableName(table.properties().get(getCanonicalFieldName("tableId")))
        .tableType(table.properties().get(getCanonicalFieldName("tableType")))
        .tableCreator((table.properties().get(getCanonicalFieldName("tableCreator"))))
        .tableCreationTimestamp(
            table.properties().containsKey(getCanonicalFieldName("creationTime"))
                ? Long.parseLong(table.properties().get(getCanonicalFieldName("creationTime")))
                : 0)
        .tableLastUpdatedTimestamp(
            table.properties().containsKey(getCanonicalFieldName("lastModifiedTime"))
                ? Long.parseLong(table.properties().get(getCanonicalFieldName("lastModifiedTime")))
                : 0)
        .tableUUID(table.properties().get(getCanonicalFieldName("tableUUID")))
        .tableLocation(table.location())
        .sharingEnabled(policyStats.getSharingEnabled())
        .retentionPolicies(policyStats.getRetentionPolicy())
        .build();
  }

  /**
   * Get all manifest files (currently referenced or part of older snapshot) count depending on
   * metadata type to query.
   */
  private static long getManifestFilesCount(
      Table table, SparkSession spark, MetadataTableType metadataTableType) {
    return SparkTableUtil.loadMetadataTable(spark, table, metadataTableType)
        .selectExpr(new String[] {"path", "length"})
        .dropDuplicates("path", "length")
        .count();
  }

  /**
   * Return summary of table files content either from all snapshots or current snapshot depending
   * on metadataTableType.
   */
  private static Map<Integer, FilesSummary> getFileMetadataTable(
      Table table, SparkSession spark, MetadataTableType metadataTableType) {
    Encoder<FilesSummary> dataFilesSummaryEncoder = FilesSummary.getEncoder();
    Map<Integer, FilesSummary> result = new HashMap<>();
    SparkTableUtil.loadMetadataTable(spark, table, metadataTableType)
        .select("content", "file_path", "file_size_in_bytes")
        .dropDuplicates()
        .groupBy("content")
        .agg(count("*").as("totalFileCount"), sum("file_size_in_bytes").as("sumOfFileSizeBytes"))
        .as(dataFilesSummaryEncoder)
        .collectAsList()
        .forEach(
            row -> {
              int content = row.getContent();
              long totalSizeBytes = row.getSumOfFileSizeBytes();
              long fileCount = row.getTotalFileCount();
              result.put(content, new FilesSummary(content, totalSizeBytes, fileCount));
            });
    return result;
  }

  private static String getEarliestPartitionDate(
      Table table, SparkSession spark, RetentionStatsSchema retentionStatsSchema) {

    // Check if retention policy is present by checking if granularity exists
    if (retentionStatsSchema.getGranularity() == null) {
      return null;
    }
    String partitionColumnName =
        retentionStatsSchema.getColumnName() != null
            ? retentionStatsSchema.getColumnName()
            : getPartitionColumnName(table);
    // Table has no partition, we need to avoid AnalysisException
    if (partitionColumnName == null) {
      return null;
    }

    Dataset<Row> partitionData =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.PARTITIONS);
    String partitionColumn = String.format("partition.%s", partitionColumnName);

    if (partitionData.isEmpty()) {
      return null;
    }

    Row firstRow =
        partitionData.select(partitionColumn).orderBy(functions.asc(partitionColumn)).first();

    return firstRow != null ? firstRow.get(0).toString() : null;
  }

  private static String getPartitionColumnName(Table table) {
    return table.spec().partitionType().fields().stream()
        .filter(field -> field.type() instanceof Types.DateType)
        .map(Types.NestedField::name)
        .findFirst()
        .orElse(null);
  }

  private static PolicyStats getTablePolicies(Table table) {
    String policies = table.properties().get("policies");
    JsonObject policiesObject = new Gson().fromJson(policies, JsonObject.class);
    return convertObjectToPolicyStats(policiesObject);
  }

  private static PolicyStats convertObjectToPolicyStats(JsonObject jsonObject) {
    PolicyStats policyStats = new PolicyStats();
    // Set defaults
    RetentionStatsSchema defaultRetentionPolicy = RetentionStatsSchema.builder().count(0).build();
    policyStats.setRetentionPolicy(defaultRetentionPolicy);
    HistoryPolicyStatsSchema defaultHistoryPolicy =
        HistoryPolicyStatsSchema.builder()
            .maxAge(3)
            .granularity(TimePartitionSpec.GranularityEnum.DAY.toString())
            .versions(0)
            .build();
    policyStats.setHistoryPolicy(defaultHistoryPolicy);
    policyStats.setSharingEnabled(false);
    if (jsonObject == null) {
      return policyStats;
    }
    if (jsonObject.has("retention")) {
      GsonBuilder gsonBuilder = new GsonBuilder();
      gsonBuilder.registerTypeAdapter(
          RetentionStatsSchema.class, new RetentionStatsSchema.RetentionPolicyDeserializer());
      RetentionStatsSchema retentionPolicyStats =
          gsonBuilder.create().fromJson(jsonObject.get("retention"), RetentionStatsSchema.class);
      policyStats.setRetentionPolicy(retentionPolicyStats);
    }
    if (jsonObject.has("history")) {
      Gson gson = new Gson();
      HistoryPolicyStatsSchema historyPolicyStats =
          gson.fromJson(jsonObject.get("history"), HistoryPolicyStatsSchema.class);
      policyStats.setHistoryPolicy(historyPolicyStats);
    }
    if (jsonObject.has("sharingEnabled")) {
      policyStats.setSharingEnabled(jsonObject.get("sharingEnabled").getAsBoolean());
    }
    return policyStats;
  }

  /**
   * Populate commit event table data from snapshots metadata table.
   *
   * <p><b>Stateless Implementation:</b> Queries ALL non-expired snapshots from the Iceberg
   * snapshots metadata table on every job run, resulting in duplicates across runs. Downstream
   * consumers handle deduplication at query time.
   *
   * <p><b>Behavior:</b>
   *
   * <ul>
   *   <li>Queries: All active snapshots from {@code table.snapshots}
   *   <li>Collects: Every snapshot every time (no tracking of previous runs)
   *   <li>Result: Same {@code commit_id} appears in multiple partitions with different event
   *       timestamps
   *   <li>Deduplication: Downstream consumers use {@code GROUP BY commit_id} or {@code DISTINCT}
   * </ul>
   *
   * <p><b>Example Timeline:</b>
   *
   * <pre>
   * Day 1 12:00 - Snapshot A created
   * Day 1 18:00 - Job Run 1: Collects Snapshot A (event_timestamp = Day 1 18:00)
   * Day 2 18:00 - Job Run 2: Collects Snapshot A again (event_timestamp = Day 2 18:00)
   * Day 3 18:00 - Job Run 3: Collects Snapshot A again (event_timestamp = Day 3 18:00)
   * Day N       - Continues until Snapshot A expires via Iceberg retention
   * </pre>
   *
   * <p><b>Rationale:</b> "One duplicate == 100 duplicates" when querying. Downstream already needs
   * deduplication for late-arriving data and reprocessing.
   *
   * @param table Iceberg table instance
   * @param spark SparkSession
   * @return List of CommitEventTable objects (event_timestamp_ms will be set at publish time)
   */
  public static List<CommitEventTable> populateCommitEventTable(Table table, SparkSession spark) {
    String fullTableName = table.name();
    log.info("Collecting commit events for table: {} (all non-expired snapshots)", fullTableName);

    // Parse table name components
    String dbName = getDatabaseName(fullTableName);
    if (dbName == null) {
      return Collections.emptyList();
    }

    TableIdentifier identifier = TableIdentifier.parse(fullTableName);
    String tableName = identifier.name();
    String clusterName = getClusterName(spark);

    // Query all snapshots from Iceberg metadata table (no time filtering)
    String snapshotsQuery =
        String.format(
            "SELECT snapshot_id, committed_at, parent_id, operation, summary "
                + "FROM %s.snapshots",
            table.name());

    log.info("Executing snapshots query: {}", snapshotsQuery);
    Dataset<Row> snapshotsDF = spark.sql(snapshotsQuery);

    // Cache BEFORE first action to materialize during count() and reuse for collection
    snapshotsDF.cache();

    // This count() triggers cache materialization (single metadata scan)
    long totalSnapshots = snapshotsDF.count();

    if (totalSnapshots == 0) {
      log.info("No snapshots found for table: {}", fullTableName);
      snapshotsDF.unpersist(); // Clean up even though empty
      return Collections.emptyList();
    }

    log.info("Found {} snapshots for table: {}", totalSnapshots, fullTableName);

    // Get partition spec string representation
    String partitionSpec = table.spec().toString();

    // Get table location
    String tableMetadataLocation = table.location();

    // Use Spark encoder pattern for automatic DataFrame-to-object deserialization
    Encoder<CommitEventTable> commitEventEncoder = Encoders.bean(CommitEventTable.class);

    // Transform to nested structure matching CommitEventTable schema
    List<CommitEventTable> commitEventTableList =
        snapshotsDF
            .select(
                functions
                    .struct(
                        functions.lit(dbName).as("databaseName"),
                        functions.lit(tableName).as("tableName"),
                        functions.lit(clusterName).as("clusterName"),
                        functions.lit(tableMetadataLocation).as("tableMetadataLocation"),
                        functions.lit(partitionSpec).as("partitionSpec"))
                    .as("dataset"),
                functions
                    .struct(
                        functions.col("snapshot_id").cast("long").as("commitId"),
                        functions
                            .col("committed_at")
                            .cast("long")
                            .multiply(1000)
                            .as("commitTimestampMs"),
                        functions.col("summary").getItem("spark.app.id").as("commitAppId"),
                        functions.col("summary").getItem("spark.app.name").as("commitAppName"),
                        functions.upper(functions.col("operation")).as("commitOperation"))
                    .as("commitMetadata"),
                functions.lit(0L).as("eventTimestampMs"))
            .orderBy(functions.col("commitMetadata.commitTimestampMs"))
            .as(commitEventEncoder)
            .collectAsList();

    log.info("Collected {} commit events for table: {}", totalSnapshots, fullTableName);

    // Unpersist cached data to free memory
    snapshotsDF.unpersist();

    return commitEventTableList;
  }

  /**
   * Builds an enriched DataFrame containing partition data joined with commit metadata.
   *
   * <p>This shared helper method queries Iceberg metadata tables (all_entries and snapshots) and
   * creates a DataFrame with partition information enriched with commit metadata. The resulting
   * DataFrame can be used by both partition events and partition stats collection methods.
   *
   * <p><b>Output Schema:</b>
   *
   * <ul>
   *   <li>snapshot_id: long - Iceberg snapshot ID
   *   <li>committed_at: long - Commit timestamp in epoch milliseconds
   *   <li>operation: string - Commit operation (append, overwrite, delete, etc.)
   *   <li>summary: map&lt;string,string&gt; - Commit summary metadata
   *   <li>partition: struct - Partition column values as a struct
   * </ul>
   *
   * <p><b>For unpartitioned tables:</b> Returns null to indicate no partition data available.
   *
   * <p><b>Visibility:</b> Package-private for testing purposes.
   *
   * @param table Iceberg Table
   * @param spark SparkSession
   * @return DataFrame with enriched partition and commit data, or null if unpartitioned/empty
   */
  static Dataset<Row> buildEnrichedPartitionDataFrame(Table table, SparkSession spark) {
    String fullTableName = table.name();

    // Check if table is partitioned
    PartitionSpec spec = table.spec();
    if (spec.isUnpartitioned()) {
      log.info("Table {} is unpartitioned, no enriched partition data to build", fullTableName);
      return null;
    }

    // Query all_entries metadata table for partitions per commit
    // Use DISTINCT to deduplicate (snapshot_id, partition) pairs
    // No status filter - captures all affected partitions (ADDED or DELETED files)
    String allEntriesQuery =
        String.format(
            "SELECT DISTINCT snapshot_id, data_file.partition " + "FROM %s.all_entries",
            table.name());

    log.info("Executing all_entries query for table {}: {}", fullTableName, allEntriesQuery);
    Dataset<Row> partitionsPerCommitDF = spark.sql(allEntriesQuery);

    // Cache for reuse
    partitionsPerCommitDF.cache();
    long totalRecords = partitionsPerCommitDF.count();

    if (totalRecords == 0) {
      log.info("No partition-level records found for table: {}", fullTableName);
      partitionsPerCommitDF.unpersist();
      return null;
    }

    log.info("Found {} partition-level records for table: {}", totalRecords, fullTableName);

    // Query snapshots to get commit metadata
    String snapshotsQuery =
        String.format(
            "SELECT snapshot_id, committed_at, operation, summary " + "FROM %s.snapshots",
            table.name());

    Dataset<Row> snapshotsDF = spark.sql(snapshotsQuery);

    // Join partitions with commit metadata
    Dataset<Row> enrichedDF =
        partitionsPerCommitDF
            .join(snapshotsDF, "snapshot_id")
            .select(
                functions.col("snapshot_id"),
                functions.col("committed_at").cast("long"), // Cast timestamp to epoch seconds
                functions.col("operation"),
                functions.col("summary"),
                functions.col("partition")); // Keep partition struct for transformation

    // Important: unpersist the cached DF after creating enrichedDF
    partitionsPerCommitDF.unpersist();

    return enrichedDF;
  }

  /**
   * Collect partition-level commit events for a table.
   *
   * <p>For each commit, identifies all affected partitions and creates one
   * CommitEventTablePartitions record per (commit_id, partition) pair.
   *
   * <p>Uses Row API pattern: Query in Spark, collect to driver, transform in Java with full type
   * safety. This matches the existing populateCommitEventTable() pattern.
   *
   * <p><b>Behavior:</b>
   *
   * <ul>
   *   <li>Unpartitioned tables → Returns empty list
   *   <li>Queries all_entries metadata table for affected partitions
   *   <li>Joins with snapshots to get commit metadata
   *   <li>Transforms partition values to typed ColumnData objects
   * </ul>
   *
   * @param table Iceberg table instance
   * @param spark SparkSession
   * @return List of CommitEventTablePartitions objects (one per commit-partition pair)
   */
  public static List<CommitEventTablePartitions> populateCommitEventTablePartitions(
      Table table, SparkSession spark) {

    String fullTableName = table.name();
    log.info("Collecting partition-level commit events for table: {}", fullTableName);

    // Step 1: Build enriched DataFrame with partition and commit data using shared helper
    Dataset<Row> enrichedDF = buildEnrichedPartitionDataFrame(table, spark);

    // Check if any data was found
    if (enrichedDF == null) {
      log.info("No partition-level commit events found for table: {}", fullTableName);
      return Collections.emptyList();
    }

    // Step 2: Parse table name components for transformation
    PartitionSpec spec = table.spec();
    String dbName = getDatabaseName(fullTableName);
    if (dbName == null) {
      return Collections.emptyList();
    }

    TableIdentifier identifier = TableIdentifier.parse(fullTableName);
    String tableName = identifier.name();
    String clusterName = getClusterName(spark);
    String tableMetadataLocation = table.location();
    String partitionSpecString = spec.toString();

    // Extract partition column names from spec
    List<String> partitionColumnNames =
        spec.fields().stream().map(f -> f.name()).collect(Collectors.toList());

    // Step 3: Collect to driver and transform in Java with type safety
    // This matches populateCommitEventTable() pattern which also uses collectAsList()
    // Size is manageable: typically 100K rows × 200 bytes = 20MB

    // Cache BEFORE first action to materialize during count() and reuse for collection
    enrichedDF.cache();

    // This count() triggers cache materialization (single join execution)
    long totalRecords = enrichedDF.count();

    log.info("Collecting {} rows to driver for transformation", totalRecords);
    List<Row> rows = enrichedDF.collectAsList();

    // Step 4: Delegate transformation to helper method
    // Separated for testability and readability
    List<CommitEventTablePartitions> result =
        transformRowsToPartitionEvents(
            rows,
            dbName,
            tableName,
            clusterName,
            tableMetadataLocation,
            partitionSpecString,
            partitionColumnNames);

    log.info(
        "Collected {} partition-level commit events for table: {}", result.size(), fullTableName);

    // Unpersist cached data to free memory
    enrichedDF.unpersist();

    return result;
  }

  /**
   * Transform Spark rows to CommitEventTablePartitions objects.
   *
   * <p>This is a pure transformation method that converts raw Spark rows into domain objects.
   * Separated from query logic for better testability and maintainability.
   *
   * <p><b>Visibility:</b> Package-private for testing purposes.
   *
   * @param rows Spark rows containing commit and partition data
   * @param dbName Database name
   * @param tableName Table name
   * @param clusterName Cluster name
   * @param tableMetadataLocation Table metadata location
   * @param partitionSpecString Partition spec as string
   * @param partitionColumnNames List of partition column names (in spec order)
   * @return List of CommitEventTablePartitions objects
   */
  static List<CommitEventTablePartitions> transformRowsToPartitionEvents(
      List<Row> rows,
      String dbName,
      String tableName,
      String clusterName,
      String tableMetadataLocation,
      String partitionSpecString,
      List<String> partitionColumnNames) {

    List<CommitEventTablePartitions> result = new ArrayList<>();

    for (Row row : rows) {
      try {
        // Extract commit metadata
        long snapshotId = row.getAs("snapshot_id");
        long committedAtSeconds = row.getAs("committed_at");
        long committedAtMs = committedAtSeconds * 1000L;
        String operation = row.getAs("operation");

        // Convert operation string to CommitOperation enum
        CommitOperation commitOperation = null;
        if (operation != null) {
          try {
            commitOperation = CommitOperation.valueOf(operation.toUpperCase());
          } catch (IllegalArgumentException e) {
            log.warn("Unknown commit operation: {}, setting to null", operation);
          }
        }

        // Extract summary map (convert from Scala to Java)
        scala.collection.immutable.Map<String, String> scalaMap = row.getAs("summary");
        Map<String, String> summary = JavaConverters.mapAsJavaMap(scalaMap);

        // Extract partition struct and transform to ColumnData
        Row partitionRow = row.getAs("partition");
        List<ColumnData> partitionData =
            transformPartitionRowToColumnData(partitionRow, partitionColumnNames);

        // Build CommitEventTablePartitions object using builder pattern
        CommitEventTablePartitions event =
            CommitEventTablePartitions.builder()
                .dataset(
                    BaseEventModels.BaseTableIdentifier.builder()
                        .databaseName(dbName)
                        .tableName(tableName)
                        .clusterName(clusterName)
                        .tableMetadataLocation(tableMetadataLocation)
                        .partitionSpec(partitionSpecString)
                        .build())
                .commitMetadata(
                    CommitMetadata.builder()
                        .commitId(snapshotId)
                        .commitTimestampMs(committedAtMs)
                        .commitAppId(summary.get("spark.app.id"))
                        .commitAppName(summary.get("spark.app.name"))
                        .commitOperation(commitOperation)
                        .build())
                .partitionData(partitionData)
                .eventTimestampMs(0L) // Will be set at publish time
                .build();

        result.add(event);

      } catch (Exception e) {
        log.error("Failed to transform row to CommitEventTablePartitions: {}", row, e);
        // Continue processing other rows (don't fail entire batch)
      }
    }

    return result;
  }

  /**
   * Transform Iceberg partition Row to List of ColumnData with full type safety.
   *
   * <p>This method handles different partition column types and creates the appropriate ColumnData
   * subclass for each value. Uses instanceof checks for type safety.
   *
   * <p>Supported types:
   *
   * <ul>
   *   <li>Integer/Long → LongColumnData
   *   <li>Float/Double → DoubleColumnData
   *   <li>String/Date/Timestamp/Others → StringColumnData
   * </ul>
   *
   * <p><b>Visibility:</b> Package-private for testing purposes.
   *
   * @param partitionRow Spark Row containing partition column values
   * @param columnNames List of partition column names (in spec order)
   * @return List of ColumnData with typed values
   */
  static List<ColumnData> transformPartitionRowToColumnData(
      Row partitionRow, List<String> columnNames) {

    List<ColumnData> result = new ArrayList<>();

    for (int i = 0; i < columnNames.size(); i++) {
      String colName = columnNames.get(i);
      Object value = partitionRow.get(i);

      if (value == null) {
        // Skip null partition values (shouldn't happen in valid Iceberg data)
        log.warn("Null partition value for column: {}", colName);
        continue;
      }

      // Determine type and create appropriate ColumnData
      // Order matters: check more specific types first
      if (value instanceof Long) {
        result.add(new ColumnData.LongColumnData(colName, (Long) value));
      } else if (value instanceof Integer) {
        result.add(new ColumnData.LongColumnData(colName, ((Integer) value).longValue()));
      } else if (value instanceof Double) {
        result.add(new ColumnData.DoubleColumnData(colName, (Double) value));
      } else if (value instanceof Float) {
        result.add(new ColumnData.DoubleColumnData(colName, ((Float) value).doubleValue()));
      } else {
        // Default: treat as string (handles String, Date, Timestamp, etc.)
        result.add(new ColumnData.StringColumnData(colName, value.toString()));
      }
    }

    return result;
  }

  /**
   * Extract database name from fully-qualified table name.
   *
   * <p>Safely parses FQTN using Iceberg's TableIdentifier and returns the database name (last
   * namespace component).
   *
   * @param fqtn Fully-qualified table name (e.g., "db.table" or "catalog.db.table")
   * @return Database name, or null if invalid format
   */
  static String getDatabaseName(String fqtn) {
    try {
      TableIdentifier identifier = TableIdentifier.parse(fqtn);
      String[] namespaceParts = identifier.namespace().levels();

      if (namespaceParts.length == 0) {
        log.error(
            "Invalid table identifier: {}. Expected database.table or catalog.database.table",
            fqtn);
        return null;
      }

      // Database name is the last part of the namespace (e.g., "db" from "openhouse.db.table")
      return namespaceParts[namespaceParts.length - 1];
    } catch (Exception e) {
      log.error("Failed to parse table identifier: {}", fqtn, e);
      return null;
    }
  }

  /**
   * Get cluster name from Spark configuration.
   *
   * @param spark SparkSession
   * @return Cluster name, defaults to "default" if not found or error occurs
   */
  private static String getClusterName(SparkSession spark) {
    try {
      String clusterName = spark.conf().get("spark.sql.catalog.openhouse.cluster", null);
      if (clusterName != null) {
        return clusterName;
      }
    } catch (Exception e) {
      log.warn("Failed to get cluster name from Spark configuration", e);
    }
    return "default";
  }
}
