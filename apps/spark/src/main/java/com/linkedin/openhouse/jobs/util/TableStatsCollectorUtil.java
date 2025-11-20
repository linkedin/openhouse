package com.linkedin.openhouse.jobs.util;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static org.apache.spark.sql.functions.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.stats.model.BaseEventModels;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitMetadata;
import com.linkedin.openhouse.common.stats.model.CommitOperation;
import com.linkedin.openhouse.common.stats.model.HistoryPolicyStatsSchema;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.common.stats.model.PolicyStats;
import com.linkedin.openhouse.common.stats.model.RetentionStatsSchema;
import com.linkedin.openhouse.tables.client.model.TimePartitionSpec;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/** Utility class to collect stats for a given table. */
@Slf4j
public final class TableStatsCollectorUtil {

  private TableStatsCollectorUtil() {}
  /** Collect stats about referenced files in a given table. */
  public static IcebergTableStats populateStatsOfAllReferencedFiles(
      String fqtn, Table table, SparkSession spark, IcebergTableStats stats) {
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
        fqtn,
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
      String fqtn, Table table, SparkSession spark, IcebergTableStats stats) {

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
        fqtn,
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

  /** Collect storage stats for a given fully-qualified table name. */
  public static IcebergTableStats populateStorageStats(
      String fqtn, Table table, FileSystem fs, IcebergTableStats stats) {
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
      log.error("Error while listing files in HDFS directory for table: {}", fqtn, e);
      return stats;
    }

    log.info(
        "Table: {}, Count of objects in HDFS directory: {}, Sum of file sizes in bytes on HDFS: {}",
        fqtn,
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
   * @param fqtn fully-qualified table name (format: "database.table")
   * @param table Iceberg table instance
   * @param spark SparkSession
   * @return List of CommitEventTable objects (event_timestamp_ms will be set at publish time)
   */
  public static List<CommitEventTable> populateCommitEventTable(
      String fqtn, Table table, SparkSession spark) {
    log.info("Collecting commit events for table: {} (all non-expired snapshots)", fqtn);

    String[] fqtnParts = fqtn.split("\\.", 2);
    if (fqtnParts.length != 2) {
      log.error("Invalid FQTN format: {}", fqtn);
      return new ArrayList<>();
    }
    String dbName = fqtnParts[0];
    String tableName = fqtnParts[1];
    String clusterName = getClusterName(spark);

    // Query all snapshots from Iceberg metadata table (no time filtering)
    String snapshotsQuery =
        String.format(
            "SELECT snapshot_id, committed_at, parent_id, operation, summary "
                + "FROM %s.snapshots",
            fqtn);

    log.info("Executing snapshots query: {}", snapshotsQuery);
    Dataset<Row> snapshotsDF = spark.sql(snapshotsQuery);

    if (snapshotsDF.isEmpty()) {
      log.info("No snapshots found for table: {}", fqtn);
      return new ArrayList<>();
    }

    long totalSnapshots = snapshotsDF.count();
    log.info("Found {} snapshots for table: {}", totalSnapshots, fqtn);

    // Get partition spec string representation
    String partitionSpec = table.spec().toString();

    // Get table location
    String tableMetadataLocation = table.location();

    // Transform to commit events schema
    Dataset<Row> commitEventsDF =
        snapshotsDF
            .withColumn("database_name", functions.lit(dbName))
            .withColumn("table_name", functions.lit(tableName))
            .withColumn("cluster_name", functions.lit(clusterName))
            .withColumn("table_metadata_location", functions.lit(tableMetadataLocation))
            .withColumn("partition_spec", functions.lit(partitionSpec))
            .withColumn("commit_id", functions.col("snapshot_id").cast("string"))
            .withColumn(
                "commit_timestamp_ms", functions.col("committed_at").cast("long").multiply(1000))
            .withColumn("commit_app_id", functions.col("summary").getItem("spark.app.id"))
            .withColumn("commit_app_name", functions.col("summary").getItem("spark.app.name"))
            .withColumn("commit_operation", functions.col("operation"))
            .select(
                "database_name",
                "table_name",
                "cluster_name",
                "table_metadata_location",
                "partition_spec",
                "commit_id",
                "commit_timestamp_ms",
                "commit_app_id",
                "commit_app_name",
                "commit_operation")
            .orderBy("commit_timestamp_ms");

    log.info("Collected {} commit events for table: {}", totalSnapshots, fqtn);

    // Convert DataFrame to List<CommitEventTable>
    return convertToCommitEventTableList(commitEventsDF);
  }

  /**
   * Get cluster name from Spark configuration.
   *
   * @param spark SparkSession
   * @return Cluster name
   */
  private static String getClusterName(SparkSession spark) {
    try {
      String clusterName = spark.conf().get("spark.sql.catalog.openhouse.cluster", null);
      if (clusterName != null) {
        return clusterName;
      }
      return "default";
    } catch (Exception e) {
      log.warn("Failed to get cluster name from Spark configuration", e);
      return "default";
    }
  }

  /**
   * Convert Dataset<Row> to List<CommitEventTable>.
   *
   * <p>Note: Loads data into memory. This is acceptable because Iceberg retention limits active
   * snapshots to a manageable number (typically <10k per table).
   *
   * @param commitEventsDF DataFrame with commit events (without event_timestamp_ms)
   * @return List of CommitEventTable objects
   */
  private static List<CommitEventTable> convertToCommitEventTableList(Dataset<Row> commitEventsDF) {
    if (commitEventsDF == null || commitEventsDF.isEmpty()) {
      return new ArrayList<>();
    }

    return commitEventsDF.collectAsList().stream()
        .map(TableStatsCollectorUtil::rowToCommitEventTable)
        .collect(Collectors.toList());
  }

  /**
   * Convert a single Row to CommitEventTable.
   *
   * @param row DataFrame row containing commit event data
   * @return CommitEventTable object
   */
  private static CommitEventTable rowToCommitEventTable(Row row) {
    BaseEventModels.BaseTableIdentifier dataset =
        BaseEventModels.BaseTableIdentifier.builder()
            .databaseName(row.getAs("database_name"))
            .tableName(row.getAs("table_name"))
            .clusterName(row.getAs("cluster_name"))
            .tableMetadataLocation(row.getAs("table_metadata_location"))
            .partitionSpec(row.getAs("partition_spec"))
            .build();

    CommitMetadata commitMetadata =
        CommitMetadata.builder()
            .commitId(Long.parseLong(row.getAs("commit_id")))
            .commitTimestampMs(row.getAs("commit_timestamp_ms"))
            .commitAppId(row.getAs("commit_app_id"))
            .commitAppName(row.getAs("commit_app_name"))
            .commitOperation(parseCommitOperation(row.getAs("commit_operation")))
            .build();

    return CommitEventTable.builder()
        .dataset(dataset)
        .commitMetadata(commitMetadata)
        .eventTimestampMs(0L) // Placeholder - will be set at publish time
        .build();
  }

  /**
   * Parse commit operation string to CommitOperation enum.
   *
   * @param operation Operation string from Iceberg (e.g., "append", "overwrite")
   * @return CommitOperation enum value, or null if parsing fails
   */
  private static CommitOperation parseCommitOperation(String operation) {
    if (operation == null) {
      return null;
    }
    try {
      return CommitOperation.valueOf(operation.toUpperCase());
    } catch (IllegalArgumentException e) {
      log.warn("Unknown commit operation: {}", operation);
      return null;
    }
  }
}
