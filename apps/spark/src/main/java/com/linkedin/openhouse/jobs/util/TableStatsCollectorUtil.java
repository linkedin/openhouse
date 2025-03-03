package com.linkedin.openhouse.jobs.util;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static org.apache.spark.sql.functions.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.stats.model.HistoryPolicyStatsSchema;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.common.stats.model.PolicyStats;
import com.linkedin.openhouse.common.stats.model.RetentionStatsSchema;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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

    // Find minimum timestamp of all snapshots where snapshots is iterator
    Long oldestSnapshotTimestamp =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(Snapshot::timestampMillis)
            .min(Long::compareTo)
            .orElse(null);

    Long numSnapshots = StreamSupport.stream(table.snapshots().spliterator(), false).count();

    return stats
        .toBuilder()
        .currentSnapshotId(currentSnapshotId)
        .currentSnapshotTimestamp(currentSnapshotTimestamp)
        .oldestSnapshotTimestamp(oldestSnapshotTimestamp)
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

    if (retentionStatsSchema == null) {
      return null;
    }
    String partitionColumnName =
        retentionStatsSchema.getColumnName() != null
            ? retentionStatsSchema.getColumnName()
            : getPartitionColumnName(table);

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
    HistoryPolicyStatsSchema defaultHistoryPolicy =
        HistoryPolicyStatsSchema.builder().maxAge(3).granularity("DAYS").versions(0).build();
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
}
