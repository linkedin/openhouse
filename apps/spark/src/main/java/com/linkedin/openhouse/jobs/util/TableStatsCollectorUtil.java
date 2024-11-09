package com.linkedin.openhouse.jobs.util;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.common.stats.model.RetentionStatsSchema;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
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

    Dataset<Row> allDataFiles =
        getAllDataFilesCount(table, spark, MetadataTableType.ALL_DATA_FILES);
    long countOfDataFiles = allDataFiles.count();

    // Calculate sum of file sizes in bytes in above allDataFiles
    long sumOfFileSizeBytes = getSumOfFileSizeBytes(allDataFiles);

    log.info(
        "Table: {}, Count of metadata files: {}, Manifest files: {}, Manifest list files: {}, Metadata files: {},"
            + "Data files: {}, Sum of file sizes in bytes: {}",
        fqtn,
        totalMetadataFilesCount,
        referencedManifestFilesCount,
        referencedManifestListFilesCount,
        metadataFilesCount,
        countOfDataFiles,
        sumOfFileSizeBytes);

    return stats
        .toBuilder()
        .numReferencedDataFiles(countOfDataFiles)
        .totalReferencedDataFilesSizeInBytes(sumOfFileSizeBytes)
        .numReferencedManifestFiles(referencedManifestFilesCount)
        .numReferencedManifestLists(referencedManifestListFilesCount)
        .numExistingMetadataJsonFiles(metadataFilesCount)
        .build();
  }

  /** Collect stats for snapshots of a given table. */
  public static IcebergTableStats populateStatsForSnapshots(
      String fqtn, Table table, SparkSession spark, IcebergTableStats stats) {

    Dataset<Row> dataFiles = getAllDataFilesCount(table, spark, MetadataTableType.FILES);
    long countOfDataFiles = dataFiles.count();

    // Calculate sum of file sizes in bytes in above allDataFiles
    long sumOfFileSizeBytes = getSumOfFileSizeBytes(dataFiles);

    Long currentSnapshotId =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null);

    Long currentSnapshotTimestamp =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::timestampMillis).orElse(null);

    String earliestPartitionDate = getEarliestPartitionDate(table, spark, getTablePolicies(table));

    log.info(
        "Table: {}, Count of total Data files: {}, Sum of file sizes in bytes: {}, Earliest partition date: {}, for snapshot: {}",
        fqtn,
        countOfDataFiles,
        sumOfFileSizeBytes,
        currentSnapshotId,
        earliestPartitionDate);

    // Find minimum timestamp of all snapshots where snapshots is iterator
    Long oldestSnapshotTimestamp =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .map(Snapshot::timestampMillis)
            .min(Long::compareTo)
            .orElse(null);

    return stats
        .toBuilder()
        .currentSnapshotId(currentSnapshotId)
        .currentSnapshotTimestamp(currentSnapshotTimestamp)
        .oldestSnapshotTimestamp(oldestSnapshotTimestamp)
        .numCurrentSnapshotReferencedDataFiles(countOfDataFiles)
        .totalCurrentSnapshotReferencedDataFilesSizeInBytes(sumOfFileSizeBytes)
        .earliestPartitionDate(earliestPartitionDate)
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
      ContentSummary contentSummary = fs.getContentSummary(new Path(table.location()));
      numOfObjectsInDirectory = contentSummary.getFileAndDirectoryCount();
      sumOfTotalDirectorySizeInBytes = contentSummary.getLength();
    } catch (IOException e) {
      log.error("Error while counting folders, files, and getting size of table: {}", fqtn, e);
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
    Map<String, Object> policyMap = getTablePolicies(table);
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
        .sharingEnabled(
            policyMap.containsKey("sharingEnabled") && (Boolean) policyMap.get("sharingEnabled"))
        .retentionPolicies(buildRetentionStats(policyMap))
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

  /** Get all data files count depending on metadata type to query. */
  private static Dataset<Row> getAllDataFilesCount(
      Table table, SparkSession spark, MetadataTableType metadataTableType) {
    return SparkTableUtil.loadMetadataTable(spark, table, metadataTableType)
        .selectExpr(new String[] {"file_path", "file_size_in_bytes"})
        .dropDuplicates("file_path", "file_size_in_bytes");
  }

  private static long getSumOfFileSizeBytes(Dataset<Row> allDataFiles) {
    if (allDataFiles.isEmpty()) {
      return 0;
    }

    return allDataFiles
        .agg(org.apache.spark.sql.functions.sum("file_size_in_bytes"))
        .first()
        .getLong(0);
  }

  private static String getEarliestPartitionDate(
      Table table, SparkSession spark, Map<String, Object> policyMap) {
    // Check if retention policy is present by checking if granularity exists
    if (!policyMap.containsKey("granularity")) {
      return null;
    }
    String partitionColumnName =
        policyMap.containsKey("columnName")
            ? (String) policyMap.get("columnName")
            : getPartitionColumnName(table);
    if (partitionColumnName == null) {
      return null;
    }

    Dataset<Row> partitionData =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.PARTITIONS);
    String partitionColumn = String.format("partition.%s", partitionColumnName);

    return partitionData
        .select(partitionColumn)
        .orderBy(functions.asc(partitionColumn))
        .first()
        .get(0)
        .toString();
  }

  private static String getPartitionColumnName(Table table) {
    return table.spec().partitionType().fields().stream()
        .filter(field -> field.type() instanceof Types.DateType)
        .map(Types.NestedField::name)
        .findFirst()
        .orElse(null);
  }

  private static Map<String, Object> getTablePolicies(Table table) {
    String policies = table.properties().get("policies");
    JsonObject policiesObject = new Gson().fromJson(policies, JsonObject.class);
    Map<String, Object> policyMap = new HashMap<>();

    if (policies.isEmpty()) {
      return policyMap;
    }
    if (policiesObject.get("retention") != null) {
      addEntriesToMap(policiesObject.getAsJsonObject("retention"), policyMap);
    }
    if (policiesObject.get("sharingEnabled") != null) {
      policyMap.put(
          "sharingEnabled", Boolean.valueOf(policiesObject.get("sharingEnabled").getAsString()));
    }

    return policyMap;
  }

  private static RetentionStatsSchema buildRetentionStats(Map<String, Object> retentionPolicy) {
    return RetentionStatsSchema.builder()
        .count(
            retentionPolicy.containsKey("count")
                ? Integer.parseInt((String) retentionPolicy.get("count"))
                : 0)
        .granularity((String) retentionPolicy.getOrDefault("granularity", null))
        .columnPattern((String) retentionPolicy.getOrDefault("pattern", null))
        .columnName((String) retentionPolicy.getOrDefault("columnName", null))
        .build();
  }

  private static void addEntriesToMap(JsonObject jsonObject, Map<String, Object> map) {
    Type type = new TypeToken<Map<String, Object>>() {}.getType();
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      if (entry.getValue().isJsonObject()) {
        map.putAll(new Gson().fromJson(entry.getValue().getAsJsonObject(), type));
      } else {
        map.put(entry.getKey(), entry.getValue().getAsString());
      }
    }
  }
}
