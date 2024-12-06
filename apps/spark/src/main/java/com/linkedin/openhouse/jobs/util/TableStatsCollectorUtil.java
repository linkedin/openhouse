package com.linkedin.openhouse.jobs.util;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.*;
import static org.apache.spark.sql.functions.*;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

    // TODO: turn these longs into attributes of a class and use the class as a schema which spark
    // will use as an
    // encoding when serializing the table into the spark dataframe
    Dataset<Row> allDataFiles = getFileMetadataTable(table, spark, MetadataTableType.ALL_FILES);

    long countOfDataFiles = getFileMetric(allDataFiles, 0, "file_count");
    long sumOfDataFileSizeBytes = getFileMetric(allDataFiles, 0, "total_size_bytes");

    long countOfPositionDeleteFiles = getFileMetric(allDataFiles, 1, "file_count");
    long sumOfPositionDeleteFileSizeBytes = getFileMetric(allDataFiles, 1, "total_size_bytes");

    long countOfEqualityDeleteFiles = getFileMetric(allDataFiles, 2, "file_count");
    long sumOfEqualityDeleteFilesSizeBytes = getFileMetric(allDataFiles, 2, "total_size_bytes");

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

    // TODO: turn these longs into attributes of a class and use the class as a schema which spark
    // will use as an
    //  encoding when serializing the table into the spark dataframe
    Dataset<Row> currentSnapshotDataFiles =
        getFileMetadataTable(table, spark, MetadataTableType.FILES);

    long countOfDataFiles = getFileMetric(currentSnapshotDataFiles, 0, "file_count");
    long sumOfDataFileSizeBytes = getFileMetric(currentSnapshotDataFiles, 0, "total_size_bytes");

    long countOfPositionDeleteFiles = getFileMetric(currentSnapshotDataFiles, 1, "file_count");
    long sumOfPositionDeleteFileSizeBytes =
        getFileMetric(currentSnapshotDataFiles, 1, "total_size_bytes");

    long countOfEqualityDeleteFiles = getFileMetric(currentSnapshotDataFiles, 2, "file_count");
    long sumOfEqualityDeleteFilesSizeBytes =
        getFileMetric(currentSnapshotDataFiles, 2, "total_size_bytes");

    Long currentSnapshotId =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId).orElse(null);

    Long currentSnapshotTimestamp =
        Optional.ofNullable(table.currentSnapshot()).map(Snapshot::timestampMillis).orElse(null);

    String earliestPartitionDate = getEarliestPartitionDate(table, spark, getTablePolicies(table));

    log.info(
        "Table: {}, Count of total Data files: {}, Sum of file sizes in bytes: {}, Earliest partition date: {}, for snapshot: {}",
        fqtn,
        countOfDataFiles,
        sumOfDataFileSizeBytes,
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
        .totalCurrentSnapshotReferencedDataFilesSizeInBytes(sumOfDataFileSizeBytes)
        .numCurrentSnapshotPositionDeleteFiles(countOfPositionDeleteFiles)
        .totalCurrentSnapshotPositionDeleteFileSizeInBytes(sumOfPositionDeleteFileSizeBytes)
        .numCurrentSnapshotEqualityDeleteFiles(countOfEqualityDeleteFiles)
        .totalCurrentSnapshotEqualityDeleteFileSizeInBytes(sumOfEqualityDeleteFilesSizeBytes)
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
  private static Dataset<Row> getFileMetadataTable(
      Table table, SparkSession spark, MetadataTableType metadataTableType) {
    return SparkTableUtil.loadMetadataTable(spark, table, metadataTableType)
        .groupBy("content")
        .agg(count("*").as("file_count"), sum("file_size_in_bytes").as("total_size_bytes"));
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

  private static Dataset<Row> filterByContentType(Dataset<Row> dataset, int contentType) {
    return dataset.filter("content = " + contentType);
  }

  private static long getFileMetric(Dataset<Row> dataFiles, int contentType, String columnName) {
    Dataset<Row> contentDataFiles = filterByContentType(dataFiles, contentType);
    if (contentDataFiles.isEmpty()) {
      return 0;
    }
    return contentDataFiles.select(columnName).first().getLong(0);
  }
}
