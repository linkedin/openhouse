package com.linkedin.openhouse.jobs.spark;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.SparkJobUtil;
import com.linkedin.openhouse.jobs.util.TableStatsCollector;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

/**
 * Utility class to hold table maintenance operations using Spark engine, and supporting methods.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class Operations implements AutoCloseable {
  // assume that catalog name is fixed
  private static final String CATALOG = "openhouse";

  private final SparkSession spark;

  private final OtelEmitter otelEmitter;

  public static Operations of(SparkSession spark, OtelEmitter otelEmitter) {
    return new Operations(spark, otelEmitter);
  }

  public static Operations withCatalog(SparkSession spark, OtelEmitter otelEmitter) {
    Operations ops = of(spark, otelEmitter);
    ops.useCatalog();
    return ops;
  }

  public SparkSession spark() {
    return spark;
  }

  /** Get default file system from Spark Hadoop configuration. */
  public FileSystem fs() throws IOException {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration());
  }

  public Table getTable(String fqtn) {
    Catalog catalog = getCatalog();
    return catalog.loadTable(TableIdentifier.parse(fqtn));
  }

  @VisibleForTesting
  protected Transaction createTransaction(String fqtn, Schema schema) {
    Catalog catalog = getCatalog();
    return catalog.buildTable(TableIdentifier.parse(fqtn), schema).createTransaction();
  }

  /**
   * Run DeleteOrphanFiles operation on the given table with time filter, moves data files to the
   * given backup directory if backup is enabled. It moves files older than the provided timestamp.
   */
  public DeleteOrphanFiles.Result deleteOrphanFiles(
      Table table,
      long olderThanTimestampMillis,
      boolean backupEnabled,
      String backupDir,
      int concurrentDeletes) {

    DeleteOrphanFiles operation = SparkActions.get(spark).deleteOrphanFiles(table);
    // if time filter is not provided it defaults to 3 days
    if (olderThanTimestampMillis > 0) {
      operation = operation.olderThan(olderThanTimestampMillis);
    }
    if (concurrentDeletes > 1) {
      operation = operation.executeDeleteWith(removeFilesService(concurrentDeletes));
    }
    Map<String, Boolean> dataManifestsCache = new ConcurrentHashMap<>();
    Path backupDirRoot = new Path(table.location(), backupDir);
    Path dataDirRoot = new Path(table.location(), "data");
    operation =
        operation.deleteWith(
            file -> {
              log.info("Detected orphan file {}", file);
              if (file.endsWith("metadata.json")) {
                // Don't remove metadata.json files since current metadata.json is recognized as
                // orphan because of inclusion of the scheme in its file path returned by catalog.
                // Also, we want Iceberg commits to remove the metadata.json files not the OFD job.
                log.info("Skipped deleting metadata file {}", file);
              } else if (file.contains(backupDirRoot.toString())) {
                // files present in .backup dir should not be considered orphan
                log.info("Skipped deleting backup file {}", file);
              } else if (file.contains(dataDirRoot.toString())
                  && backupEnabled
                  && isExistBackupDataManifests(table, file, backupDir, dataManifestsCache)) {
                // move data files to backup dir if backup is enabled
                Path backupFilePath = getTrashPath(table, file, backupDir);
                log.info("Moving orphan file {} to {}", file, backupFilePath);
                try {
                  rename(new Path(file), backupFilePath);
                  // update modification time to current time
                  fs().setTimes(backupFilePath, System.currentTimeMillis(), -1);
                } catch (IOException e) {
                  log.error(String.format("Move operation failed for file: %s", file), e);
                }
              } else {
                log.info("Deleting orphan file {}", file);
                try {
                  fs().delete(new Path(file), false);
                } catch (IOException e) {
                  log.error(String.format("Delete operation failed for file: %s", file), e);
                }
              }
            });
    return operation.execute();
  }

  private ExecutorService removeFilesService(int concurrentDeletes) {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                concurrentDeletes,
                new ThreadFactoryBuilder().setNameFormat("remove-orphans-%d").build()));
  }

  private boolean isExistBackupDataManifests(
      Table table, String file, String backupDir, Map<String, Boolean> dataManifestsCache) {
    try {
      Path backupPartition = getTrashPath(table, file, backupDir).getParent();
      if (dataManifestsCache.containsKey(backupPartition.toString())) {
        return dataManifestsCache.get(backupPartition.toString());
      }
      Path pattern = new Path(backupPartition, "data_manifest*");
      FileStatus[] matches = fs().globStatus(pattern);
      boolean isExist = matches != null && matches.length > 0;
      dataManifestsCache.put(backupPartition.toString(), isExist);
      return isExist;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Run deleteOrphanDirectory operation on the given table directory path with time filter, moves
   * files to the given trash subdirectory if the table is created older than the provided
   * timestamp.
   */
  public boolean deleteOrphanDirectory(
      Path tableDirectoryPath, String trashDir, long olderThanTimestampMillis) {
    List<Path> matchingFiles = Lists.newArrayList();
    listFiles(
        tableDirectoryPath,
        file -> !file.getPath().toString().contains(trashDir),
        true,
        matchingFiles);

    boolean anyMatched = false;
    // if there is one file that satisfy predicate, all files should go into trash
    try {
      FileSystem fileSystem = fs();
      for (Path matchingFile : matchingFiles) {
        FileStatus filestatus = fileSystem.getFileStatus(matchingFile);
        if (filestatus.getModificationTime() < olderThanTimestampMillis) {
          anyMatched = true;
          break;
        }
      }
    } catch (IOException e) {
      log.error("Error fetching the file system given path: {}", tableDirectoryPath);
    }

    if (!anyMatched) {
      return false;
    }

    for (Path matchingFile : matchingFiles) {
      Path trashPath =
          getTrashPath(tableDirectoryPath.toString(), matchingFile.toString(), trashDir);
      try {
        rename(matchingFile, trashPath);
      } catch (IOException e) {
        log.error(String.format("Move operation failed for file path: %s", tableDirectoryPath), e);
      }
    }
    return true;
  }

  /**
   * Remove staged orphan files in the staged trash directory and eventually delete the table
   * directory
   */
  public void deleteStagedOrphanDirectory(
      Path tableDirectoryPath, String trashDir, long olderThanTimestampMillis) {
    Path trashFolderPath = new Path(tableDirectoryPath, trashDir);
    try {
      deleteStagedFiles(trashFolderPath, olderThanTimestampMillis, true);
      if (!fs().delete(tableDirectoryPath, true)) {
        log.error(String.format("Failed to delete directory %s", tableDirectoryPath));
      }
    } catch (IOException e) {
      log.error(
          String.format("Delete staged files failed for orphan directory: %s", tableDirectoryPath),
          e);
    }
  }

  /** Expire snapshots on a given fully-qualified table name. */
  public void expireSnapshots(String fqtn, int maxAge, String granularity, int versions) {
    expireSnapshots(getTable(fqtn), maxAge, granularity, versions);
  }

  /**
   * Expire snapshots on a given {@link Table}. If maxAge is provided, it will expire snapshots
   * older than maxAge in granularity timeunit. If versions is provided, it will retain the last
   * versions snapshots. If both are provided, it will prioritize maxAge; only retain up to versions
   * number of snapshots younger than the maxAge
   */
  public void expireSnapshots(Table table, int maxAge, String granularity, int versions) {
    ExpireSnapshots expireSnapshotsCommand = table.expireSnapshots().cleanExpiredFiles(false);

    // maxAge will always be defined
    ChronoUnit timeUnitGranularity =
        ChronoUnit.valueOf(
            SparkJobUtil.convertGranularityToChrono(granularity.toUpperCase()).name());
    long expireBeforeTimestampMs =
        System.currentTimeMillis()
            - timeUnitGranularity.getDuration().multipliedBy(maxAge).toMillis();
    log.info("Expiring snapshots for table: {} older than {}ms", table, expireBeforeTimestampMs);
    expireSnapshotsCommand.expireOlderThan(expireBeforeTimestampMs).commit();

    if (versions > 0 && Iterators.size(table.snapshots().iterator()) > versions) {
      log.info("Expiring snapshots for table: {} retaining last {} versions", table, versions);
      // Note: retainLast keeps the last N snapshots that WOULD be expired, hence expireOlderThan
      // currentTime
      expireSnapshotsCommand
          .expireOlderThan(System.currentTimeMillis())
          .retainLast(versions)
          .commit();
    }
  }

  /**
   * Run table retention operation if there are rows with partition column (@columnName) value older
   * than @count @granularity.
   *
   * @param fqtn fully qualified tableName e.g test_db.test_table
   * @param columnName partition column based on which records are retained. Schema type
   *     of @columnName could be timestamp or String
   * @param columnPattern If schema type of @columnName is String @columnPattern is used to parse it
   *     to a timestamp. columnPattern should always represent a valid DateTimeFormat
   * @param granularity time granularity in Hour, Day, Month, Year
   * @param count granularity count representing retention timeline for @fqtn records
   * @param backupEnabled flag to indicate if backup manifests need to be created for data files
   * @param backupDir backup directory under which data manifests are created
   * @param now current timestamp to be used for retention calculation
   */
  public void runRetention(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      boolean backupEnabled,
      String backupDir,
      ZonedDateTime now) {
    if (backupEnabled) {
      // Cache of manifests: partitionPath -> list of data file path
      Map<String, List<String>> manifestCache =
          prepareBackupDataManifests(fqtn, columnName, columnPattern, granularity, count, now);
      writeBackupDataManifests(manifestCache, getTable(fqtn), backupDir, now);
      exposeBackupLocation(getTable(fqtn), backupDir);
    }
    final String statement =
        SparkJobUtil.createDeleteStatement(
            fqtn, columnName, columnPattern, granularity, count, now);
    log.info("deleting records from table: {}", fqtn);
    spark.sql(statement);
  }

  private Map<String, List<String>> prepareBackupDataManifests(
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      ZonedDateTime now) {
    Table table = getTable(fqtn);
    Expression filter =
        SparkJobUtil.createDeleteFilter(columnName, columnPattern, granularity, count, now);
    TableScan scan = table.newScan().filter(filter);
    try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
      List<FileScanTask> filesList = Lists.newArrayList(filesIterable);
      return filesList.stream()
          .collect(
              Collectors.groupingBy(
                  task -> {
                    String path = task.file().path().toString();
                    return Paths.get(path).getParent().toString();
                  },
                  Collectors.mapping(task -> task.file().path().toString(), Collectors.toList())));
    } catch (IOException e) {
      throw new RuntimeException("Failed to plan backup files for retention", e);
    }
  }

  private void writeBackupDataManifests(
      Map<String, List<String>> manifestCache, Table table, String backupDir, ZonedDateTime now) {
    for (String partitionPath : manifestCache.keySet()) {
      List<String> files = manifestCache.get(partitionPath);
      List<String> backupFiles =
          files.stream()
              .map(file -> getTrashPath(table, file, backupDir).toString())
              .collect(Collectors.toList());
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put("file_count", backupFiles.size());
      jsonMap.put("files", backupFiles);
      String jsonStr = new Gson().toJson(jsonMap);
      // Create data_manifest.json
      String manifestName = String.format("data_manifest_%d.json", now.toInstant().toEpochMilli());
      Path destPath =
          getTrashPath(table, new Path(partitionPath, manifestName).toString(), backupDir);
      try {
        final FileSystem fs = fs();
        if (!fs.exists(destPath.getParent())) {
          fs.mkdirs(destPath.getParent());
        }
        try (FSDataOutputStream out = fs.create(destPath, true)) {
          out.write(jsonStr.getBytes());
        }
        log.info("Wrote {} with {} backup files", destPath, backupFiles.size());
      } catch (IOException e) {
        throw new RuntimeException("Failed to write data_manifest.json", e);
      }
    }
  }

  private void exposeBackupLocation(Table table, String backupDir) {
    Path fullyQualifiedBackupDir = new Path(table.location(), backupDir);
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('%s'='%s')",
            table.name(), AppConstants.BACKUP_DIR_KEY, fullyQualifiedBackupDir));
  }

  private Path getTrashPath(String path, String filePath, String trashDir) {
    return new Path(filePath.replace(path, new Path(path, trashDir).toString()));
  }

  /**
   * Get trash path location for a file in a table. It replaces the tableLocation part from filePath
   * with trashPath which contains <tableLocation>+/.trash
   */
  public Path getTrashPath(Table table, String filePath, String trashDir) {
    return getTrashPath(table.location(), filePath, trashDir);
  }

  private List<Path> deleteStagedFiles(Path baseDir, long modTimeThreshold, boolean recursive)
      throws IOException {
    List<Path> matchingFiles = Lists.newArrayList();
    Predicate<FileStatus> predicate = file -> file.getModificationTime() < modTimeThreshold;
    FileSystem fs = fs();
    if (fs.exists(baseDir)) {
      listFiles(baseDir, predicate, recursive, matchingFiles);
      log.info(
          "Deleting {} files from {} that are older than modificationTimeThreshold {}",
          matchingFiles.size(),
          baseDir,
          modTimeThreshold);
      for (Path p : matchingFiles) {
        try {
          if (!fs.delete(p, false)) {
            log.error(String.format("Failed to delete file %s", p));
          }
        } catch (IOException e) {
          log.error(String.format("Exception while deleting file %s", p), e);
        }
      }
    } else {
      log.info("Trash dir {} does not exist", baseDir);
    }
    return matchingFiles;
  }

  /**
   * Run deleteStagedFiles operation for the given table with time filter. It deletes files older
   * than the provided number of days from the staged directory.
   */
  public List<Path> deleteStagedFiles(Path baseDir, int olderThanDays, boolean recursive)
      throws IOException {
    return deleteStagedFiles(
        baseDir, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanDays), recursive);
  }

  /**
   * Run RewriteDataFiles operation for the given {@link Table}. Check
   * https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files for more
   * configuration.
   *
   * <p>Compaction organizes files into file groups before doing rewrite. For partitioned tables,
   * there could be 0 or more file groups per partition. File groups don't cross partition
   * boundaries. Those file groups could be processed in parallel, controlled by
   * max-concurrent-file-group-rewrites. As file groups rewrite complete, the app can do commits if
   * partial-progress is enabled. It can do up to partial-progress.max-commits commits per run,
   * multiple file group rewrites can be part of the same commit. Generally, the algorithm is to
   * commit about the same number of groups every time. Compaction does full scan, so in the first
   * run, it can compact all partitions, hence we should be careful and to set the
   * max-concurrent-file-group-rewrites to lower number.
   */
  public RewriteDataFiles.Result rewriteDataFiles(
      Table table,
      long targetByteSize,
      long minByteSize,
      long maxByteSize,
      int minInputFiles,
      int maxConcurrentFileGroupRewrites,
      boolean partialProgressEnabled,
      int partialProgressMaxCommits,
      int deleteFileThreshold) {
    return SparkActions.get(spark)
        .rewriteDataFiles(table)
        .binPack()
        // maximum number of file groups to be simultaneously rewritten
        .option(
            "max-concurrent-file-group-rewrites", Integer.toString(maxConcurrentFileGroupRewrites))
        // enable committing groups of files prior to the entire rewrite completing
        .option("partial-progress.enabled", Boolean.toString(partialProgressEnabled))
        // maximum amount of commits that this rewrite is allowed to produce if partial progress is
        // enabled
        .option("partial-progress.max-commits", Integer.toString(partialProgressMaxCommits))
        // any file group exceeding this number of files will be rewritten regardless of other
        // criteria
        .option("min-input-files", Integer.toString(minInputFiles))
        // 512MB
        .option("target-file-size-bytes", Long.toString(targetByteSize))
        // files under this threshold will be considered for rewriting regardless of any other
        // criteria
        .option("min-file-size-bytes", Long.toString(minByteSize))
        // files with sizes above this threshold will be considered for rewriting regardless of any
        // other criteria
        .option("max-file-size-bytes", Long.toString(maxByteSize))
        .option("delete-file-threshold", Integer.toString(deleteFileThreshold))
        .execute();
  }

  public void rename(final Path src, final Path dest) throws IOException {
    final FileSystem fs = fs();
    if (!fs.exists(dest.getParent())) {
      fs.mkdirs(dest.getParent());
    }
    if (fs.rename(src, dest)) {
      log.info("Moved " + src + " to " + dest);
    } else {
      throw new IOException(
          String.format("FileSystem move operation failed from src: %s to dest %s", src, dest));
    }
  }

  /**
   * Perform recursive lookup of files in a directory and collect files matching the predicate
   * filter.
   */
  public void listFiles(
      Path dir, Predicate<FileStatus> predicate, boolean recursive, List<Path> matchingFiles) {
    try {
      RemoteIterator<LocatedFileStatus> it = fs().listFiles(dir, recursive);
      log.info(
          "StagedFiles Deletion: Path {}, recursive {}, testing predicate {}",
          dir,
          recursive,
          predicate.toString());
      while (it.hasNext()) {
        LocatedFileStatus status = it.next();
        if (predicate.test(status)) {
          matchingFiles.add(status.getPath());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to list files in dir %s: ", dir.toString()), e);
    }
  }

  @Override
  public void close() throws Exception {
    spark.close();
  }

  private Catalog getCatalog() {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = String.format("spark.sql.catalog.%s.", CATALOG);
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    // this initializes the catalog based on runtime Catalog class passed in catalog-impl conf.
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        CATALOG,
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }

  private void useCatalog() {
    spark.sql("USE " + CATALOG);
  }

  static String groupInfoToString(RewriteDataFiles.FileGroupInfo groupInfo) {
    return "(partition="
        + partitionToString(groupInfo.partition())
        + ", partitionIndex="
        + groupInfo.partitionIndex()
        + ", globalIndex="
        + groupInfo.globalIndex()
        + ")";
  }

  /**
   * Converts partition to string representation.
   *
   * @param partition - StructLike is a map of column names to values
   * @return string representation of partition
   */
  static String partitionToString(StructLike partition) {
    StringBuilder ret = new StringBuilder();
    ret.append("{");
    for (int i = 0; i < partition.size(); i++) {
      if (i > 0) {
        ret.append(", ");
      }
      ret.append(partition.get(i, Object.class));
    }
    ret.append("}");
    return ret.toString();
  }

  /**
   * Collect and publish table stats for a given fully-qualified table name.
   *
   * @param fqtn fully-qualified table name
   */
  public IcebergTableStats collectTableStats(String fqtn) {
    Table table = getTable(fqtn);

    try {
      TableStatsCollector tableStatsCollector = new TableStatsCollector(fs(), spark, table);
      return tableStatsCollector.collectTableStats();
    } catch (IOException e) {
      log.error("Unable to initialize file system for table stats collection", e);
      return null;
    } catch (Exception e) {
      log.error("Failed to collect table stats for table: {}", fqtn, e);
      return null;
    }
  }

  /**
   * Collect commit events for a given fully-qualified table name.
   *
   * @param fqtn fully-qualified table name
   * @return List of CommitEventTable objects (event_timestamp_ms will be set at publish time)
   */
  public List<CommitEventTable> collectCommitEventTable(String fqtn) {
    Table table = getTable(fqtn);

    try {
      TableStatsCollector tableStatsCollector = new TableStatsCollector(fs(), spark, table);
      return tableStatsCollector.collectCommitEventTable();
    } catch (IOException e) {
      log.error("Unable to initialize file system for commit events collection", e);
      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Failed to collect commit events for table: {}", fqtn, e);
      return Collections.emptyList();
    }
  }

  /**
   * Collect partition-level commit events for a given fully-qualified table name.
   *
   * <p>Returns one record per (commit_id, partition) pair. Returns empty list for unpartitioned
   * tables or errors.
   *
   * @param fqtn fully-qualified table name
   * @return List of CommitEventTablePartitions objects (event_timestamp_ms will be set at publish
   *     time)
   */
  public List<CommitEventTablePartitions> collectCommitEventTablePartitions(String fqtn) {
    Table table = getTable(fqtn);

    try {
      TableStatsCollector tableStatsCollector = new TableStatsCollector(fs(), spark, table);
      return tableStatsCollector.collectCommitEventTablePartitions();
    } catch (IOException e) {
      log.error("Unable to initialize file system for partition events collection", e);
      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Failed to collect partition events for table: {}", fqtn, e);
      return Collections.emptyList();
    }
  }

  /**
   * Collect statistics for a given fully-qualified table name (partitioned or unpartitioned).
   *
   * <p><b>For PARTITIONED tables:</b> Returns one record per unique partition with aggregated
   * statistics. Each partition is associated with its LATEST commit (highest committed_at
   * timestamp).
   *
   * <p><b>For UNPARTITIONED tables:</b> Returns a single record with aggregated statistics from ALL
   * data_files and current snapshot metadata.
   *
   * <p><b>Key differences from collectCommitEventTablePartitions:</b>
   *
   * <ul>
   *   <li>One record per unique partition (or single record for unpartitioned), not per
   *       commit-partition pair
   *   <li>Latest commit only (max committed_at or current snapshot)
   *   <li>Includes aggregated statistics from data_files metadata table
   * </ul>
   *
   * <p>Returns empty list on errors.
   *
   * @param fqtn fully-qualified table name
   * @return List of CommitEventTablePartitionStats objects (event_timestamp_ms will be set at
   *     publish time)
   */
  public List<CommitEventTablePartitionStats> collectCommitEventTablePartitionStats(String fqtn) {
    Table table = getTable(fqtn);

    try {
      TableStatsCollector tableStatsCollector = new TableStatsCollector(fs(), spark, table);
      return tableStatsCollector.collectCommitEventTablePartitionStats();
    } catch (IOException e) {
      log.error("Unable to initialize file system for partition stats collection", e);
      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Failed to collect partition stats for table: {}", fqtn, e);
      return Collections.emptyList();
    }
  }
}
