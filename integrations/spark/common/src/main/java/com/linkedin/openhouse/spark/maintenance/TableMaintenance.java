package com.linkedin.openhouse.spark.maintenance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.maintenance.SnapshotRetention;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.ImmutableExpireSnapshots;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snapshot expiration and orphan file deletion, shared by the scheduled maintenance jobs and the
 * {@code VACUUM TABLE} statement so both run the same code with the same table rules.
 */
public final class TableMaintenance {
  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenance.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String BACKUP_DIR = ".backup";
  public static final String TABLE_TYPE_KEY = "openhouse.tableType";
  public static final String TABLE_TYPE_REPLICA = "REPLICA_TABLE";
  public static final String ONE_DAY_ORPHAN_FILE_TTL_KEY = "ofd.one_day_ttl.enabled";
  public static final int MIN_REPLICA_ORPHAN_FILE_TTL_DAYS = 3;
  public static final int MAX_ORPHAN_FILE_SAMPLE_SIZE = 20000;
  /** Orphan file age VACUUM TABLE starts from, before {@link #orphanFileTtlSeconds}. */
  public static final int VACUUM_ORPHAN_FILE_TTL_DAYS = 3;

  private final SparkSession spark;

  public TableMaintenance(SparkSession spark) {
    this.spark = spark;
  }

  /** Default file system from the Spark Hadoop configuration. */
  public FileSystem fs() throws IOException {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration());
  }

  /**
   * Run what the scheduled maintenance jobs run for this table, governed by the same table
   * settings: snapshot expiration with the table's history policy (skipped for replica tables, as
   * the scheduler does), deleting expired files, then orphan file deletion with {@link
   * #orphanFileTtlSeconds}. Both send files through {@link FileRemover}.
   *
   * <p>A dry run commits nothing and leaves every file in place. It counts the snapshots that
   * would expire, and the orphan files as they are before expiration.
   *
   * @return counters in a stable order, for reporting
   */
  public Map<String, Long> vacuum(Table table, boolean dryRun) throws IOException {
    Map<String, Long> result = new LinkedHashMap<>();
    if (!isReplica(table)) {
      SnapshotRetention retention = snapshotRetention(table);
      FileRemover expired = new FileRemover(fs(), table, BACKUP_DIR, dryRun);
      long expiredSnapshots;
      if (dryRun) {
        expiredSnapshots = previewExpiredSnapshots(table, retention);
      } else {
        int before = Iterables.size(table.snapshots());
        expireSnapshots(
            table, retention.maxAge(), retention.granularity(), retention.versions(), expired);
        table.refresh();
        expiredSnapshots = before - Iterables.size(table.snapshots());
      }
      result.put("expired_snapshots", expiredSnapshots);
      result.put("expired_files_deleted", expired.deleted());
      result.put("expired_files_backed_up", expired.backedUp());
      result.put("expired_files_failed", expired.failed());
    }
    FileRemover orphans = new FileRemover(fs(), table, BACKUP_DIR, dryRun);
    long ttlSeconds =
        orphanFileTtlSeconds(table, TimeUnit.DAYS.toSeconds(VACUUM_ORPHAN_FILE_TTL_DAYS));
    deleteOrphanFiles(
        table,
        System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttlSeconds),
        1,
        true,
        MAX_ORPHAN_FILE_SAMPLE_SIZE,
        orphans);
    result.put("orphan_files_deleted", orphans.deleted());
    result.put("orphan_files_backed_up", orphans.backedUp());
    result.put("orphan_files_failed", orphans.failed());
    return result;
  }

  /**
   * Run DeleteOrphanFiles on the given table for files older than the provided timestamp; each
   * orphan file is handed to {@code deleteWith}.
   */
  public DeleteOrphanFiles.Result deleteOrphanFiles(
      Table table,
      long olderThanTimestampMillis,
      int concurrentDeletes,
      boolean streamResults,
      int maxOrphanFileSampleSize,
      Consumer<String> deleteWith) {
    DeleteOrphanFiles operation =
        SparkActions.get(spark)
            .deleteOrphanFiles(table)
            .option("stream-results", String.valueOf(streamResults))
            .option("max-orphan-file-sample-size", String.valueOf(maxOrphanFileSampleSize));
    // if time filter is not provided it defaults to 3 days
    if (olderThanTimestampMillis > 0) {
      operation = operation.olderThan(olderThanTimestampMillis);
    }
    if (concurrentDeletes > 1) {
      operation = operation.executeDeleteWith(removeFilesService(concurrentDeletes));
    }
    return operation.deleteWith(deleteWith).execute();
  }

  /**
   * The orphan file age the jobs use for a table: one day when the table opts in with {@link
   * #ONE_DAY_ORPHAN_FILE_TTL_KEY}, and never less than {@link #MIN_REPLICA_ORPHAN_FILE_TTL_DAYS}
   * for replica tables.
   */
  public static long orphanFileTtlSeconds(Table table, long ttlSeconds) {
    long resolved = ttlSeconds;
    if (Boolean.parseBoolean(
        table.properties().getOrDefault(ONE_DAY_ORPHAN_FILE_TTL_KEY, "false"))) {
      resolved = TimeUnit.DAYS.toSeconds(1);
    }
    // Keep the min default OFD TTL for replica tables
    if (isReplica(table)
        && Duration.ofSeconds(resolved).toDays() < MIN_REPLICA_ORPHAN_FILE_TTL_DAYS) {
      resolved = TimeUnit.DAYS.toSeconds(MIN_REPLICA_ORPHAN_FILE_TTL_DAYS);
    }
    return resolved;
  }

  public static boolean isReplica(Table table) {
    return TABLE_TYPE_REPLICA.equals(table.properties().get(TABLE_TYPE_KEY));
  }

  /**
   * Expire snapshots on a given {@link Table}. It expires snapshots older than maxAge in
   * granularity time units, then, if versions is positive, keeps only the last versions snapshots.
   * With a {@code remover}, the files only expired snapshots used go through it; without one, only
   * metadata changes, which skips file enumeration entirely.
   *
   * @return deleted-file counts summed over both phases
   */
  public ExpireSnapshots.Result expireSnapshots(
      Table table, int maxAge, ChronoUnit granularity, int versions, FileRemover remover) {
    long expireBeforeTimestampMs =
        System.currentTimeMillis() - granularity.getDuration().multipliedBy(maxAge).toMillis();

    LOG.info(
        "Expiring snapshots for table: {} older than {}ms with deleteFiles={}",
        table,
        expireBeforeTimestampMs,
        remover != null);

    // First expiration: based on maxAge
    ExpireSnapshots.Result result = expire(table, expireBeforeTimestampMs, null, remover);

    // Second expiration: based on versions (if needed). Both phases can delete files, so their
    // results are combined rather than the second phase's result silently replacing the first's
    // and dropping its deleted-file/manifest counts from observability (metrics and logs).
    if (versions > 0 && Iterables.size(table.snapshots()) > versions) {
      LOG.info("Expiring snapshots for table: {} retaining last {} versions", table, versions);
      result =
          combineResults(result, expire(table, System.currentTimeMillis(), versions, remover));
    }
    return result;
  }

  /**
   * The number of snapshots {@link #expireSnapshots} would expire, without committing. {@code
   * apply()} returns the snapshots an expiration would remove. The versions phase removes all but
   * the newest snapshots, and the age phase removes no snapshot newer than one it keeps, so the
   * two phases together remove the union of what each removes from the current table.
   */
  int previewExpiredSnapshots(Table table, SnapshotRetention retention) {
    long expireBeforeTimestampMs =
        System.currentTimeMillis()
            - retention.granularity().getDuration().multipliedBy(retention.maxAge()).toMillis();
    Set<Long> removed =
        snapshotIds(table.expireSnapshots().expireOlderThan(expireBeforeTimestampMs).apply());
    if (retention.versions() > 0) {
      removed.addAll(
          snapshotIds(
              table
                  .expireSnapshots()
                  .expireOlderThan(System.currentTimeMillis())
                  .retainLast(retention.versions())
                  .apply()));
    }
    return removed.size();
  }

  /**
   * Combine two {@link ExpireSnapshots.Result} instances by summing their deleted-file/manifest
   * counts. Used to accumulate results across the maxAge-based and versions-based expiration phases
   * so metrics/logs reflect files deleted by both phases instead of only the last one run.
   */
  public static ExpireSnapshots.Result combineResults(
      ExpireSnapshots.Result first, ExpireSnapshots.Result second) {
    return ImmutableExpireSnapshots.Result.builder()
        .deletedDataFilesCount(first.deletedDataFilesCount() + second.deletedDataFilesCount())
        .deletedManifestsCount(first.deletedManifestsCount() + second.deletedManifestsCount())
        .deletedManifestListsCount(
            first.deletedManifestListsCount() + second.deletedManifestListsCount())
        .deletedPositionDeleteFilesCount(
            first.deletedPositionDeleteFilesCount() + second.deletedPositionDeleteFilesCount())
        .deletedEqualityDeleteFilesCount(
            first.deletedEqualityDeleteFilesCount() + second.deletedEqualityDeleteFilesCount())
        .build();
  }

  /** Replace the table location part of {@code filePath} with {@code <location>/<trashDir>}. */
  public static Path trashPath(String location, String filePath, String trashDir) {
    return new Path(filePath.replace(location, new Path(location, trashDir).toString()));
  }

  /** Move {@code src} to {@code dest}, creating the destination's parent directory. */
  public static void rename(FileSystem fs, Path src, Path dest) throws IOException {
    if (!fs.exists(dest.getParent())) {
      fs.mkdirs(dest.getParent());
    }
    if (fs.rename(src, dest)) {
      LOG.info("Moved " + src + " to " + dest);
    } else {
      throw new IOException(
          String.format("FileSystem move operation failed from src: %s to dest %s", src, dest));
    }
  }

  private ExpireSnapshots.Result expire(
      Table table, long expireBeforeTimestampMs, Integer versions, FileRemover remover) {
    if (remover == null) {
      // Table API, metadata only
      org.apache.iceberg.ExpireSnapshots expireSnapshots =
          table.expireSnapshots().cleanExpiredFiles(false).expireOlderThan(expireBeforeTimestampMs);
      if (versions != null) {
        expireSnapshots = expireSnapshots.retainLast(versions);
      }
      expireSnapshots.commit();
      return ImmutableExpireSnapshots.Result.builder()
          .deletedDataFilesCount(0L)
          .deletedManifestsCount(0L)
          .deletedManifestListsCount(0L)
          .deletedPositionDeleteFilesCount(0L)
          .deletedEqualityDeleteFilesCount(0L)
          .build();
    }
    // Snapshot planning/manifest enumeration is distributed via Spark; the deleteWith callback
    // itself runs driver-side via Iceberg's internal executor pool.
    ExpireSnapshots action =
        SparkActions.get(spark)
            .expireSnapshots(table)
            .deleteWith(remover)
            .expireOlderThan(expireBeforeTimestampMs);
    if (versions != null) {
      action = action.retainLast(versions);
    }
    return action.execute();
  }

  private static Set<Long> snapshotIds(Iterable<Snapshot> snapshots) {
    Set<Long> ids = new HashSet<>();
    snapshots.forEach(snapshot -> ids.add(snapshot.snapshotId()));
    return ids;
  }

  private static ExecutorService removeFilesService(int concurrentDeletes) {
    return MoreExecutors.getExitingExecutorService(
        (java.util.concurrent.ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                concurrentDeletes,
                new ThreadFactoryBuilder().setNameFormat("remove-orphans-%d").build()));
  }

  /**
   * The snapshot retention the table's history policy, in its {@code policies} property, gives it
   * under the scheduled snapshot expiration job.
   */
  static SnapshotRetention snapshotRetention(Table table) {
    String policies = table.properties().get("policies");
    JsonNode history;
    try {
      history =
          policies == null || policies.isEmpty()
              ? MAPPER.missingNode()
              : MAPPER.readTree(policies).path("history");
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read the policies of table " + table.name(), e);
    }
    return SnapshotRetention.fromHistoryPolicy(
        history.hasNonNull("maxAge") ? history.get("maxAge").asInt() : null,
        history.hasNonNull("granularity") ? history.get("granularity").asText() : null,
        history.hasNonNull("versions") ? history.get("versions").asInt() : null);
  }
}
