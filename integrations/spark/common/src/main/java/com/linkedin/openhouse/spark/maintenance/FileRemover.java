package com.linkedin.openhouse.spark.maintenance;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code deleteWith} handler shared by orphan file deletion and snapshot expiration, so
 * orphaned and expired files get identical treatment.
 *
 * <p>metadata.json files are skipped (Iceberg commits own their lifecycle), as are files already
 * under the backup directory. A data file is moved to the backup directory when a data manifest
 * exists for its partition, otherwise it is deleted: retention writes that manifest listing the
 * files to restore, so backing up a file without one would not make it restorable.
 *
 * <p>A dry run decides the same way but leaves every file in place. Iceberg may call the handler
 * from several threads.
 */
public class FileRemover implements Consumer<String> {
  private static final Logger LOG = LoggerFactory.getLogger(FileRemover.class);

  private final FileSystem fs;
  private final String tableLocation;
  private final String backupDir;
  private final String backupDirRoot;
  private final String dataDirRoot;
  private final boolean dryRun;
  private final Map<String, Boolean> dataManifestsCache = new ConcurrentHashMap<>();
  private final AtomicLong deleted = new AtomicLong();
  private final AtomicLong backedUp = new AtomicLong();
  private final AtomicLong skipped = new AtomicLong();
  private final AtomicLong failed = new AtomicLong();

  public FileRemover(FileSystem fs, Table table, String backupDir, boolean dryRun) {
    this.fs = fs;
    this.tableLocation = table.location();
    this.backupDir = backupDir;
    this.backupDirRoot = new Path(tableLocation, backupDir).toString();
    this.dataDirRoot = new Path(tableLocation, "data").toString();
    this.dryRun = dryRun;
  }

  @Override
  public void accept(String file) {
    LOG.info("Processing file for deletion {}", file);
    if (file.endsWith("metadata.json")) {
      // Don't remove metadata.json files since current metadata.json is recognized as
      // orphan/expired because of inclusion of the scheme in its file path returned by catalog.
      // Also, we want Iceberg commits to remove the metadata.json files not this job.
      LOG.info("Skipped deleting metadata file {}", file);
      skipped.incrementAndGet();
    } else if (file.contains(backupDirRoot)) {
      // files present in .backup dir should not be considered orphan/expired
      LOG.info("Skipped deleting backup file {}", file);
      skipped.incrementAndGet();
    } else if (file.contains(dataDirRoot) && hasBackupDataManifest(file)) {
      backUp(file);
    } else {
      delete(file);
    }
  }

  /** Files deleted, or that a dry run would delete. */
  public long deleted() {
    return deleted.get();
  }

  /** Files moved to the backup directory, or that a dry run would move. */
  public long backedUp() {
    return backedUp.get();
  }

  /** metadata.json and backup files left in place. */
  public long skipped() {
    return skipped.get();
  }

  /** Files that could not be deleted or moved. */
  public long failed() {
    return failed.get();
  }

  private void backUp(String file) {
    Path backupFilePath = TableMaintenance.trashPath(tableLocation, file, backupDir);
    if (dryRun) {
      backedUp.incrementAndGet();
      return;
    }
    LOG.info("Moving file {} to {}", file, backupFilePath);
    try {
      TableMaintenance.rename(fs, new Path(file), backupFilePath);
      // update modification time to current time
      fs.setTimes(backupFilePath, System.currentTimeMillis(), -1);
      backedUp.incrementAndGet();
    } catch (IOException e) {
      LOG.error(String.format("Move operation failed for file: %s", file), e);
      failed.incrementAndGet();
    }
  }

  private void delete(String file) {
    if (dryRun) {
      deleted.incrementAndGet();
      return;
    }
    LOG.info("Deleting file {}", file);
    try {
      if (fs.delete(new Path(file), false)) {
        deleted.incrementAndGet();
      } else {
        failed.incrementAndGet();
      }
    } catch (IOException e) {
      LOG.error(String.format("Delete operation failed for file: %s", file), e);
      failed.incrementAndGet();
    }
  }

  private boolean hasBackupDataManifest(String file) {
    Path backupPartition = TableMaintenance.trashPath(tableLocation, file, backupDir).getParent();
    return dataManifestsCache.computeIfAbsent(
        backupPartition.toString(),
        partition -> {
          try {
            FileStatus[] matches = fs.globStatus(new Path(backupPartition, "data_manifest*"));
            return matches != null && matches.length > 0;
          } catch (IOException e) {
            return false;
          }
        });
  }
}
