package com.linkedin.openhouse.spark.maintenance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

/**
 * Orphan table directory handling shared by the orphan directory deletion job and the {@code
 * DELETE ORPHAN DIRECTORY} statement.
 */
public final class OrphanTableDirectory {
  public static final String TRASH_DIR = ".trash";
  /** A directory counts as orphaned once a file in it is older than this. */
  public static final int DEFAULT_ORPHAN_AGE_DAYS = 7;

  public static final String ROOT_PATH_CONF = "spark.sql.openhouse.orphanDirectory.rootPath";
  public static final String DEFAULT_ROOT_PATH = "/data/openhouse";
  private static final String CATALOG = "openhouse";

  private OrphanTableDirectory() {}

  /** Files under {@code dir}, recursively, except those in its {@code trashDir}. */
  public static List<FileStatus> listFiles(FileSystem fs, Path dir, String trashDir)
      throws IOException {
    List<FileStatus> files = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, true);
    while (it.hasNext()) {
      LocatedFileStatus status = it.next();
      if (!status.getPath().toString().contains(trashDir)) {
        files.add(status);
      }
    }
    return files;
  }

  /** The jobs treat a directory as orphaned once any of its files is older than the threshold. */
  public static boolean hasFileOlderThan(List<FileStatus> files, long olderThanTimestampMillis) {
    return files.stream().anyMatch(f -> f.getModificationTime() < olderThanTimestampMillis);
  }

  /**
   * Delete everything inside one orphaned table directory, {@code <root>/<database>/<directory>},
   * with the caller's own access. The directory itself stays: OpenHouse owns it.
   *
   * <p>Refuses unless a file in the directory is older than {@link #DEFAULT_ORPHAN_AGE_DAYS} days,
   * as the job requires, and the table named by the directory's newest metadata file either does
   * not exist or is located elsewhere. Metadata is deleted last, so a rerun after a partial delete
   * makes the same check.
   *
   * @return the entries deleted, or that a dry run would delete
   */
  public static List<Entry> deleteContents(SparkSession spark, String path, boolean dryRun)
      throws IOException {
    String root = spark.conf().get(ROOT_PATH_CONF, DEFAULT_ROOT_PATH).replaceAll("/+$", "");
    Path dir = tableDirectory(path, root);
    FileSystem fs = dir.getFileSystem(spark.sparkContext().hadoopConfiguration());
    require(fs.getFileStatus(dir).isDirectory(), path + " is not a directory");

    long threshold = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(DEFAULT_ORPHAN_AGE_DAYS);
    require(
        hasFileOlderThan(listFiles(fs, dir, TRASH_DIR), threshold),
        String.format(
            "%s has no file older than %d days, so it isn't treated as orphaned yet",
            path, DEFAULT_ORPHAN_AGE_DAYS));

    checkNoLiveTable(spark, fs, dir);

    FileStatus[] children = fs.listStatus(dir);
    // metadata last, so a rerun after a partial delete still checks the table
    Arrays.sort(
        children, Comparator.comparing(c -> c.getPath().getName().contains("metadata")));
    List<Entry> deleted = new ArrayList<>();
    for (FileStatus child : children) {
      // read before deleting: a local file system looks the owner up from the path
      Entry entry = new Entry(child.getPath().toString(), child.getOwner());
      if (!dryRun && !fs.delete(child.getPath(), true)) {
        throw new IOException("Could not delete " + child.getPath());
      }
      deleted.add(entry);
    }
    return deleted;
  }

  /** A top-level entry of a table directory and its owner. */
  public static final class Entry {
    private final String path;
    private final String owner;

    Entry(String path, String owner) {
      this.path = path;
      this.owner = owner;
    }

    public String path() {
      return path;
    }

    public String owner() {
      return owner;
    }
  }

  /** {@code path} as a table directory: {@code <root>/<database>/<directory>}, with no scheme. */
  static Path tableDirectory(String path, String root) {
    require(path != null, "path is null");
    String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    String expected = String.format("%s/<database>/<directory>", root);
    String[] parts = trimmed.split("/", -1);
    require(
        trimmed.startsWith(root + "/")
            && parts.length == root.split("/", -1).length + 2
            && Arrays.stream(parts, 1, parts.length)
                .noneMatch(p -> p.isEmpty() || p.equals(".") || p.equals("..")),
        String.format("%s is not %s", path, expected));
    return new Path(trimmed);
  }

  /**
   * Refuse if the table named by the directory's newest metadata file is located in it. That
   * metadata names the table even after a rename, and a live table always keeps its current
   * metadata file in its own directory.
   */
  private static void checkNoLiveTable(SparkSession spark, FileSystem fs, Path dir)
      throws IOException {
    List<FileStatus> metadataFiles = new ArrayList<>();
    for (Path candidate : new Path[] {new Path(dir, "metadata"), dir}) {
      if (fs.exists(candidate)) {
        for (FileStatus f : fs.listStatus(candidate)) {
          if (f.getPath().getName().endsWith(".metadata.json")) {
            metadataFiles.add(f);
          }
        }
      }
    }
    if (metadataFiles.isEmpty()) {
      return;
    }
    // File names start with the zero-padded metadata version, so they break modification time ties.
    String newest =
        metadataFiles.stream()
            .max(
                Comparator.comparingLong(FileStatus::getModificationTime)
                    .thenComparing(f -> f.getPath().getName()))
            .get()
            .getPath()
            .toString();
    Map<String, String> props =
        TableMetadataParser.read(new HadoopFileIO(fs.getConf()), newest).properties();
    String database = props.get("openhouse.databaseId");
    String tableId = props.get("openhouse.tableId");
    require(database != null && tableId != null, newest + " names no OpenHouse table");

    String table = String.format("%s.%s.%s", CATALOG, database, tableId);
    String location;
    try {
      location = Spark3Util.loadIcebergTable(spark, table).location();
    } catch (NoSuchTableException | org.apache.iceberg.exceptions.NoSuchTableException e) {
      return;
    } catch (org.apache.spark.sql.catalyst.parser.ParseException e) {
      throw new IllegalArgumentException(newest + " names an invalid table " + table, e);
    }
    String live = Path.getPathWithoutSchemeAndAuthority(new Path(location)).toString();
    require(
        !live.equals(dir.toString()) && !live.startsWith(dir + "/"),
        String.format("live table %s is located in %s", table, dir));
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
