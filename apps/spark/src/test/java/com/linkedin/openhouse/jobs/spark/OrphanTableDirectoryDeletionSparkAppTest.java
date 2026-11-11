package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class OrphanTableDirectoryDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final String TRASH_DIR = ".trash";
  private static final String MARKER = TRASH_DIR + "/" + Operations.STAGED_FOR_DELETION_MARKER;
  private static final int ORPHAN_DAYS = 7;
  private static final int STAGED_DELETE_DAYS = 3;

  private final OtelEmitter opsOtelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  private OtelEmitter otelEmitter;
  private Path tableDirectory;

  @TempDir java.nio.file.Path tempDir;

  @BeforeEach
  void setup() {
    otelEmitter = Mockito.mock(OtelEmitter.class);
    tableDirectory = new Path(tempDir.toString(), "db/tb-" + UUID.randomUUID());
  }

  @Test
  public void testDirectoryIsStagedWhenQuietAndDeletedAfterGracePeriod() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, "metadata/00001.metadata.json", daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));
      createFile(fs, "data/.trash/b.orc", daysAgo(30));
      createFile(fs, "metadata/00002.metadata.json", daysAgo(1));

      // A directory written to recently may still be in use.
      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              "metadata/00001.metadata.json",
              "data/a.orc",
              "data/.trash/b.orc",
              "metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);

      setModificationTime(fs, daysAgo(30));
      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              MARKER,
              ".trash/metadata/00001.metadata.json",
              ".trash/data/a.orc",
              ".trash/data/.trash/b.orc",
              ".trash/metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);

      // The grace period starts when files are staged, not when they were last written.
      newApp().runInner(ops);
      Assertions.assertEquals(5, listFiles(fs).size());
      setModificationTime(fs, MARKER, daysAgo(ORPHAN_DAYS + STAGED_DELETE_DAYS - 1));
      newApp().runInner(ops);
      Assertions.assertEquals(5, listFiles(fs).size());
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);

      setModificationTime(fs, MARKER, daysAgo(ORPHAN_DAYS + STAGED_DELETE_DAYS + 1));
      newApp().runInner(ops);
      Assertions.assertFalse(fs.exists(tableDirectory));
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 1);
    }
  }

  @Test
  public void testDirectoryWrittenToAfterStagingIsStagedAgain() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, MARKER, daysAgo(30));
      createFile(fs, ".trash/metadata/00001.metadata.json", daysAgo(30));
      createFile(fs, "metadata/00002.metadata.json", daysAgo(1));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(MARKER, ".trash/metadata/00001.metadata.json", "metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
      // Kept even if all files are older than the threshold.
      Assertions.assertFalse(
          ops.deleteStagedOrphanDirectory(tableDirectory, TRASH_DIR, System.currentTimeMillis()));
      Assertions.assertEquals(3, listFiles(fs).size());

      // Once quiet, it is staged again and its grace period restarts.
      setModificationTime(fs, daysAgo(30));
      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              MARKER, ".trash/metadata/00001.metadata.json", ".trash/metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);
      newApp().runInner(ops);
      Assertions.assertEquals(3, listFiles(fs).size());
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
    }
  }

  @Test
  public void testDirectoryWithoutStagingMarkerIsNotDeleted() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, ".trash/data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(files(".trash/data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
    }
  }

  @Test
  public void testDirectoryWithoutFilesIsNotDeleted() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      fs.mkdirs(new Path(tableDirectory, "data"));

      newApp().runInner(ops);
      Assertions.assertTrue(fs.exists(new Path(tableDirectory, "data")));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
    }
  }

  @Test
  public void testJobFailsWhenFilesCannotBeStaged() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, "metadata/00001.metadata.json", daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));
      // A file where data/a.orc should be staged makes moving it fail.
      createFile(fs, ".trash/data", daysAgo(30));

      OrphanTableDirectoryDeletionSparkApp app = newApp();
      Assertions.assertThrows(IOException.class, () -> app.runInner(ops));
      Assertions.assertTrue(fs.exists(new Path(tableDirectory, "data/a.orc")));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
    }
  }

  @Test
  public void testCreateAppAppliesOptions() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, "data/a.orc", daysAgo(10));

      // Unmodified for longer than the default 7 days, but not 30.
      createApp("--orphanDaysOld", "30").runInner(ops);
      Assertions.assertEquals(files("data/a.orc"), listFiles(fs));

      createApp("--orphanDaysOld", "5", "--trashDir", ".staged").runInner(ops);
      Assertions.assertEquals(
          files(".staged/" + Operations.STAGED_FOR_DELETION_MARKER, ".staged/data/a.orc"),
          listFiles(fs));

      // Staged for longer than the default 7 + 3 days, but not 5 + 30.
      setModificationTime(fs, daysAgo(15));
      createApp("--orphanDaysOld", "5", "--stagedDeleteDaysOld", "30", "--trashDir", ".staged")
          .runInner(ops);
      Assertions.assertEquals(2, listFiles(fs).size());

      createApp("-o", "5", "-s", "9", "-b", ".staged").runInner(ops);
      Assertions.assertFalse(fs.exists(tableDirectory));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "--orphanDaysOld, 0",
    "--stagedDeleteDaysOld, -1",
    "--stagedDeleteDaysOld, three",
    "--trashDir, ''",
    "--trashDir, ..",
    "--trashDir, ../.trash",
    "--trashDir, /tmp/.trash"
  })
  public void testCreateAppRejectsInvalidOptions(String option, String value) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> createApp(option, value));
  }

  private OrphanTableDirectoryDeletionSparkApp createApp(String... options) {
    List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "--jobId",
                "test-odd-job",
                "--storageURL",
                "http://localhost:8080",
                "--tableDirectoryPath",
                tableDirectory.toString()));
    args.addAll(Arrays.asList(options));
    return OrphanTableDirectoryDeletionSparkApp.createApp(args.toArray(new String[0]), otelEmitter);
  }

  private OrphanTableDirectoryDeletionSparkApp newApp() {
    return new OrphanTableDirectoryDeletionSparkApp(
        "test-odd-job",
        null,
        tableDirectory,
        TRASH_DIR,
        ORPHAN_DAYS,
        STAGED_DELETE_DAYS,
        otelEmitter);
  }

  private void verifyCount(String metric, int times) {
    Mockito.verify(otelEmitter, Mockito.times(times))
        .count(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.eq(metric),
            ArgumentMatchers.eq(1L),
            ArgumentMatchers.any());
  }

  private static long daysAgo(int days) {
    return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days);
  }

  private void createFile(FileSystem fs, String relativePath, long modificationTimeMillis)
      throws IOException {
    Path file = new Path(tableDirectory, relativePath);
    fs.create(file).close();
    fs.setTimes(file, modificationTimeMillis, -1);
  }

  private void setModificationTime(FileSystem fs, long modificationTimeMillis) throws IOException {
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(tableDirectory, true);
    while (files.hasNext()) {
      fs.setTimes(files.next().getPath(), modificationTimeMillis, -1);
    }
  }

  private void setModificationTime(FileSystem fs, String relativePath, long modificationTimeMillis)
      throws IOException {
    fs.setTimes(new Path(tableDirectory, relativePath), modificationTimeMillis, -1);
  }

  /** Lists files relative to the table directory. */
  private Set<String> listFiles(FileSystem fs) throws IOException {
    String prefix = tableDirectory.toUri().getPath() + Path.SEPARATOR;
    Set<String> relativePaths = new HashSet<>();
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(tableDirectory, true);
    while (files.hasNext()) {
      relativePaths.add(files.next().getPath().toUri().getPath().substring(prefix.length()));
    }
    return relativePaths;
  }

  private static Set<String> files(String... relativePaths) {
    return new HashSet<>(Arrays.asList(relativePaths));
  }
}
