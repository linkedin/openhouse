package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class OrphanTableDirectoryDeletionSparkAppTest extends OpenHouseSparkITest {
  private static final String TRASH_DIR = ".trash";
  private static final String MARKER = TRASH_DIR + "/" + Operations.STAGED_FOR_DELETION_MARKER;
  private static final int ORPHAN_DAYS = 7;
  private static final int STAGED_DELETE_DAYS = 3;
  private static final String TABLE_UUID = "0b7c4a6e-3f8d-4c1a-9e2b-5d6f7a8b9c0d";
  private static final String ROOT_METADATA = "00001-" + UUID.randomUUID() + ".metadata.json";

  private final OtelEmitter opsOtelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  private OtelEmitter otelEmitter;
  private Path tableDirectory;

  @TempDir java.nio.file.Path tempDir;

  @BeforeEach
  void setup() {
    otelEmitter = Mockito.mock(OtelEmitter.class);
    tableDirectory = new Path(tempDir.toString(), "db/tb-" + TABLE_UUID);
  }

  @Test
  public void testDirectoryIsStagedWhenQuietAndDeletedAfterGracePeriod() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, ROOT_METADATA, tableMetadata(TABLE_UUID, location()), daysAgo(30));
      createFile(fs, "metadata/00001.metadata.json", daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));
      createFile(fs, "data/.trash/b.orc", daysAgo(30));
      createFile(fs, "metadata/00002.metadata.json", daysAgo(1));

      // A directory written to recently may still be in use.
      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              ROOT_METADATA,
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
              ".trash/" + ROOT_METADATA,
              ".trash/metadata/00001.metadata.json",
              ".trash/data/a.orc",
              ".trash/data/.trash/b.orc",
              ".trash/metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);

      // The grace period starts when files are staged, not when they were last written.
      newApp().runInner(ops);
      Assertions.assertEquals(6, listFiles(fs).size());
      setModificationTime(fs, MARKER, daysAgo(ORPHAN_DAYS + STAGED_DELETE_DAYS - 1));
      newApp().runInner(ops);
      Assertions.assertEquals(6, listFiles(fs).size());
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
      createFile(fs, ".trash/" + ROOT_METADATA, tableMetadata(TABLE_UUID, location()), daysAgo(30));
      createFile(fs, ".trash/metadata/00001.metadata.json", daysAgo(30));
      createFile(fs, "metadata/00002.metadata.json", daysAgo(1));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              MARKER,
              ".trash/" + ROOT_METADATA,
              ".trash/metadata/00001.metadata.json",
              "metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
      // Kept even if all files are older than the threshold.
      Assertions.assertFalse(
          ops.deleteStagedOrphanDirectory(tableDirectory, TRASH_DIR, System.currentTimeMillis()));
      Assertions.assertEquals(4, listFiles(fs).size());

      // Once quiet, it is staged again and its grace period restarts.
      setModificationTime(fs, daysAgo(30));
      newApp().runInner(ops);
      Assertions.assertEquals(
          files(
              MARKER,
              ".trash/" + ROOT_METADATA,
              ".trash/metadata/00001.metadata.json",
              ".trash/metadata/00002.metadata.json"),
          listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);
      newApp().runInner(ops);
      Assertions.assertEquals(4, listFiles(fs).size());
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
      createFile(fs, ROOT_METADATA, tableMetadata(TABLE_UUID, location()), daysAgo(30));
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
  public void testPartiallyStagedDirectoryIsStagedAgain() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, MARKER, daysAgo(30));
      createFile(fs, ".trash/" + ROOT_METADATA, tableMetadata(TABLE_UUID, location()), daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(MARKER, ".trash/" + ROOT_METADATA, ".trash/data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);
    }
  }

  @Test
  public void testDirectoryWithCompressedRootMetadataIsStagedAndDeleted() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      String rootMetadata = "00001-" + UUID.randomUUID() + ".gz.metadata.json";
      createCompressedFile(fs, rootMetadata, tableMetadata(TABLE_UUID, location()), daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(MARKER, ".trash/" + rootMetadata, ".trash/data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 1);

      setModificationTime(fs, MARKER, daysAgo(ORPHAN_DAYS + STAGED_DELETE_DAYS + 1));
      newApp().runInner(ops);
      Assertions.assertFalse(fs.exists(tableDirectory));
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 1);
    }
  }

  @Test
  public void testDirectoryWhoseLatestRootMetadataIsOfAnotherLocationIsNotStaged()
      throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      String latestRootMetadata = "00002-" + UUID.randomUUID() + ".metadata.json";
      createFile(fs, ROOT_METADATA, tableMetadata(TABLE_UUID, location()), daysAgo(30));
      createFile(
          fs, latestRootMetadata, tableMetadata(TABLE_UUID, location() + "_other"), daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(ROOT_METADATA, latestRootMetadata, "data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("directoriesOfNoOpenHouseTable")
  public void testDirectoryOfNoOpenHouseTableIsNotStaged(
      String description, String metadataFile, Function<String, String> metadata) throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, metadataFile, metadata.apply(location()), daysAgo(30));
      createFile(fs, "data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(files(metadataFile, "data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.ORPHAN_DIRECTORY_COUNT, 0);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("directoriesOfNoOpenHouseTable")
  public void testStagedDirectoryOfNoOpenHouseTableIsNotDeleted(
      String description, String metadataFile, Function<String, String> metadata) throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), opsOtelEmitter)) {
      FileSystem fs = ops.fs();
      createFile(fs, MARKER, daysAgo(30));
      createFile(fs, ".trash/" + metadataFile, metadata.apply(location()), daysAgo(30));
      createFile(fs, ".trash/data/a.orc", daysAgo(30));

      newApp().runInner(ops);
      Assertions.assertEquals(
          files(MARKER, ".trash/" + metadataFile, ".trash/data/a.orc"), listFiles(fs));
      verifyCount(AppConstants.STAGED_DIRECTORY_COUNT, 0);
    }
  }

  /**
   * Arguments: description, root metadata file relative to the directory, its content by location.
   */
  private static Stream<Arguments> directoriesOfNoOpenHouseTable() {
    Function<String, String> table = location -> tableMetadata(TABLE_UUID, location);
    return Stream.of(
        Arguments.of("metadataNotAtRoot", "metadata/" + ROOT_METADATA, table),
        Arguments.of("metadataNotNamedByOpenHouse", "v1.metadata.json", table),
        Arguments.of(
            "viewMetadata",
            ROOT_METADATA,
            (Function<String, String>) location -> viewMetadata(TABLE_UUID, location)),
        Arguments.of(
            "metadataOfAnotherTable",
            ROOT_METADATA,
            (Function<String, String>)
                location -> tableMetadata(UUID.randomUUID().toString(), location)),
        Arguments.of(
            "metadataOfTableLocatedElsewhere",
            ROOT_METADATA,
            (Function<String, String>) location -> tableMetadata(TABLE_UUID, location + "_other")),
        Arguments.of(
            "malformedMetadata",
            ROOT_METADATA,
            (Function<String, String>) location -> "{\"table-uuid\": "),
        Arguments.of(
            "uncompressedMetadataNamedCompressed",
            "00001-" + UUID.randomUUID() + ".gz.metadata.json",
            table));
  }

  /**
   * Root metadata as OpenHouse writes it, in Iceberg's field order. Its Iceberg table UUID is not
   * the OpenHouse one.
   */
  private static String tableMetadata(String openHouseTableUuid, String location) {
    return String.format(
        "{\"format-version\":2,\"table-uuid\":\"%s\",\"location\":\"%s\","
            + "\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":"
            + "[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"string\"}]}],"
            + "\"properties\":{\"write.format.default\":\"orc\",\"openhouse.tableUUID\":\"%s\"},"
            + "\"snapshots\":[]}",
        UUID.randomUUID(), location, openHouseTableUuid);
  }

  /** View metadata as OpenHouse writes it: a view UUID instead of a table UUID. */
  private static String viewMetadata(String openHouseTableUuid, String location) {
    return String.format(
        "{\"view-uuid\":\"%s\",\"format-version\":1,\"location\":\"%s\","
            + "\"properties\":{\"openhouse.tableUUID\":\"%s\"},"
            + "\"current-version-id\":1,\"versions\":[]}",
        UUID.randomUUID(), location, openHouseTableUuid);
  }

  /** The table directory as OpenHouse records table locations, without a scheme. */
  private String location() {
    return tableDirectory.toUri().getPath();
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
    createFile(fs, relativePath, "", modificationTimeMillis);
  }

  private void createFile(
      FileSystem fs, String relativePath, String content, long modificationTimeMillis)
      throws IOException {
    Path file = new Path(tableDirectory, relativePath);
    try (OutputStream out = fs.create(file)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
    fs.setTimes(file, modificationTimeMillis, -1);
  }

  private void createCompressedFile(
      FileSystem fs, String relativePath, String content, long modificationTimeMillis)
      throws IOException {
    Path file = new Path(tableDirectory, relativePath);
    try (OutputStream out = new GZIPOutputStream(fs.create(file))) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
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
