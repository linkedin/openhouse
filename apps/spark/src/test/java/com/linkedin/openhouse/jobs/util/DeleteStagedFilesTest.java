package com.linkedin.openhouse.jobs.util;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeleteStagedFilesTest extends OpenHouseSparkITest {
  static java.nio.file.Path baseDir = FileSystems.getDefault().getPath(".").toAbsolutePath();
  static final String TEST_DIR = "oh_delete_test";
  static final Path TEST_PATH = new Path(baseDir.toString(), TEST_DIR);
  static final String DIR_PREFIX = "dir_";
  static final String FILE_PREFIX = "file_";
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @BeforeEach
  @AfterEach
  void cleanUpTestFiles() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      FileSystem fs = ops.fs();
      fs.delete(TEST_PATH, true);
      Assertions.assertFalse(fs.exists(TEST_PATH));
    }
  }

  @Test
  void testShouldDeleteFilesOlderThanXDaysInDir() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      FileSystem fs = ops.fs();
      generateDirStructure(fs, TEST_PATH.toString(), 3, 3);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(TEST_PATH.toString(), "dir_2"), 4);
      ops.deleteStagedFiles(TEST_PATH, 3, true);
      Assertions.assertTrue(fs.exists(new Path(TEST_PATH + "/dir_0/" + "file_0")));
      Assertions.assertFalse(fs.exists(new Path(TEST_PATH + "/dir_2/" + "file_0")));
    }
  }

  @Test
  void testShouldFindMatchingFilesRecursively() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      FileSystem fs = ops.fs();
      generateDirStructure(fs, TEST_PATH.toString(), 4, 2);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(TEST_PATH.toString(), "dir_1"), 7);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(TEST_PATH.toString(), "dir_3"), 7);
      Predicate<FileStatus> predicate =
          file ->
              file.getModificationTime() < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
      List<Path> matchingFiles = Lists.newArrayList();
      ops.listFiles(TEST_PATH, predicate, true, matchingFiles);
      Assertions.assertEquals(4, matchingFiles.size());
    }
  }

  private void generateDirStructure(FileSystem fs, String startDir, int depth, int filesAtEach)
      throws IOException {
    fs.delete(new Path(startDir), true);
    for (int row = 0; row < depth; ++row) {
      Path basePath = new Path(startDir, DIR_PREFIX + row);
      fs.mkdirs(basePath);
      for (int count = 0; count < filesAtEach; ++count) {
        Path fp = new Path(basePath.toString(), FILE_PREFIX + count);
        fs.createNewFile(fp);
      }
    }
  }
}
