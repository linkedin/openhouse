package com.linkedin.openhouse.jobs.util;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.nio.file.FileSystems;
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
  static final String testDir = "oh_delete_test";
  static final Path testPath = new Path(baseDir.toString(), testDir);
  static final String dirPrefix = "dir_";
  static final String filePrefix = "file_";
  private final Meter meter = OtelConfig.getMeter(this.getClass().getName());

  @BeforeEach
  @AfterEach
  void cleanUpTestFiles() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), meter)) {
      FileSystem fs = ops.fs();
      fs.delete(testPath, true);
      Assertions.assertFalse(fs.exists(testPath));
    }
  }

  @Test
  void testShouldDeleteFilesOlderThanXDaysInDir() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), meter)) {
      FileSystem fs = ops.fs();
      generateDirStructure(fs, testPath.toString(), 3, 3);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(testPath.toString(), "dir_2"), 4);
      ops.deleteStagedFiles(testPath, 3, true);
      Assertions.assertTrue(fs.exists(new Path(testPath + "/dir_0/" + "file_0")));
      Assertions.assertFalse(fs.exists(new Path(testPath + "/dir_2/" + "file_0")));
    }
  }

  @Test
  void testShouldFindMatchingFilesRecursively() throws Exception {
    try (Operations ops = Operations.withCatalog(getSparkSession(), meter)) {
      FileSystem fs = ops.fs();
      generateDirStructure(fs, testPath.toString(), 4, 2);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(testPath.toString(), "dir_1"), 7);
      SparkJobUtil.setModifiedTimeStamp(fs, new Path(testPath.toString(), "dir_3"), 7);
      Predicate<FileStatus> predicate =
          file ->
              file.getModificationTime() < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
      List<Path> matchingFiles = Lists.newArrayList();
      ops.listFiles(testPath, predicate, true, matchingFiles);
      Assertions.assertEquals(4, matchingFiles.size());
    }
  }

  private void generateDirStructure(FileSystem fs, String startDir, int depth, int filesAtEach)
      throws IOException {
    fs.delete(new Path(startDir), true);
    for (int row = 0; row < depth; ++row) {
      Path basePath = new Path(startDir, dirPrefix + row);
      fs.mkdirs(basePath);
      for (int count = 0; count < filesAtEach; ++count) {
        Path fp = new Path(basePath.toString(), filePrefix + count);
        fs.createNewFile(fp);
      }
    }
  }
}
