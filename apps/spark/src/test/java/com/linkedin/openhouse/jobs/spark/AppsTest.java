package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class AppsTest extends OpenHouseSparkITest {
  @Test
  public void testCreateDataCompactionSparkApp() {
    final String tableName = "db.test_data_compaction_app";
    DataCompactionSparkApp.createApp(
        new String[] {
          "--jobId",
          "test-job-id",
          "--storageURL",
          "http://localhost:8080",
          "--tableName",
          tableName,
          "--targetByteSize",
          "1048576",
          "--minByteSizeRatio",
          "0.75",
          "--maxByteSizeRatio",
          "1.8",
          "--minInputFiles",
          "5",
          "--maxConcurrentFileGroupRewrites",
          "2",
          "--partialProgressEnabled",
          "--partialProgressMaxCommits",
          "10"
        });
    DataCompactionSparkApp.createApp(
        new String[] {
          "--jobId",
          "test-job-id",
          "--storageURL",
          "http://localhost:8080",
          "--tableName",
          tableName,
          "--targetByteSize",
          "1048576",
        });
    DataCompactionSparkApp.createApp(
        new String[] {
          "--jobId",
          "test-job-id",
          "--storageURL",
          "http://localhost:8080",
          "--tableName",
          tableName,
          "--maxConcurrentFileGroupRewrites",
          "2",
          "--partialProgressEnabled",
          "--partialProgressMaxCommits",
          "10"
        });
    DataCompactionSparkApp.createApp(
        new String[] {
          "--jobId",
          "test-job-id",
          "--storageURL",
          "http://localhost:8080",
          "--tableName",
          tableName
        });
  }
}
