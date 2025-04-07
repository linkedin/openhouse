package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

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

  @Test
  public void testHeartbeat() throws Exception {
    final int minNumHeartbeats = 3;
    final int heartbeatIntervalSeconds = 1;
    final String jobId = "testJobId";
    final CountDownLatch latch = new CountDownLatch(minNumHeartbeats);
    StateManager stateManagerMock = Mockito.mock(StateManager.class);
    Mockito.doAnswer(
            (Answer<Void>)
                invocation -> {
                  latch.countDown();
                  return null;
                })
        .when(stateManagerMock)
        .sendHeartbeat(jobId);
    BaseSparkApp app =
        new BaseSparkApp(jobId, stateManagerMock, heartbeatIntervalSeconds) {
          @Override
          protected void runInner(Operations ops) {
            try {
              // wait until at least 3 heartbeats are sent
              latch.await(minNumHeartbeats * heartbeatIntervalSeconds + 2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        };
    // pre-create spark session configured for local catalog
    getSparkSession();
    app.run();
    Mockito.verify(
            stateManagerMock,
            Mockito.timeout((minNumHeartbeats * heartbeatIntervalSeconds) + 2)
                .atLeast(minNumHeartbeats))
        .sendHeartbeat(jobId);
  }
}
