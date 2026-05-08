package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@Slf4j
public class AppsTest extends OpenHouseSparkITest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

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
        },
        otelEmitter);
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
        },
        otelEmitter);
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
        },
        otelEmitter);
    DataCompactionSparkApp.createApp(
        new String[] {
          "--jobId",
          "test-job-id",
          "--storageURL",
          "http://localhost:8080",
          "--tableName",
          tableName
        },
        otelEmitter);
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
        new BaseSparkApp(jobId, stateManagerMock, heartbeatIntervalSeconds, otelEmitter) {
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

  @Test
  public void testRetentionSparkAppPassesUtcNowToRunRetention() {
    final String tableName = "db.test_retention_app";
    Operations ops = Mockito.mock(Operations.class);
    Table table = Mockito.mock(Table.class);
    StateManager stateManagerMock = Mockito.mock(StateManager.class);
    Mockito.when(ops.getTable(tableName)).thenReturn(table);
    Mockito.when(table.properties()).thenReturn(Map.of(AppConstants.BACKUP_ENABLED_KEY, "true"));

    RetentionSparkApp app =
        new RetentionSparkApp(
            "test-job-id",
            stateManagerMock,
            tableName,
            "ts",
            "yyyy-MM-dd-HH",
            "DAY",
            7,
            otelEmitter,
            ".backup");

    app.runInner(ops);

    ArgumentCaptor<ZonedDateTime> nowCaptor = ArgumentCaptor.forClass(ZonedDateTime.class);
    Mockito.verify(ops)
        .runRetention(
            Mockito.eq(tableName),
            Mockito.eq("ts"),
            Mockito.eq("yyyy-MM-dd-HH"),
            Mockito.eq("DAY"),
            Mockito.eq(7),
            Mockito.eq(true),
            Mockito.eq(".backup"),
            nowCaptor.capture());
    Assertions.assertEquals(ZoneOffset.UTC, nowCaptor.getValue().getZone());
  }
}
