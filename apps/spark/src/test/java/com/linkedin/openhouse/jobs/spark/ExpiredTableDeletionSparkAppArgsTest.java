package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpiredTableDeletionSparkAppArgsTest {
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void createAppRejectsListsOfDifferentLengths() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ExpiredTableDeletionSparkApp.createApp(
                arguments("db.a,db.b", "operation-a"), otelEmitter));
  }

  @Test
  public void createAppRejectsTableNameWithoutDatabase() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ExpiredTableDeletionSparkApp.createApp(
                arguments("db.a,b", "operation-a,operation-b"), otelEmitter));
  }

  private static String[] arguments(String tableNames, String operationIds) {
    return new String[] {
      "--jobId",
      "test-job",
      "--storageURL",
      "http://localhost:8080",
      "--tableNames",
      tableNames,
      "--operationIds",
      operationIds,
      "--resultsEndpoint",
      "http://localhost:8003"
    };
  }
}
