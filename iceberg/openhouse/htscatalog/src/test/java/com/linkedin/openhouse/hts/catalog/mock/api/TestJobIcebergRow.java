package com.linkedin.openhouse.hts.catalog.mock.api;

import com.linkedin.openhouse.hts.catalog.api.IcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.jobtable.JobIcebergRow;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJobIcebergRow {

  static JobIcebergRow jobIcebergRow;

  @BeforeAll
  static void setup() {
    jobIcebergRow =
        JobIcebergRow.builder()
            .jobId("id1")
            .state("ENQUEUED")
            .version("v1")
            .clusterId("testCluster")
            .jobName("jobName")
            .jobConf("jobConf")
            .creationTimeMs(1651016746000L)
            .startTimeMs(1651016750000L)
            .finishTimeMs(1651017746000L)
            .lastUpdateTimeMs(1651017746000L)
            .heartbeatTimeMs(1651017746000L)
            .executionId("1")
            .retentionTimeSec(1745908497L)
            .build();
  }

  @Test
  void testVersioning() {
    Assertions.assertEquals(jobIcebergRow.getVersionColumnName(), "version");
    Assertions.assertNotEquals(jobIcebergRow.getNextVersion(), jobIcebergRow.getCurrentVersion());
    Assertions.assertEquals(jobIcebergRow.getNextVersion(), jobIcebergRow.getNextVersion());
  }

  @Test
  void testSchema() {
    Assertions.assertTrue(
        jobIcebergRow.getSchema().columns().stream()
            .map(Types.NestedField::name)
            .collect(Collectors.toList())
            .containsAll(
                Arrays.asList(
                    "jobId",
                    "state",
                    "version",
                    "jobName",
                    "clusterId",
                    "creationTimeMs",
                    "startTimeMs",
                    "finishTimeMs",
                    "lastUpdateTimeMs",
                    "jobConf",
                    "heartbeatTimeMs",
                    "executionId",
                    "retentionTimeSec")));
  }

  @Test
  void testRecord() {
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("jobId"), "id1");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("state"), "ENQUEUED");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("version"), "v1");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("jobName"), "jobName");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("jobConf"), "jobConf");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("creationTimeMs"), 1651016746000L);
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("startTimeMs"), 1651016750000L);
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("finishTimeMs"), 1651017746000L);
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("lastUpdateTimeMs"), 1651017746000L);
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("heartbeatTimeMs"), 1651017746000L);
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("executionId"), "1");
    Assertions.assertEquals(jobIcebergRow.getRecord().getField("retentionTimeSec"), 1745908497L);
  }

  @Test
  void testToPrimaryKey() {
    IcebergRowPrimaryKey irpk = jobIcebergRow.getIcebergRowPrimaryKey();
    Assertions.assertEquals(irpk.getRecord().getField("jobId"), "id1");
  }
}
