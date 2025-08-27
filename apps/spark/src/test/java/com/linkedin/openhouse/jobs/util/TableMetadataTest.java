package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableMetadataTest {

  @Test
  public void testIsMaintenanceJobDisabled() {
    Map<String, String> props = new HashMap<>();
    props.put("disabled", "true");
    TableMetadata metadata =
        TableMetadata.builder().dbName("db").tableName("t1").jobExecutionProperties(props).build();
    Assertions.assertTrue(metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.RETENTION));
  }

  @Test
  public void testIsMaintenanceJobDisabledCaseInsensitive() {
    Map<String, String> props = new HashMap<>();
    props.put("disabled", "True");
    TableMetadata metadata =
        TableMetadata.builder().dbName("db").tableName("t1").jobExecutionProperties(props).build();
    Assertions.assertTrue(metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.RETENTION));
  }

  @Test
  public void testIsMaintenanceJobDisabledFalse() {
    Map<String, String> props = new HashMap<>();
    props.put("disabled", "false");
    TableMetadata metadata =
        TableMetadata.builder().dbName("db").tableName("t1").jobExecutionProperties(props).build();
    Assertions.assertFalse(metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.RETENTION));
  }

  @Test
  public void testIsMaintenanceJobDisabledNotSet() {
    TableMetadata metadata =
        TableMetadata.builder()
            .dbName("db")
            .tableName("t1")
            .jobExecutionProperties(Collections.emptyMap())
            .build();
    Assertions.assertFalse(metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.RETENTION));
  }

  @Test
  public void testIsIndividualMaintenanceJobDisabled() {
    Map<String, String> props = new HashMap<>();
    props.put("ORPHAN_FILES_DELETION.disabled", "true");
    TableMetadata metadata =
        TableMetadata.builder().dbName("db").tableName("t1").jobExecutionProperties(props).build();
    Assertions.assertTrue(
        metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION));
    Assertions.assertFalse(metadata.isMaintenanceJobDisabled(JobConf.JobTypeEnum.RETENTION));
  }
}
