package com.linkedin.openhouse.jobs.util;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitOperation;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableStatsCollectorUtilTest {

  @Test
  public void testParseCommitOperation() {
    Assertions.assertEquals(
        CommitOperation.APPEND,
        TableStatsCollectorUtil.parseCommitOperation("append"),
        "Should correctly parse 'append'");
    Assertions.assertEquals(
        CommitOperation.OVERWRITE,
        TableStatsCollectorUtil.parseCommitOperation("overwrite"),
        "Should correctly parse 'overwrite'");
    Assertions.assertNull(
        TableStatsCollectorUtil.parseCommitOperation("unknown_op"),
        "Should return null for unknown operations");
    Assertions.assertNull(
        TableStatsCollectorUtil.parseCommitOperation(null), "Should return null for null input");
  }

  @Test
  public void testRowToCommitEventTable() {
    // Mock a Spark Row
    Row mockRow = mock(Row.class);
    when(mockRow.getAs("database_name")).thenReturn("db");
    when(mockRow.getAs("table_name")).thenReturn("table");
    when(mockRow.getAs("cluster_name")).thenReturn("cluster");
    when(mockRow.getAs("table_metadata_location")).thenReturn("/path/to/metadata");
    when(mockRow.getAs("partition_spec")).thenReturn("spec");
    when(mockRow.getAs("commit_id")).thenReturn("12345");
    when(mockRow.getAs("commit_timestamp_ms")).thenReturn(98765L);
    when(mockRow.getAs("commit_app_id")).thenReturn("app_id");
    when(mockRow.getAs("commit_app_name")).thenReturn("app_name");
    when(mockRow.getAs("commit_operation")).thenReturn("append");

    // Call the method
    CommitEventTable result = TableStatsCollectorUtil.rowToCommitEventTable(mockRow);

    // Assertions
    Assertions.assertNotNull(result);

    // Verify dataset fields
    Assertions.assertEquals("db", result.getDataset().getDatabaseName());
    Assertions.assertEquals("table", result.getDataset().getTableName());
    Assertions.assertEquals("cluster", result.getDataset().getClusterName());
    Assertions.assertEquals("/path/to/metadata", result.getDataset().getTableMetadataLocation());
    Assertions.assertEquals("spec", result.getDataset().getPartitionSpec());

    // Verify commit metadata fields
    Assertions.assertEquals(12345L, result.getCommitMetadata().getCommitId());
    Assertions.assertEquals(98765L, result.getCommitMetadata().getCommitTimestampMs());
    Assertions.assertEquals("app_id", result.getCommitMetadata().getCommitAppId());
    Assertions.assertEquals("app_name", result.getCommitMetadata().getCommitAppName());
    Assertions.assertEquals(
        CommitOperation.APPEND, result.getCommitMetadata().getCommitOperation());

    // Verify placeholder for event timestamp
    Assertions.assertEquals(0L, result.getEventTimestampMs());
  }

  @Test
  public void testRowToCommitEventTableWithNulls() {
    // Mock a Spark Row with some null values
    Row mockRow = mock(Row.class);
    when(mockRow.getAs("database_name")).thenReturn("db");
    when(mockRow.getAs("table_name")).thenReturn("table");
    when(mockRow.getAs("cluster_name")).thenReturn("cluster");
    when(mockRow.getAs("table_metadata_location")).thenReturn("/path/to/metadata");
    when(mockRow.getAs("partition_spec")).thenReturn("spec");
    when(mockRow.getAs("commit_id")).thenReturn("12345");
    when(mockRow.getAs("commit_timestamp_ms")).thenReturn(98765L);
    when(mockRow.getAs("commit_app_id")).thenReturn(null);
    when(mockRow.getAs("commit_app_name")).thenReturn(null);
    when(mockRow.getAs("commit_operation")).thenReturn(null);

    // Call the method
    CommitEventTable result = TableStatsCollectorUtil.rowToCommitEventTable(mockRow);

    // Assertions
    Assertions.assertNotNull(result);
    Assertions.assertNull(result.getCommitMetadata().getCommitAppId());
    Assertions.assertNull(result.getCommitMetadata().getCommitAppName());
    Assertions.assertNull(result.getCommitMetadata().getCommitOperation());
  }
}
