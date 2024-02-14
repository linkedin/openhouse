package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.Retention;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TableRetentionTaskTest {
  private TablesClient tablesClient;
  private JobsClient jobsClient;
  private TableMetadata tableMetadata;

  @BeforeEach
  void setup() {
    tablesClient = Mockito.mock(TablesClient.class);
    jobsClient = Mockito.mock(JobsClient.class);
    tableMetadata = TableMetadata.builder().tableName("table").dbName("db").build();
  }

  @Test
  void testRetentionJobArgsForTableWithPattern() {
    TableRetentionTask tableRetentionTask =
        new TableRetentionTask(jobsClient, tablesClient, tableMetadata);
    String columnPattern = "yyyy-MM-DD";
    String columnName = "testColumnName";
    int count = 1;
    Retention.GranularityEnum retentionGranularity = Retention.GranularityEnum.DAY;
    RetentionConfig retentionConfigMock = Mockito.mock(RetentionConfig.class);
    Mockito.when(retentionConfigMock.getColumnPattern()).thenReturn(columnPattern);
    Mockito.when(retentionConfigMock.getColumnName()).thenReturn(columnName);
    Mockito.when(retentionConfigMock.getGranularity()).thenReturn(retentionGranularity);
    Mockito.when(retentionConfigMock.getCount()).thenReturn(count);
    Mockito.when(tablesClient.getTableRetention(tableMetadata))
        .thenReturn(Optional.of(retentionConfigMock));
    List<String> expectedArgs =
        Stream.of(
                "--tableName",
                tableMetadata.fqtn(),
                "--columnName",
                columnName,
                "--granularity",
                retentionGranularity.getValue(),
                "--count",
                String.valueOf(count),
                "--columnPattern",
                columnPattern)
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }

  @Test
  void testRetentionJobArgsForTableWithoutPattern() {
    TableRetentionTask tableRetentionTask =
        new TableRetentionTask(jobsClient, tablesClient, tableMetadata);
    String columnPattern = "";
    String columnName = "testColumnName";
    int count = 1;
    Retention.GranularityEnum retentionGranularity = Retention.GranularityEnum.DAY;
    RetentionConfig retentionConfigMock = Mockito.mock(RetentionConfig.class);
    Mockito.when(retentionConfigMock.getColumnPattern()).thenReturn(columnPattern);
    Mockito.when(retentionConfigMock.getColumnName()).thenReturn(columnName);
    Mockito.when(retentionConfigMock.getGranularity()).thenReturn(retentionGranularity);
    Mockito.when(retentionConfigMock.getCount()).thenReturn(count);
    Mockito.when(tablesClient.getTableRetention(tableMetadata))
        .thenReturn(Optional.of(retentionConfigMock));
    List<String> expectedArgs =
        Stream.of(
                "--tableName",
                tableMetadata.fqtn(),
                "--columnName",
                columnName,
                "--granularity",
                retentionGranularity.getValue(),
                "--count",
                String.valueOf(count))
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }
}
