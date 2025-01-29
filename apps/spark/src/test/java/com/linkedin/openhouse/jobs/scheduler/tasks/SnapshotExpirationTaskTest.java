package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.HistoryConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.History;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SnapshotExpirationTaskTest {
  private TablesClient tablesClient;
  private JobsClient jobsClient;
  private TableMetadata tableMetadata;

  @BeforeEach
  void setup() {
    tablesClient = Mockito.mock(TablesClient.class);
    jobsClient = Mockito.mock(JobsClient.class);
    tableMetadata = Mockito.mock(TableMetadata.class);
    Mockito.when(tableMetadata.fqtn()).thenReturn("db.table");
  }

  @Test
  void testSnapshotExpirationForTableWithoutConfig() {
    TableSnapshotsExpirationTask tableRetentionTask =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, tableMetadata);

    List<String> expectedArgs =
        Stream.of("--tableName", tableMetadata.fqtn()).collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }

  @Test
  void testSnapshotExpirationJobWithOnlyMaxAgeConfig() {
    TableSnapshotsExpirationTask tableRetentionTask =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, tableMetadata);

    HistoryConfig historyConfigMock = Mockito.mock(HistoryConfig.class);
    int maxAge = 1;
    History.GranularityEnum granularity = History.GranularityEnum.DAY;

    Mockito.when(tableMetadata.getHistoryConfig()).thenReturn(historyConfigMock);
    Mockito.when(historyConfigMock.getMaxAge()).thenReturn(maxAge);
    Mockito.when(historyConfigMock.getGranularity()).thenReturn(granularity);
    List<String> expectedArgs =
        Stream.of(
                "--tableName",
                tableMetadata.fqtn(),
                "--maxAge",
                String.valueOf(maxAge),
                "--granularity",
                granularity.getValue())
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }

  @Test
  void testSnapshotExpirationJobWithOnlyVersionsConfig() {
    TableSnapshotsExpirationTask tableRetentionTask =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, tableMetadata);

    HistoryConfig historyConfigMock = Mockito.mock(HistoryConfig.class);
    int versions = 3;

    Mockito.when(tableMetadata.getHistoryConfig()).thenReturn(historyConfigMock);
    Mockito.when(historyConfigMock.getVersions()).thenReturn(versions);
    List<String> expectedArgs =
        Stream.of("--tableName", tableMetadata.fqtn(), "--versions", String.valueOf(versions))
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }

  @Test
  void testSnapshotExpirationJobWithMaxAgeAndVersions() {
    TableSnapshotsExpirationTask tableRetentionTask =
        new TableSnapshotsExpirationTask(jobsClient, tablesClient, tableMetadata);

    HistoryConfig historyConfigMock = Mockito.mock(HistoryConfig.class);
    int maxAge = 3;
    History.GranularityEnum granularity = History.GranularityEnum.DAY;
    int versions = 3;

    Mockito.when(tableMetadata.getHistoryConfig()).thenReturn(historyConfigMock);
    Mockito.when(historyConfigMock.getMaxAge()).thenReturn(maxAge);
    Mockito.when(historyConfigMock.getGranularity()).thenReturn(granularity);
    Mockito.when(historyConfigMock.getVersions()).thenReturn(versions);

    List<String> expectedArgs =
        Stream.of(
                "--tableName",
                tableMetadata.fqtn(),
                "--maxAge",
                String.valueOf(maxAge),
                "--granularity",
                granularity.getValue(),
                "--versions",
                String.valueOf(versions))
            .collect(Collectors.toList());
    Assertions.assertEquals(expectedArgs, tableRetentionTask.getArgs());
  }
}
