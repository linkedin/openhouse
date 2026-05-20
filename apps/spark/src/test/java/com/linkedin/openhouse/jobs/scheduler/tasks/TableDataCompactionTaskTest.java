package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TableDataCompactionTaskTest {
  private TablesClient tablesClient;
  private JobsClient jobsClient;
  private TableMetadata tableMetadata;
  private TableDataCompactionTask task;

  @BeforeEach
  void setup() {
    tablesClient = Mockito.mock(TablesClient.class);
    jobsClient = Mockito.mock(JobsClient.class);
    tableMetadata = Mockito.mock(TableMetadata.class);
    Mockito.when(tableMetadata.fqtn()).thenReturn("db.table");
    Mockito.when(tableMetadata.isPrimary()).thenReturn(true);
    Mockito.when(tableMetadata.isTimePartitioned()).thenReturn(true);
    Mockito.when(tableMetadata.isClustered()).thenReturn(false);
    task = new TableDataCompactionTask(jobsClient, tablesClient, tableMetadata);
  }

  @Test
  void shouldRunWhenAtLeastOneStrategyHasPositiveGain() {
    Mockito.when(tablesClient.getDataLayoutStrategies(tableMetadata))
        .thenReturn(
            Arrays.asList(
                DataLayoutStrategy.builder().gain(0.0).build(),
                DataLayoutStrategy.builder().gain(2.5).build()));
    Assertions.assertTrue(task.shouldRunTask());
  }

  @Test
  void shouldNotRunWhenStrategiesEmpty() {
    Mockito.when(tablesClient.getDataLayoutStrategies(tableMetadata))
        .thenReturn(Collections.emptyList());
    Assertions.assertFalse(task.shouldRunTask());
  }

  @Test
  void shouldNotRunWhenAllStrategiesHaveNonPositiveGain() {
    Mockito.when(tablesClient.getDataLayoutStrategies(tableMetadata))
        .thenReturn(
            Arrays.asList(
                DataLayoutStrategy.builder().gain(0.0).build(),
                DataLayoutStrategy.builder().gain(-1.0).build()));
    Assertions.assertFalse(task.shouldRunTask());
  }

  @Test
  void shouldNotRunForNonPrimaryTable() {
    Mockito.when(tableMetadata.isPrimary()).thenReturn(false);
    Assertions.assertFalse(task.shouldRunTask());
    // No need to fetch strategies if the primary/partitioning guard fails first.
    Mockito.verify(tablesClient, Mockito.never()).getDataLayoutStrategies(tableMetadata);
  }

  @Test
  void shouldNotRunWhenNeitherPartitionedNorClustered() {
    Mockito.when(tableMetadata.isTimePartitioned()).thenReturn(false);
    Mockito.when(tableMetadata.isClustered()).thenReturn(false);
    Assertions.assertFalse(task.shouldRunTask());
    Mockito.verify(tablesClient, Mockito.never()).getDataLayoutStrategies(tableMetadata);
  }

  @Test
  void shouldRunForClusteredNonTimePartitionedTableWithGain() {
    Mockito.when(tableMetadata.isTimePartitioned()).thenReturn(false);
    Mockito.when(tableMetadata.isClustered()).thenReturn(true);
    Mockito.when(tablesClient.getDataLayoutStrategies(tableMetadata))
        .thenReturn(Collections.singletonList(DataLayoutStrategy.builder().gain(1.0).build()));
    Assertions.assertTrue(task.shouldRunTask());
  }
}
