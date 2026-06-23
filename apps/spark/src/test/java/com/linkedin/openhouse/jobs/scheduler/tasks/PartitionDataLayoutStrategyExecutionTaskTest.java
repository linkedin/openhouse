package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PartitionDataLayoutStrategyExecutionTaskTest {

  private static DataLayoutStrategy partitionStrategy(String partitionId) {
    return DataLayoutStrategy.builder()
        .partitionId(partitionId)
        .partitionColumns("date, hour")
        .config(DataCompactionConfig.builder().build())
        .build();
  }

  private PartitionDataLayoutStrategyExecutionTask task(
      List<DataLayoutStrategy> strategies, boolean isPartitionScope, boolean isPrimary) {
    TableDataLayoutMetadata metadata =
        TableDataLayoutMetadata.builder()
            .dbName("db")
            .tableName("t")
            .isPrimary(isPrimary)
            .isPartitionScope(isPartitionScope)
            .dataLayoutStrategies(strategies)
            .build();
    return new PartitionDataLayoutStrategyExecutionTask(
        Mockito.mock(JobsClient.class), Mockito.mock(TablesClient.class), metadata);
  }

  @Test
  void testGetArgsEmitsNormalizedColumnsAndOnePartitionValuesPerStrategy() {
    PartitionDataLayoutStrategyExecutionTask task =
        task(Arrays.asList(partitionStrategy("a, b"), partitionStrategy("c, d")), true, true);
    List<String> args = task.getArgs();

    Assertions.assertEquals("db.t", args.get(args.indexOf("--tableName") + 1));
    // generator's ", " join is normalized to ","
    Assertions.assertEquals("date,hour", args.get(args.indexOf("--partitionColumns") + 1));
    // one --partitionValues per selected partition, normalized
    long partitionValuesFlags = args.stream().filter("--partitionValues"::equals).count();
    Assertions.assertEquals(2, partitionValuesFlags);
    Assertions.assertTrue(args.contains("a,b"));
    Assertions.assertTrue(args.contains("c,d"));
    // config args present
    Assertions.assertTrue(args.contains("--targetByteSize"));
  }

  @Test
  void testShouldRunRequiresPartitionScopeAndPrimary() {
    Assertions.assertTrue(
        task(Collections.singletonList(partitionStrategy("a")), true, true).shouldRunTask());
    Assertions.assertFalse(
        task(Collections.singletonList(partitionStrategy("a")), false, true).shouldRunTask());
    Assertions.assertFalse(
        task(Collections.singletonList(partitionStrategy("a")), true, false).shouldRunTask());
  }
}
