package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.TablesClient;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DataLayoutPartitionUtilTest {

  private static DataLayoutStrategy strategy(String partitionId, double cost) {
    return DataLayoutStrategy.builder()
        .partitionId(partitionId)
        .partitionColumns("date")
        .cost(cost)
        .gain(10)
        .build();
  }

  @Test
  void testKeepsOnlyPartitionScopeEnabledTables() {
    Map<String, List<DataLayoutStrategy>> selected = new LinkedHashMap<>();
    selected.put("db.enabled", Arrays.asList(strategy("a", 1.0), strategy("b", 2.0)));
    selected.put("db.disabled", Collections.singletonList(strategy("c", 3.0)));

    TablesClient tablesClient = Mockito.mock(TablesClient.class);
    Mockito.when(tablesClient.isPartitionScopeEnabled("db", "enabled")).thenReturn(true);
    Mockito.when(tablesClient.isPartitionScopeEnabled("db", "disabled")).thenReturn(false);

    List<TableDataLayoutMetadata> result =
        DataLayoutPartitionUtil.toPartitionTableDataLayoutMetadata(selected, tablesClient);

    Assertions.assertEquals(1, result.size());
    TableDataLayoutMetadata metadata = result.get(0);
    Assertions.assertEquals("db", metadata.getDbName());
    Assertions.assertEquals("enabled", metadata.getTableName());
    Assertions.assertTrue(metadata.isPartitionScope());
    Assertions.assertTrue(metadata.isPrimary());
    Assertions.assertEquals(2, metadata.getDataLayoutStrategies().size());
  }

  @Test
  void testSkipsMalformedFqtnAndEmptyStrategyLists() {
    Map<String, List<DataLayoutStrategy>> selected = new LinkedHashMap<>();
    selected.put("no_dot_fqtn", Collections.singletonList(strategy("a", 1.0)));
    selected.put("db.empty", Collections.emptyList());

    TablesClient tablesClient = Mockito.mock(TablesClient.class);
    Mockito.when(tablesClient.isPartitionScopeEnabled(Mockito.anyString(), Mockito.anyString()))
        .thenReturn(true);

    List<TableDataLayoutMetadata> result =
        DataLayoutPartitionUtil.toPartitionTableDataLayoutMetadata(selected, tablesClient);

    Assertions.assertTrue(result.isEmpty());
  }
}
