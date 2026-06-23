package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class DloPartitionStrategiesReaderTest {

  private static Map<String, Object> row(
      String fqtn, String partitionId, double cost, double gain) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("fqtn", fqtn);
    m.put("partition_id", partitionId);
    m.put("partition_columns", "date");
    m.put("cost", cost);
    m.put("gain", gain);
    m.put("file_size_entropy", 1.0);
    m.put("pos_delete_file_count", 1L);
    m.put("eq_delete_file_count", 2L);
    m.put("pos_delete_file_bytes", 3L);
    m.put("eq_delete_file_bytes", 4L);
    m.put("pos_delete_record_count", 5L);
    m.put("eq_delete_record_count", 6L);
    m.put("file_count_reduction_penalty", 0.0);
    return m;
  }

  @Test
  void testPushesDownRankingAndMapsRows() throws Exception {
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(row("db.t1", "a", 10.0, 100.0));
    rows.add(row("db.t1", "b", 20.0, 50.0));
    rows.add(row("db.t2", "c", 5.0, 30.0));

    TrinoClient trinoClient = Mockito.mock(TrinoClient.class);
    Mockito.when(trinoClient.executeQuery(Mockito.anyString())).thenReturn(rows);

    DloPartitionStrategiesReader reader =
        new DloPartitionStrategiesReader(
            trinoClient,
            "u_openhouse.dlo_partition_strategies",
            /* globalMaxCostBudgetGbHrs */ 5000.0,
            /* perTableCostBudgetGbHrs */ 480.0,
            /* maxPartitionsPerTable */ 1000);
    Map<String, List<DataLayoutStrategy>> result = reader.readSelectedGroupedByTable();

    // grouped by table, preserving order and mapping
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(2, result.get("db.t1").size());
    Assertions.assertEquals(1, result.get("db.t2").size());
    DataLayoutStrategy first = result.get("db.t1").get(0);
    Assertions.assertEquals("a", first.getPartitionId());
    Assertions.assertEquals("date", first.getPartitionColumns());
    Assertions.assertEquals(10.0, first.getCost());
    Assertions.assertEquals(100.0, first.getGain());
    Assertions.assertEquals(5L, first.getPosDeleteRecordCount());

    // verify global ranking + per-table guardrail are pushed into the query
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(trinoClient).executeQuery(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    Assertions.assertTrue(sql.contains("u_openhouse.dlo_partition_strategies"));
    // global normalization (no PARTITION BY) and global prefix-sum cutoff
    Assertions.assertTrue(sql.contains("MIN(cost) OVER ()"));
    Assertions.assertTrue(sql.contains("global_cost_before"));
    Assertions.assertTrue(sql.contains("< 5000"));
    // per-table guardrail: partitioned prefix-sum + count cap
    Assertions.assertTrue(sql.contains("table_cost_before"));
    Assertions.assertTrue(sql.contains("PARTITION BY fqtn ORDER BY score DESC"));
    Assertions.assertTrue(sql.contains("< 480"));
    Assertions.assertTrue(sql.contains("table_rnk <= 1000"));
  }
}
