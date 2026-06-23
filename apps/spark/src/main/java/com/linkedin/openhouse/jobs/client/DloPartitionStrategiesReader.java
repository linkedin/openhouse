package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads partition-level data layout strategies from the DLO partition strategies table (written by
 * {@code DataLayoutStrategyGeneratorSparkApp}) via Trino.
 *
 * <p>Ranking and budgeting are pushed into the Trino query so the scheduler never sorts/selects
 * across the full strategy set in memory (there can be millions of partition strategies across tens
 * of thousands of tables). The selection is <b>global</b> with a <b>per-table guardrail</b>:
 *
 * <ol>
 *   <li>keep each table's most recent generation (the table is append-only);
 *   <li>drop strategies with no discounted file-count reduction (gain);
 *   <li>score every strategy with a globally normalized weighted sum (same formula as {@code
 *       SimpleWeightedSumDataLayoutStrategyScorer}, but min-max normalized across ALL strategies);
 *   <li>per-table guardrail: a strategy is eligible only while the cost of strictly higher-scoring
 *       strategies <i>in its own table</i> stays under the per-table budget (= maxExecutionHours *
 *       executorMemoryGb GB-hr, i.e. the 24h compute-time cap), bounded by a max partition count;
 *   <li>global cap: among the table-eligible strategies, rank globally by score and keep them while
 *       the cumulative cost of strictly higher-scoring eligible strategies stays under the global
 *       cost budget.
 * </ol>
 *
 * <p>Both prefix-sum cutoffs match {@code GreedyMaxBudgetCandidateSelector} (the boundary strategy
 * that crosses a budget is included; the next is not). Per-table selection is a pure prefix (no
 * skip-and-continue), so table eligibility is independent of the global decision and the two passes
 * compose into the same result as a single sequential greedy. Only the finally-selected partitions
 * are returned, grouped by table.
 *
 * <p>Note: the strategies table does not persist the {@link DataCompactionConfig}; the
 * reconstructed strategies use config defaults.
 */
@Slf4j
@AllArgsConstructor
public class DloPartitionStrategiesReader {
  // Must match SimpleWeightedSumDataLayoutStrategyScorer weights and the DataLayoutUtil gain
  // filter.
  private static final double COMPACTION_GAIN_WEIGHT = 0.7;
  private static final double COMPUTE_COST_WEIGHT = 0.3;
  private static final double MIN_DISCOUNTED_GAIN = 1.0;

  private final TrinoClient trinoClient;
  private final String dloPartitionStrategiesTable;
  // Global cost budget in GB-hr across all selected partitions of all tables.
  private final double globalMaxCostBudgetGbHrs;
  // Per-table cost budget in GB-hr = maxExecutionHours * executorMemoryGb (the 24h compute-time
  // cap).
  private final double perTableCostBudgetGbHrs;
  // Hard cap on partitions selected per table (bounds the rewrite WHERE clause / arg list size).
  private final int maxPartitionsPerTable;

  /**
   * @return map of fully-qualified table name to that table's selected partition strategies
   *     (globally ranked, highest score first), preserving query order.
   */
  public Map<String, List<DataLayoutStrategy>> readSelectedGroupedByTable() {
    String sql = buildRankAndSelectSql();
    log.info("Reading selected partition strategies from {}", dloPartitionStrategiesTable);
    List<Map<String, Object>> rows;
    try {
      rows = trinoClient.executeQuery(sql);
    } catch (SQLException e) {
      throw new RuntimeException(
          String.format("Failed to read partition strategies from %s", dloPartitionStrategiesTable),
          e);
    }
    log.info(
        "Read {} selected partition strategy rows from {}",
        rows.size(),
        dloPartitionStrategiesTable);

    Map<String, List<DataLayoutStrategy>> result = new LinkedHashMap<>();
    for (Map<String, Object> row : rows) {
      String fqtn = Objects.toString(row.get("fqtn"), null);
      if (fqtn == null) {
        continue;
      }
      result.computeIfAbsent(fqtn, k -> new ArrayList<>()).add(toStrategy(row));
    }
    return result;
  }

  /**
   * Builds the global rank-and-select SQL. The cost budgets and partition cap are numeric and the
   * table name is an operator-supplied trusted identifier, so they are inlined directly.
   */
  private String buildRankAndSelectSql() {
    return String.format(
        "WITH latest AS ("
            + "  SELECT *, MAX(timestamp) OVER (PARTITION BY fqtn) AS max_ts FROM %1$s"
            + "), "
            + "current_gen AS (SELECT * FROM latest WHERE timestamp = max_ts), "
            + "filtered AS ("
            + "  SELECT fqtn, partition_id, partition_columns, "
            + "    estimated_compute_cost AS cost, estimated_file_count_reduction AS gain, "
            + "    file_size_entropy, pos_delete_file_count, eq_delete_file_count, "
            + "    pos_delete_file_bytes, eq_delete_file_bytes, pos_delete_record_count, "
            + "    eq_delete_record_count, file_count_reduction_penalty, "
            + "    estimated_file_count_reduction * (1 - file_count_reduction_penalty) AS dgain "
            + "  FROM current_gen "
            + "  WHERE estimated_file_count_reduction * (1 - file_count_reduction_penalty) >= %5$s"
            + "), "
            + "bounds AS ("
            + "  SELECT *, MIN(cost) OVER () AS min_cost, MAX(cost) OVER () AS max_cost, "
            + "    MIN(dgain) OVER () AS min_dgain, MAX(dgain) OVER () AS max_dgain FROM filtered"
            + "), "
            + "scored AS ("
            + "  SELECT *, "
            + "    (%6$s * (CASE WHEN max_dgain = min_dgain THEN 0.0 "
            + "                  ELSE (dgain - min_dgain) / (max_dgain - min_dgain) END)) "
            + "    - (%7$s * (CASE WHEN max_cost = min_cost THEN 0.0 "
            + "                  ELSE (cost - min_cost) / (max_cost - min_cost) END)) AS score "
            + "  FROM bounds"
            + "), "
            + "table_ranked AS ("
            + "  SELECT *, "
            + "    ROW_NUMBER() OVER (PARTITION BY fqtn ORDER BY score DESC, partition_id) AS table_rnk, "
            + "    SUM(cost) OVER (PARTITION BY fqtn ORDER BY score DESC, partition_id "
            + "      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS table_cost_before "
            + "  FROM scored"
            + "), "
            + "table_eligible AS ("
            + "  SELECT * FROM table_ranked "
            + "  WHERE COALESCE(table_cost_before, 0) < %3$s AND table_rnk <= %4$d"
            + "), "
            + "global_ranked AS ("
            + "  SELECT *, "
            + "    SUM(cost) OVER (ORDER BY score DESC, fqtn, partition_id "
            + "      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS global_cost_before "
            + "  FROM table_eligible"
            + ") "
            + "SELECT fqtn, partition_id, partition_columns, cost, gain, file_size_entropy, "
            + "  pos_delete_file_count, eq_delete_file_count, pos_delete_file_bytes, "
            + "  eq_delete_file_bytes, pos_delete_record_count, eq_delete_record_count, "
            + "  file_count_reduction_penalty "
            + "FROM global_ranked "
            + "WHERE COALESCE(global_cost_before, 0) < %2$s "
            + "ORDER BY score DESC, fqtn, partition_id",
        dloPartitionStrategiesTable,
        globalMaxCostBudgetGbHrs,
        perTableCostBudgetGbHrs,
        maxPartitionsPerTable,
        MIN_DISCOUNTED_GAIN,
        COMPACTION_GAIN_WEIGHT,
        COMPUTE_COST_WEIGHT);
  }

  private static DataLayoutStrategy toStrategy(Map<String, Object> row) {
    return DataLayoutStrategy.builder()
        .cost(toDouble(row.get("cost")))
        .gain(toDouble(row.get("gain")))
        .entropy(toDouble(row.get("file_size_entropy")))
        .partitionId(Objects.toString(row.get("partition_id"), null))
        .partitionColumns(Objects.toString(row.get("partition_columns"), null))
        .posDeleteFileCount(toLong(row.get("pos_delete_file_count")))
        .eqDeleteFileCount(toLong(row.get("eq_delete_file_count")))
        .posDeleteFileBytes(toLong(row.get("pos_delete_file_bytes")))
        .eqDeleteFileBytes(toLong(row.get("eq_delete_file_bytes")))
        .posDeleteRecordCount(toLong(row.get("pos_delete_record_count")))
        .eqDeleteRecordCount(toLong(row.get("eq_delete_record_count")))
        .fileCountReductionPenalty(toDouble(row.get("file_count_reduction_penalty")))
        .config(DataCompactionConfig.builder().build())
        .build();
  }

  private static double toDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return value == null ? 0.0 : Double.parseDouble(value.toString());
  }

  private static long toLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return value == null ? 0L : Long.parseLong(value.toString());
  }
}
